package sparkplug

import (
	"context"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	bootstrapInterfaces "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"google.golang.org/protobuf/proto"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/secure"

	"github.com/edgexfoundry/app-service-configurable/internal/sparkplug/spplugb"
)

const (
	namespace = "spBv1.0"

	msgTypeNBirth = "NBIRTH"
	msgTypeNDeath = "NDEATH"
	msgTypeDBirth = "DBIRTH"
	msgTypeDData  = "DDATA"
	msgTypeNCmd   = "NCMD"

	// metricBdSeq and metricNodeRebirth are the two well-known node metrics every NBIRTH/NDEATH
	// must/should carry; they're given fixed aliases so they're recognizable across messages.
	metricBdSeq       = "bdSeq"
	metricNodeRebirth = "Node Control/Rebirth"

	aliasBdSeq       = uint64(0)
	aliasNodeRebirth = uint64(1)
	firstDeviceAlias = uint64(2)

	connectTimeout = 30 * time.Second
	publishTimeout = 10 * time.Second
)

// device tracks the Sparkplug birth state for a single EdgeX device: the metric name->alias
// table built up as new metrics are seen, and the most recently published metric set (kept so a
// DBIRTH can be replayed on reconnect/rebirth without waiting for fresh data).
type device struct {
	aliases map[string]uint64
	metrics []Metric
	born    bool
}

// Node manages one Sparkplug B Edge Node session: its MQTT connection and Will, the NBIRTH/NDEATH
// and per-device DBIRTH/DDATA lifecycle, the node-level sequence counter, and the set of EdgeX
// devices it has seen so far. One Node corresponds to one running instance of this service.
type Node struct {
	cfg    Config
	lc     logger.LoggingClient
	client mqtt.Client

	mu        sync.Mutex
	bdSeq     uint64
	seq       uint8
	nextAlias uint64
	devices   map[string]*device
}

// NewNode builds a Node and its underlying MQTT client, with the Will configured to publish
// NDEATH on ungraceful disconnect. Call Start to actually connect.
func NewNode(cfg Config, sp bootstrapInterfaces.SecretProvider, lc logger.LoggingClient) (*Node, error) {
	if cfg.GroupId == "" || cfg.EdgeNodeId == "" || cfg.BrokerAddress == "" {
		return nil, fmt.Errorf("sparkplug: GroupId, EdgeNodeId and BrokerAddress are all required")
	}

	n := &Node{
		cfg:       cfg,
		lc:        lc,
		nextAlias: firstDeviceAlias,
		devices:   make(map[string]*device),
	}

	deathPayload, err := n.deathPayload()
	if err != nil {
		return nil, fmt.Errorf("sparkplug: failed to build NDEATH will payload: %w", err)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.BrokerAddress).
		SetClientID(cfg.ClientId).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetBinaryWill(n.topic(msgTypeNDeath, ""), deathPayload, byte(cfg.QoS), false)

	opts.OnConnect = func(_ mqtt.Client) {
		if err := n.onConnect(); err != nil {
			n.lc.Errorf("sparkplug: error handling connect: %v", err)
		}
	}
	opts.OnConnectionLost = func(_ mqtt.Client, err error) {
		n.lc.Warnf("sparkplug: connection to broker lost: %v", err)
	}

	factory := secure.NewMqttFactory(sp, lc, cfg.AuthMode, cfg.SecretName, cfg.SkipCertVerify)
	client, err := factory.Create(opts)
	if err != nil {
		return nil, fmt.Errorf("sparkplug: failed to create mqtt client: %w", err)
	}
	n.client = client

	return n, nil
}

// Start connects to the broker and returns once the initial NBIRTH has been published. It
// registers a goroutine that publishes a best-effort NDEATH and disconnects cleanly when ctx is
// cancelled, for graceful shutdown.
func (n *Node) Start(ctx context.Context) error {
	token := n.client.Connect()
	if !token.WaitTimeout(connectTimeout) {
		return fmt.Errorf("sparkplug: timed out connecting to broker %s", n.cfg.BrokerAddress)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("sparkplug: failed to connect to broker %s: %w", n.cfg.BrokerAddress, err)
	}

	go func() {
		<-ctx.Done()
		n.shutdown()
	}()

	return nil
}

// shutdown publishes a clean NDEATH (best-effort) and disconnects. Used on graceful service stop;
// an unexpected process exit instead relies on the broker publishing the MQTT Will.
func (n *Node) shutdown() {
	n.mu.Lock()
	payload, err := n.deathPayload()
	n.mu.Unlock()

	if err == nil && n.client.IsConnected() {
		token := n.client.Publish(n.topic(msgTypeNDeath, ""), byte(n.cfg.QoS), false, payload)
		token.WaitTimeout(publishTimeout)
	}
	n.client.Disconnect(250)
}

// onConnect subscribes to this Edge Node's NCMD topic and performs an initial birth. It runs on
// every successful (re)connect, since a new MQTT session invalidates any state a host application
// previously had for this node.
func (n *Node) onConnect() error {
	topic := n.topic(msgTypeNCmd, "")
	token := n.client.Subscribe(topic, byte(n.cfg.QoS), n.handleNCmd)
	if !token.WaitTimeout(connectTimeout) {
		return fmt.Errorf("sparkplug: timed out subscribing to %s", topic)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("sparkplug: failed to subscribe to %s: %w", topic, err)
	}

	return n.rebirth()
}

// handleNCmd reacts to the "Node Control/Rebirth" command, the only NCMD metric handled. Per-
// device command dispatch (DCMD) would require forwarding into EdgeX's command service and is a
// deferred future enhancement, not a mandatory edge-node behavior.
func (n *Node) handleNCmd(_ mqtt.Client, msg mqtt.Message) {
	var payload spplugb.Payload
	if err := proto.Unmarshal(msg.Payload(), &payload); err != nil {
		n.lc.Errorf("sparkplug: failed to decode NCMD payload: %v", err)
		return
	}

	for _, m := range payload.GetMetrics() {
		if m.GetName() == metricNodeRebirth && m.GetBooleanValue() {
			n.lc.Info("sparkplug: received Node Control/Rebirth command, republishing birth certificates")
			if err := n.rebirth(); err != nil {
				n.lc.Errorf("sparkplug: failed to republish birth certificates: %v", err)
			}
			return
		}
	}
}

// rebirth resets the node's sequence counter and republishes NBIRTH followed by a DBIRTH for
// every device seen so far, replaying each device's last known metric snapshot. Called on every
// (re)connect and on an inbound Node Control/Rebirth command.
func (n *Node) rebirth() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.seq = 0
	for _, d := range n.devices {
		d.born = false
	}

	if err := n.publishNodeBirth(); err != nil {
		return err
	}

	for name, d := range n.devices {
		if err := n.publishDeviceBirth(name, d); err != nil {
			return err
		}
	}
	return nil
}

// PublishDeviceData publishes metrics for an EdgeX device, auto-birthing the device (DBIRTH) the
// first time it's seen, or whenever a metric name not covered by an earlier DBIRTH appears.
// Otherwise it publishes a DDATA referencing existing aliases only, per the spec's recommendation
// of omitting names from data messages to save bandwidth.
func (n *Node) PublishDeviceData(deviceName string, metrics []Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	d, ok := n.devices[deviceName]
	if !ok {
		d = &device{aliases: make(map[string]uint64)}
		n.devices[deviceName] = d
	}

	needsBirth := !d.born
	for _, m := range metrics {
		if _, known := d.aliases[m.Name]; !known {
			needsBirth = true
			break
		}
	}

	d.metrics = metrics

	if needsBirth {
		return n.publishDeviceBirth(deviceName, d)
	}

	protoMetrics := make([]*spplugb.Payload_Metric, 0, len(metrics))
	for _, m := range metrics {
		pm, err := newProtoMetric(m.Name, d.aliases[m.Name], m.Timestamp, m.DataType, m.Value, false)
		if err != nil {
			return err
		}
		protoMetrics = append(protoMetrics, pm)
	}

	return n.publish(msgTypeDData, deviceName, n.newPayload(protoMetrics))
}

// publishNodeBirth publishes NBIRTH for this Edge Node, carrying the two well-known node metrics:
// bdSeq (matching the value used in the MQTT Will's NDEATH) and Node Control/Rebirth. Callers must
// hold n.mu.
func (n *Node) publishNodeBirth() error {
	bdSeqMetric, err := newProtoMetric(metricBdSeq, aliasBdSeq, time.Now(), spplugb.DataType_UInt64, n.bdSeq, true)
	if err != nil {
		return err
	}
	rebirthMetric, err := newProtoMetric(metricNodeRebirth, aliasNodeRebirth, time.Now(), spplugb.DataType_Boolean, false, true)
	if err != nil {
		return err
	}

	payload := n.newPayload([]*spplugb.Payload_Metric{bdSeqMetric, rebirthMetric})
	return n.publish(msgTypeNBirth, "", payload)
}

// publishDeviceBirth publishes DBIRTH for a device using its last known metric snapshot,
// allocating aliases for any metric names not seen before. Callers must hold n.mu.
func (n *Node) publishDeviceBirth(deviceName string, d *device) error {
	protoMetrics := make([]*spplugb.Payload_Metric, 0, len(d.metrics))
	for _, m := range d.metrics {
		alias, ok := d.aliases[m.Name]
		if !ok {
			alias = n.nextAlias
			n.nextAlias++
			d.aliases[m.Name] = alias
		}

		pm, err := newProtoMetric(m.Name, alias, m.Timestamp, m.DataType, m.Value, true)
		if err != nil {
			return err
		}
		protoMetrics = append(protoMetrics, pm)
	}

	if err := n.publish(msgTypeDBirth, deviceName, n.newPayload(protoMetrics)); err != nil {
		return err
	}
	d.born = true
	return nil
}

// deathPayload builds the NDEATH payload for the current bdSeq, used both as the MQTT Will and
// for a clean shutdown's explicit NDEATH publish.
func (n *Node) deathPayload() ([]byte, error) {
	metric, err := newProtoMetric(metricBdSeq, aliasBdSeq, time.Now(), spplugb.DataType_UInt64, n.bdSeq, true)
	if err != nil {
		return nil, err
	}
	payload := &spplugb.Payload{
		Timestamp: proto.Uint64(unixMilliTimestamp(time.Now())),
		Metrics:   []*spplugb.Payload_Metric{metric},
	}
	return proto.Marshal(payload)
}

// newPayload builds a Payload carrying the given metrics, stamped with the current time and the
// node's next sequence number (consumed and advanced, wrapping at 256 per the spec). Callers must
// hold n.mu.
func (n *Node) newPayload(metrics []*spplugb.Payload_Metric) *spplugb.Payload {
	seq := n.seq
	n.seq++ // uint8 wraps at 256 naturally
	return &spplugb.Payload{
		Timestamp: proto.Uint64(unixMilliTimestamp(time.Now())),
		Seq:       proto.Uint64(uint64(seq)),
		Metrics:   metrics,
	}
}

// publish marshals and publishes payload to the topic for msgType and (optionally) deviceName.
// BIRTH/DATA/DEATH messages are never retained, per the spec. Callers must hold n.mu.
func (n *Node) publish(msgType, deviceName string, payload *spplugb.Payload) error {
	data, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("sparkplug: failed to marshal %s payload: %w", msgType, err)
	}

	topic := n.topic(msgType, deviceName)
	token := n.client.Publish(topic, byte(n.cfg.QoS), false, data)
	if !token.WaitTimeout(publishTimeout) {
		return fmt.Errorf("sparkplug: timed out publishing to %s", topic)
	}
	return token.Error()
}

// topic builds a Sparkplug B topic for this Edge Node: spBv1.0/{group}/{msgType}/{edgeNode}, or
// with a trailing /{deviceName} for device-scoped message types.
func (n *Node) topic(msgType, deviceName string) string {
	if deviceName == "" {
		return fmt.Sprintf("%s/%s/%s/%s", namespace, n.cfg.GroupId, msgType, n.cfg.EdgeNodeId)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s", namespace, n.cfg.GroupId, msgType, n.cfg.EdgeNodeId, deviceName)
}

// unixMilliTimestamp converts t to the milliseconds-since-epoch timestamp Sparkplug B payloads
// use. Epoch milliseconds only go negative for times before 1970, never the case here.
func unixMilliTimestamp(t time.Time) uint64 {
	return uint64(t.UnixMilli()) //nolint:gosec
}

// newProtoMetric builds a *spplugb.Payload_Metric for the given name/alias/value, including the
// metric name only when includeName is true (BIRTH messages include names, DATA messages omit
// them and rely on the alias, per the spec's bandwidth-saving recommendation).
func newProtoMetric(name string, alias uint64, ts time.Time, dataType spplugb.DataType, value any, includeName bool) (*spplugb.Payload_Metric, error) {
	pm := &spplugb.Payload_Metric{
		Alias:     proto.Uint64(alias),
		Timestamp: proto.Uint64(unixMilliTimestamp(ts)),
		Datatype:  proto.Uint32(uint32(dataType)), //nolint:gosec // DataType is a small, always-non-negative enum.
	}
	if includeName {
		pm.Name = proto.String(name)
	}
	if err := setProtoValue(pm, dataType, value); err != nil {
		return nil, fmt.Errorf("metric '%s': %w", name, err)
	}
	return pm, nil
}
