package sparkplug

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	bootstrapInterfaces "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces"
	bootstrapMessaging "github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/messaging"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	gometrics "github.com/rcrowley/go-metrics"
	"google.golang.org/protobuf/proto"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/secure"

	"github.com/edgexfoundry/app-service-configurable/internal/sparkplug/spplugb"
)

const (
	msgTypeNBirth = "NBIRTH"
	msgTypeNDeath = "NDEATH"
	msgTypeDBirth = "DBIRTH"
	msgTypeDData  = "DDATA"
	msgTypeDDeath = "DDEATH"
	msgTypeNCmd   = "NCMD"
	msgTypeDCmd   = "DCMD"

	// metricBdSeq and metricNodeRebirth are the two well-known node metrics every NBIRTH/NDEATH
	// must/should carry; they're given fixed aliases so they're recognizable across messages.
	metricBdSeq       = "bdSeq"
	metricNodeRebirth = "Node Control/Rebirth"

	aliasBdSeq       = uint64(0)
	aliasNodeRebirth = uint64(1)
	firstDeviceAlias = uint64(2)

	subscribeTimeout = 30 * time.Second
	publishTimeout   = 10 * time.Second

	// go-metrics counters registered with the service's MetricsManager, giving an operator
	// visibility into publish failures and rebirths without having to scrape the broker.
	counterPublishErrors     = "SparkplugPublishErrors"
	counterRebirths          = "SparkplugRebirths"
	counterMessagesPublished = "SparkplugMessagesPublished"
)

// validAuthModes are the AuthMode values secure.NewMqttFactory understands, re-exposed here so
// NewNode can reject a typo'd AuthMode up front instead of failing later inside the MQTT factory.
var validAuthModes = map[string]bool{
	bootstrapMessaging.AuthModeNone:             true,
	bootstrapMessaging.AuthModeUsernamePassword: true,
	bootstrapMessaging.AuthModeCert:             true,
	bootstrapMessaging.AuthModeCA:               true,
}

// device tracks the Sparkplug birth state for a single EdgeX device: the metric name->alias
// table built up as new metrics are seen, and the latest value of every metric known for it (kept
// so a DBIRTH always declares the device's complete metric set, and can be replayed on
// reconnect/rebirth without waiting for fresh data).
type device struct {
	aliases map[string]uint64
	// metrics holds the latest Metric per name, in first-seen order; index maps name -> position.
	metrics     []Metric
	index       map[string]int
	profileName string
	born        bool
	// registered is set once the device has been seen in core-metadata by the lifecycle poller;
	// only such devices get a DDEATH when they later disappear from it.
	registered bool
}

func newDevice(profileName string) *device {
	return &device{aliases: make(map[string]uint64), index: make(map[string]int), profileName: profileName}
}

// merge records metrics as the device's latest values and reports whether any of them is a metric
// name the device didn't know yet. A nil-valued metric (declared from a profile) never overwrites
// a known value.
func (d *device) merge(metrics []Metric) bool {
	added := false
	for _, m := range metrics {
		i, ok := d.index[m.Name]
		switch {
		case !ok:
			d.index[m.Name] = len(d.metrics)
			d.metrics = append(d.metrics, m)
			added = true
		case m.Value != nil || d.metrics[i].Value == nil:
			d.metrics[i] = m
		}
	}
	return added
}

// metric returns the known metric with the given name, or the one using the given alias.
func (d *device) metric(name string, alias *uint64) (Metric, bool) {
	if name == "" && alias != nil {
		for n, a := range d.aliases {
			if a == *alias {
				name = n
				break
			}
		}
	}
	i, ok := d.index[name]
	if !ok {
		return Metric{}, false
	}
	return d.metrics[i], true
}

// Node manages one Sparkplug B Edge Node session: its MQTT connection and Will, the NBIRTH/NDEATH
// and per-device DBIRTH/DDATA/DDEATH lifecycle, the node-level sequence counter, inbound NCMD
// rebirth and (opt-in) DCMD commands, and the set of EdgeX devices it knows about. One Node
// corresponds to one running instance of this service.
//
// Primary Host Application STATE monitoring is not implemented; see docs/sparkplug-export.md.
type Node struct {
	cfg    Config
	s      settings
	lc     logger.LoggingClient
	client mqtt.Client

	// commands and profiles are set by EnableCommands; commands stays nil unless DCMD is enabled.
	commands commandIssuer
	profiles profileGetter

	publishErrors     gometrics.Counter
	rebirths          gometrics.Counter
	messagesPublished gometrics.Counter

	mu        sync.Mutex
	bdSeq     uint64
	seq       uint8
	nextAlias uint64
	devices   map[string]*device
}

// NewNode builds a Node and its underlying MQTT client, with the Will configured to publish
// NDEATH on ungraceful disconnect. Call Start to actually connect. metrics may be nil, in which
// case the counters described on Node are silently not registered/updated (used by tests).
func NewNode(cfg Config, sp bootstrapInterfaces.SecretProvider, lc logger.LoggingClient, metrics bootstrapInterfaces.MetricsManager) (*Node, error) {
	s, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	if cfg.MqttBroker.Retain {
		lc.Warn("sparkplug: MqttBroker.Retain is true; Sparkplug 3.0 requires BIRTH, DATA and DEATH messages to be non-retained")
	}

	n := &Node{
		cfg:               cfg,
		s:                 s,
		lc:                lc,
		bdSeq:             nextBdSeq(cfg.BdSeqStatePath, lc),
		nextAlias:         firstDeviceAlias,
		devices:           make(map[string]*device),
		publishErrors:     registerCounter(metrics, counterPublishErrors, lc),
		rebirths:          registerCounter(metrics, counterRebirths, lc),
		messagesPublished: registerCounter(metrics, counterMessagesPublished, lc),
	}

	deathPayload, err := n.deathPayload()
	if err != nil {
		return nil, fmt.Errorf("sparkplug: failed to build NDEATH will payload: %w", err)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MqttBroker.Url).
		SetClientID(clientID(s.clientIdPrefix)).
		SetCleanSession(true).
		SetConnectTimeout(s.connectTimeout).
		SetKeepAlive(s.keepAlive).
		SetAutoReconnect(cfg.MqttBroker.AutoReconnect).
		SetBinaryWill(n.topic(msgTypeNDeath, ""), deathPayload, byte(cfg.MqttBroker.QoS), false)

	opts.OnConnect = func(_ mqtt.Client) {
		if err := n.onConnect(); err != nil {
			n.lc.Errorf("sparkplug: error handling connect: %v", err)
		}
	}
	opts.OnConnectionLost = func(_ mqtt.Client, err error) {
		n.lc.Warnf("sparkplug: connection to broker lost: %v", err)
	}

	factory := secure.NewMqttFactory(sp, lc, cfg.MqttBroker.AuthMode, cfg.MqttBroker.SecretPath, cfg.MqttBroker.SkipCertVerify)
	client, err := factory.Create(opts)
	if err != nil {
		return nil, fmt.Errorf("sparkplug: failed to create mqtt client: %w", err)
	}
	n.client = client

	return n, nil
}

// clientID appends a random suffix to prefix so a restarted process never
// collides with a broker session the previous one still holds.
func clientID(prefix string) string {
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		return prefix
	}
	return prefix + "-" + hex.EncodeToString(suffix)
}

// Start connects to the broker, retrying every MqttBroker.RetryInterval for up to
// MqttBroker.RetryDuration, and returns once connected (the initial NBIRTH is published from the
// connect handler). It registers a goroutine that publishes a best-effort NDEATH and disconnects
// cleanly when ctx is cancelled, for graceful shutdown.
func (n *Node) Start(ctx context.Context) error {
	deadline := time.Now().Add(n.s.retryDuration)
	for {
		err := n.connect()
		if err == nil {
			break
		}
		if time.Now().Add(n.s.retryInterval).After(deadline) {
			return err
		}
		n.lc.Warnf("%v; retrying in %s", err, n.s.retryInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(n.s.retryInterval):
		}
	}

	go func() {
		<-ctx.Done()
		n.shutdown()
	}()

	return nil
}

func (n *Node) connect() error {
	token := n.client.Connect()
	if !token.WaitTimeout(n.s.connectTimeout + publishTimeout) {
		return fmt.Errorf("sparkplug: timed out connecting to broker %s", n.cfg.MqttBroker.Url)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("sparkplug: failed to connect to broker %s: %w", n.cfg.MqttBroker.Url, err)
	}
	return nil
}

// shutdown publishes a clean NDEATH (best-effort) and disconnects. Used on graceful service stop;
// an unexpected process exit instead relies on the broker publishing the MQTT Will.
func (n *Node) shutdown() {
	n.mu.Lock()
	payload, err := n.deathPayload()
	n.mu.Unlock()

	if err == nil && n.client.IsConnected() {
		token := n.client.Publish(n.topic(msgTypeNDeath, ""), byte(n.cfg.MqttBroker.QoS), n.cfg.MqttBroker.Retain, payload)
		token.WaitTimeout(publishTimeout)
	}
	n.client.Disconnect(250)
}

// onConnect subscribes to this Edge Node's NCMD topic (and DCMD topics, when commands are enabled)
// and performs an initial birth. It runs on every successful (re)connect, since a new MQTT session
// invalidates any state a host application previously had for this node.
func (n *Node) onConnect() error {
	if err := n.subscribe(n.topic(msgTypeNCmd, ""), n.handleNCmd); err != nil {
		return err
	}
	if n.commands != nil {
		if err := n.subscribe(n.topic(msgTypeDCmd, "+"), n.handleDCmd); err != nil {
			return err
		}
	}
	return n.rebirth()
}

func (n *Node) subscribe(topic string, handler mqtt.MessageHandler) error {
	token := n.client.Subscribe(topic, byte(n.cfg.MqttBroker.QoS), handler)
	if !token.WaitTimeout(subscribeTimeout) {
		return fmt.Errorf("sparkplug: timed out subscribing to %s", topic)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("sparkplug: failed to subscribe to %s: %w", topic, err)
	}
	return nil
}

// handleNCmd reacts to the "Node Control/Rebirth" command, the only NCMD metric handled.
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

	incCounter(n.rebirths)
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
// first time it's seen, or whenever a metric name not covered by an earlier DBIRTH appears; the
// DBIRTH always declares every metric known for the device, not only those in this call, since a
// host application treats a DBIRTH as the device's complete metric set. Otherwise it publishes a
// DDATA carrying just these metrics by alias, per the spec's recommendation of omitting names from
// data messages to save bandwidth.
//
// This holds n.mu for the entire publish, including the network round trip (up to
// publishTimeout), so concurrent calls for different devices serialize behind whichever one is
// currently talking to the broker. That's intentional, not an oversight: Sparkplug's seq counter
// must reach the wire in the same order it's consumed, and releasing the lock before the network
// write would let two publishes race past each other and land out of order.
func (n *Node) PublishDeviceData(deviceName, profileName string, metrics []Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	d, ok := n.devices[deviceName]
	if !ok {
		d = newDevice(profileName)
		n.devices[deviceName] = d
	}
	if d.profileName == "" {
		d.profileName = profileName
	}

	if added := d.merge(metrics); added || !d.born {
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

// publishDeviceDeath publishes DDEATH for a device (timestamp and seq only, per the spec) and
// forgets it, so a device registered again later is born afresh. Callers must hold n.mu.
func (n *Node) publishDeviceDeath(deviceName string) error {
	delete(n.devices, deviceName)
	return n.publish(msgTypeDDeath, deviceName, n.newPayload(nil))
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
// Messages are non-retained unless MqttBroker.Retain says otherwise. Callers must hold n.mu.
func (n *Node) publish(msgType, deviceName string, payload *spplugb.Payload) error {
	data, err := proto.Marshal(payload)
	if err != nil {
		incCounter(n.publishErrors)
		return fmt.Errorf("sparkplug: failed to marshal %s payload: %w", msgType, err)
	}

	topic := n.topic(msgType, deviceName)
	token := n.client.Publish(topic, byte(n.cfg.MqttBroker.QoS), n.cfg.MqttBroker.Retain, data)
	if !token.WaitTimeout(publishTimeout) {
		incCounter(n.publishErrors)
		return fmt.Errorf("sparkplug: timed out publishing to %s", topic)
	}
	if err := token.Error(); err != nil {
		incCounter(n.publishErrors)
		return err
	}

	incCounter(n.messagesPublished)
	return nil
}

// topic builds a Sparkplug B topic for this Edge Node: {namespace}/{group}/{msgType}/{edgeNode},
// or with a trailing /{deviceName} for device-scoped message types.
func (n *Node) topic(msgType, deviceName string) string {
	if deviceName == "" {
		return fmt.Sprintf("%s/%s/%s/%s", n.s.namespace, n.cfg.GroupId, msgType, n.cfg.EdgeNodeId)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s", n.s.namespace, n.cfg.GroupId, msgType, n.cfg.EdgeNodeId, deviceName)
}

// unixMilliTimestamp converts t to the milliseconds-since-epoch timestamp Sparkplug B payloads
// use. Epoch milliseconds only go negative for times before 1970, never the case here.
func unixMilliTimestamp(t time.Time) uint64 {
	return uint64(t.UnixMilli()) //nolint:gosec
}

// nextBdSeq returns the bdSeq value for this session. When path is empty, bdSeq is volatile and
// starts at 0 every run (today's behavior). When set, it reads the last persisted value from
// path, increments it, and writes the new value back so a Primary Host Application can tell a
// genuine new session apart from a replay across restarts. Any read/write failure (missing file
// on first run, corrupt content, unwritable path) is logged as a warning and falls back to 0
// rather than failing startup.
func nextBdSeq(path string, lc logger.LoggingClient) uint64 {
	if path == "" {
		return 0
	}

	var seq uint64
	if data, err := os.ReadFile(path); err == nil {
		parsed, perr := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if perr != nil {
			lc.Warnf("sparkplug: bdSeq state file '%s' has invalid content, starting bdSeq at 0: %v", path, perr)
		} else {
			seq = parsed + 1
		}
	} else if !os.IsNotExist(err) {
		lc.Warnf("sparkplug: failed to read bdSeq state file '%s', starting bdSeq at 0: %v", path, err)
	}

	if err := os.WriteFile(path, []byte(strconv.FormatUint(seq, 10)), 0o600); err != nil {
		lc.Warnf("sparkplug: failed to persist bdSeq to '%s': %v", path, err)
	}

	return seq
}

// registerCounter registers and returns a go-metrics Counter named "Sparkplug-"+name with the
// service's MetricsManager, or nil if metrics is nil (used by tests that don't exercise metrics).
// Registration failures (e.g. already registered) are logged and result in a nil counter, since a
// missing metric shouldn't stop the node from starting.
func registerCounter(metrics bootstrapInterfaces.MetricsManager, name string, lc logger.LoggingClient) gometrics.Counter {
	if metrics == nil {
		return nil
	}

	counter := gometrics.NewCounter()
	if err := metrics.Register(name, counter, nil); err != nil {
		lc.Warnf("sparkplug: failed to register metric '%s': %v", name, err)
		return nil
	}
	return counter
}

// incCounter increments counter by 1, tolerating a nil counter (metrics disabled/unregistered).
func incCounter(counter gometrics.Counter) {
	if counter != nil {
		counter.Inc(1)
	}
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
