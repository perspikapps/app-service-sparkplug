package sparkplug

import (
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"google.golang.org/protobuf/proto"

	"github.com/edgexfoundry/app-service-configurable/internal/sparkplug/spplugb"
)

// fakeToken is a completed mqtt.Token, so callers waiting on it never block.
type fakeToken struct {
	err error
}

func (t *fakeToken) Wait() bool                     { return true }
func (t *fakeToken) WaitTimeout(time.Duration) bool { return true }
func (t *fakeToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (t *fakeToken) Error() error { return t.err }

// published records a single call to fakeClient.Publish, decoded for assertions.
type published struct {
	topic    string
	qos      byte
	retained bool
	payload  *spplugb.Payload
}

// fakeClient is a minimal in-memory mqtt.Client that records publishes and lets tests directly
// invoke the subscribed callback, avoiding the need for a real broker in unit tests.
type fakeClient struct {
	connected  bool
	published  []published
	subscribed map[string]mqtt.MessageHandler
}

func newFakeClient() *fakeClient {
	return &fakeClient{connected: true, subscribed: make(map[string]mqtt.MessageHandler)}
}

func (c *fakeClient) IsConnected() bool      { return c.connected }
func (c *fakeClient) IsConnectionOpen() bool { return c.connected }
func (c *fakeClient) Connect() mqtt.Token    { c.connected = true; return &fakeToken{} }
func (c *fakeClient) Disconnect(uint)        { c.connected = false }

func (c *fakeClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	pb := &spplugb.Payload{}
	data, _ := payload.([]byte)
	_ = proto.Unmarshal(data, pb)
	c.published = append(c.published, published{topic: topic, qos: qos, retained: retained, payload: pb})
	return &fakeToken{}
}

func (c *fakeClient) Subscribe(topic string, _ byte, callback mqtt.MessageHandler) mqtt.Token {
	c.subscribed[topic] = callback
	return &fakeToken{}
}

func (c *fakeClient) SubscribeMultiple(filters map[string]byte, callback mqtt.MessageHandler) mqtt.Token {
	for topic := range filters {
		c.subscribed[topic] = callback
	}
	return &fakeToken{}
}

func (c *fakeClient) Unsubscribe(topics ...string) mqtt.Token {
	for _, topic := range topics {
		delete(c.subscribed, topic)
	}
	return &fakeToken{}
}

func (c *fakeClient) AddRoute(string, mqtt.MessageHandler) {}

func (c *fakeClient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}

// fakeMessage is a minimal mqtt.Message used to feed an NCMD payload into handleNCmd.
type fakeMessage struct {
	topic   string
	payload []byte
}

func (m *fakeMessage) Duplicate() bool   { return false }
func (m *fakeMessage) Qos() byte         { return 0 }
func (m *fakeMessage) Retained() bool    { return false }
func (m *fakeMessage) Topic() string     { return m.topic }
func (m *fakeMessage) MessageID() uint16 { return 0 }
func (m *fakeMessage) Payload() []byte   { return m.payload }
func (m *fakeMessage) Ack()              {}

func testNode(client *fakeClient) *Node {
	return &Node{
		cfg: Config{
			GroupId:    "TestGroup",
			EdgeNodeId: "TestNode",
			QoS:        0,
		},
		lc:        logger.NewMockClient(),
		client:    client,
		nextAlias: firstDeviceAlias,
		devices:   make(map[string]*device),
	}
}

func TestRebirth_PublishesNBirthWithBdSeqAndRebirthMetrics(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	n.bdSeq = 5

	if err := n.rebirth(); err != nil {
		t.Fatalf("rebirth() error = %v", err)
	}

	if len(client.published) != 1 {
		t.Fatalf("expected 1 publish (NBIRTH only, no devices yet), got %d", len(client.published))
	}

	got := client.published[0]
	if got.topic != "spBv1.0/TestGroup/NBIRTH/TestNode" {
		t.Errorf("unexpected NBIRTH topic: %s", got.topic)
	}
	if got.payload.GetSeq() != 0 {
		t.Errorf("NBIRTH seq = %d, want 0", got.payload.GetSeq())
	}

	metrics := got.payload.GetMetrics()
	if len(metrics) != 2 {
		t.Fatalf("expected 2 metrics in NBIRTH, got %d", len(metrics))
	}
	if metrics[0].GetName() != metricBdSeq || metrics[0].GetLongValue() != 5 {
		t.Errorf("unexpected bdSeq metric: name=%s value=%d", metrics[0].GetName(), metrics[0].GetLongValue())
	}
	if metrics[1].GetName() != metricNodeRebirth || metrics[1].GetBooleanValue() != false {
		t.Errorf("unexpected Node Control/Rebirth metric: name=%s value=%v", metrics[1].GetName(), metrics[1].GetBooleanValue())
	}
}

func TestPublishDeviceData_AutoBirthsOnFirstSighting(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)

	metrics := []Metric{{Name: "temperature", Timestamp: time.Now(), DataType: spplugb.DataType_Double, Value: 21.5}}
	if err := n.PublishDeviceData("Thermostat1", metrics); err != nil {
		t.Fatalf("PublishDeviceData() error = %v", err)
	}

	if len(client.published) != 1 {
		t.Fatalf("expected 1 publish (DBIRTH), got %d", len(client.published))
	}
	got := client.published[0]
	if got.topic != "spBv1.0/TestGroup/DBIRTH/TestNode/Thermostat1" {
		t.Errorf("unexpected topic: %s", got.topic)
	}
	if len(got.payload.GetMetrics()) != 1 || got.payload.GetMetrics()[0].GetName() != "temperature" {
		t.Errorf("unexpected DBIRTH metrics: %+v", got.payload.GetMetrics())
	}
	if !n.devices["Thermostat1"].born {
		t.Error("expected device to be marked born after DBIRTH")
	}
}

func TestPublishDeviceData_SubsequentPublishIsDataWithoutName(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)

	metrics := []Metric{{Name: "temperature", Timestamp: time.Now(), DataType: spplugb.DataType_Double, Value: 21.5}}
	if err := n.PublishDeviceData("Thermostat1", metrics); err != nil {
		t.Fatalf("PublishDeviceData() error = %v", err)
	}
	if err := n.PublishDeviceData("Thermostat1", metrics); err != nil {
		t.Fatalf("PublishDeviceData() error = %v", err)
	}

	if len(client.published) != 2 {
		t.Fatalf("expected 2 publishes (DBIRTH, DDATA), got %d", len(client.published))
	}

	ddata := client.published[1]
	if !strings.Contains(ddata.topic, "/DDATA/") {
		t.Errorf("expected second publish to be DDATA, topic = %s", ddata.topic)
	}
	dataMetrics := ddata.payload.GetMetrics()
	if len(dataMetrics) != 1 {
		t.Fatalf("expected 1 metric in DDATA, got %d", len(dataMetrics))
	}
	if dataMetrics[0].GetName() != "" {
		t.Errorf("DDATA metric should omit name, got %q", dataMetrics[0].GetName())
	}
	if dataMetrics[0].GetAlias() != firstDeviceAlias {
		t.Errorf("DDATA metric alias = %d, want %d", dataMetrics[0].GetAlias(), firstDeviceAlias)
	}
}

func TestPublishDeviceData_NewMetricTriggersAnotherBirth(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)

	if err := n.PublishDeviceData("Thermostat1", []Metric{
		{Name: "temperature", Timestamp: time.Now(), DataType: spplugb.DataType_Double, Value: 21.5},
	}); err != nil {
		t.Fatalf("PublishDeviceData() error = %v", err)
	}
	if err := n.PublishDeviceData("Thermostat1", []Metric{
		{Name: "temperature", Timestamp: time.Now(), DataType: spplugb.DataType_Double, Value: 21.5},
		{Name: "humidity", Timestamp: time.Now(), DataType: spplugb.DataType_Double, Value: 55.0},
	}); err != nil {
		t.Fatalf("PublishDeviceData() error = %v", err)
	}

	if len(client.published) != 2 {
		t.Fatalf("expected 2 publishes (DBIRTH, DBIRTH), got %d", len(client.published))
	}
	second := client.published[1]
	if !strings.Contains(second.topic, "/DBIRTH/") {
		t.Errorf("expected second publish to be a DBIRTH due to new metric, topic = %s", second.topic)
	}
	if len(second.payload.GetMetrics()) != 2 {
		t.Errorf("expected DBIRTH to include both metrics, got %d", len(second.payload.GetMetrics()))
	}
}

func TestSeq_WrapsAt256(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	n.seq = 255

	payload := n.newPayload(nil)
	if payload.GetSeq() != 255 {
		t.Errorf("expected seq 255, got %d", payload.GetSeq())
	}
	if n.seq != 0 {
		t.Errorf("expected seq to wrap to 0 after 255, got %d", n.seq)
	}
}

func TestHandleNCmd_RebirthCommandRepublishesBirths(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)

	if err := n.PublishDeviceData("Thermostat1", []Metric{
		{Name: "temperature", Timestamp: time.Now(), DataType: spplugb.DataType_Double, Value: 21.5},
	}); err != nil {
		t.Fatalf("PublishDeviceData() error = %v", err)
	}
	client.published = nil // reset; only interested in what the rebirth command triggers

	cmdPayload, err := proto.Marshal(&spplugb.Payload{
		Metrics: []*spplugb.Payload_Metric{
			{
				Name:  proto.String(metricNodeRebirth),
				Value: &spplugb.Payload_Metric_BooleanValue{BooleanValue: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to marshal NCMD payload: %v", err)
	}

	n.handleNCmd(client, &fakeMessage{topic: "spBv1.0/TestGroup/NCMD/TestNode", payload: cmdPayload})

	if len(client.published) != 2 {
		t.Fatalf("expected NBIRTH + DBIRTH after rebirth command, got %d publishes", len(client.published))
	}
	if !strings.Contains(client.published[0].topic, "/NBIRTH/") {
		t.Errorf("expected first republish to be NBIRTH, got %s", client.published[0].topic)
	}
	if !strings.Contains(client.published[1].topic, "/DBIRTH/") {
		t.Errorf("expected second republish to be DBIRTH, got %s", client.published[1].topic)
	}
	if client.published[0].payload.GetSeq() != 0 {
		t.Errorf("expected seq to reset to 0 on rebirth, got %d", client.published[0].payload.GetSeq())
	}
}

func TestDeathPayload_CarriesBdSeq(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	n.bdSeq = 42

	data, err := n.deathPayload()
	if err != nil {
		t.Fatalf("deathPayload() error = %v", err)
	}

	var payload spplugb.Payload
	if err := proto.Unmarshal(data, &payload); err != nil {
		t.Fatalf("failed to unmarshal death payload: %v", err)
	}
	if len(payload.GetMetrics()) != 1 || payload.GetMetrics()[0].GetName() != metricBdSeq {
		t.Fatalf("unexpected death payload metrics: %+v", payload.GetMetrics())
	}
	if payload.GetMetrics()[0].GetLongValue() != 42 {
		t.Errorf("death payload bdSeq = %d, want 42", payload.GetMetrics()[0].GetLongValue())
	}
}

func TestNewNode_RequiresGroupEdgeNodeAndBroker(t *testing.T) {
	_, err := NewNode(Config{AuthMode: "none"}, nil, logger.NewMockClient())
	if err == nil {
		t.Fatal("expected error for missing required config fields")
	}
}
