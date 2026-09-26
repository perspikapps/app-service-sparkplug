package sparkplug

import (
	"context"
	"testing"
	"time"

	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"google.golang.org/protobuf/proto"

	"github.com/edgexfoundry/app-service-configurable/internal/sparkplug/spplugb"
)

type setCall struct {
	device   string
	command  string
	settings map[string]any
}

type fakeCommands struct {
	calls []setCall
}

func (f *fakeCommands) IssueSetCommandByName(_ context.Context, deviceName, commandName string, settings map[string]any) (dtoCommon.BaseResponse, errors.EdgeX) {
	f.calls = append(f.calls, setCall{deviceName, commandName, settings})
	return dtoCommon.BaseResponse{}, nil
}

// commandNode returns a Node with commands enabled and Thermostat1 registered from its profile.
func commandNode(t *testing.T) (*Node, *fakeClient, *fakeCommands) {
	t.Helper()
	client := newFakeClient()
	n := testNode(client)
	metadata := newFakeMetadata("Thermostat1")
	commands := &fakeCommands{}
	n.EnableCommands(commands, metadata)
	n.pollDevices(context.Background(), metadata, metadata)
	return n, client, commands
}

func dcmd(t *testing.T, device string, metrics ...*spplugb.Payload_Metric) *fakeMessage {
	t.Helper()
	data, err := proto.Marshal(&spplugb.Payload{Timestamp: proto.Uint64(1), Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	return &fakeMessage{topic: "spBv1.0/TestGroup/DCMD/TestNode/" + device, payload: data}
}

func TestOnConnect_SubscribesToDCmdOnlyWhenEnabled(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	if err := n.onConnect(); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.subscribed["spBv1.0/TestGroup/DCMD/TestNode/+"]; ok {
		t.Error("DCMD must not be subscribed while commands are disabled")
	}

	n, client, _ = commandNode(t)
	if err := n.onConnect(); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.subscribed["spBv1.0/TestGroup/DCMD/TestNode/+"]; !ok {
		t.Error("expected a DCMD subscription with commands enabled")
	}
}

func TestEnableCommands_NilClientsLeaveCommandsDisabled(t *testing.T) {
	n := testNode(newFakeClient())
	n.EnableCommands(nil, nil)
	if n.commands != nil {
		t.Error("expected commands to stay disabled")
	}
}

func TestHandleDCmd_ByNameIssuesSet(t *testing.T) {
	n, client, commands := commandNode(t)
	n.handleDCmd(client, dcmd(t, "Thermostat1", &spplugb.Payload_Metric{
		Name:  proto.String("Floor1/setpoint"),
		Value: &spplugb.Payload_Metric_DoubleValue{DoubleValue: 21.5},
	}))

	if len(commands.calls) != 1 {
		t.Fatalf("expected 1 SET command, got %+v", commands.calls)
	}
	got := commands.calls[0]
	if got.device != "Thermostat1" || got.command != "setpoint" || got.settings["setpoint"] != "21.5" {
		t.Errorf("unexpected SET command: %+v", got)
	}
}

func TestHandleDCmd_ByAliasIssuesSet(t *testing.T) {
	n, client, commands := commandNode(t)
	alias := n.devices["Thermostat1"].aliases["Floor1/enabled"]
	n.handleDCmd(client, dcmd(t, "Thermostat1", &spplugb.Payload_Metric{
		Alias: proto.Uint64(alias),
		Value: &spplugb.Payload_Metric_BooleanValue{BooleanValue: true},
	}))

	if len(commands.calls) != 1 || commands.calls[0].command != "enabled" || commands.calls[0].settings["enabled"] != "true" {
		t.Errorf("unexpected SET commands: %+v", commands.calls)
	}
}

func TestHandleDCmd_RejectsReadOnlyUnknownAndMistyped(t *testing.T) {
	n, client, commands := commandNode(t)
	n.handleDCmd(client, dcmd(t, "Thermostat1",
		&spplugb.Payload_Metric{Name: proto.String("HVAC/Floor1/temperature"), Value: &spplugb.Payload_Metric_DoubleValue{DoubleValue: 1}},
		&spplugb.Payload_Metric{Name: proto.String("nope"), Value: &spplugb.Payload_Metric_DoubleValue{DoubleValue: 1}},
		&spplugb.Payload_Metric{Name: proto.String("Floor1/enabled"), Value: &spplugb.Payload_Metric_StringValue{StringValue: "yes"}},
		&spplugb.Payload_Metric{Name: proto.String("Floor1/setpoint"), IsNull: proto.Bool(true)},
	))
	n.handleDCmd(client, dcmd(t, "UnknownDevice", &spplugb.Payload_Metric{
		Name: proto.String("Floor1/setpoint"), Value: &spplugb.Payload_Metric_DoubleValue{DoubleValue: 1},
	}))

	if len(commands.calls) != 0 {
		t.Errorf("expected no SET commands, got %+v", commands.calls)
	}
}

func TestHandleDCmd_DataBornMetric(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	metadata := newFakeMetadata()
	commands := &fakeCommands{}
	n.EnableCommands(commands, metadata)
	if err := n.PublishDeviceData("Thermostat1", "ThermostatProfile", []Metric{
		{Name: "setpoint", ResourceName: "setpoint", Timestamp: time.Now(), DataType: spplugb.DataType_Double, Value: 20.0},
	}); err != nil {
		t.Fatal(err)
	}

	n.handleDCmd(client, dcmd(t, "Thermostat1", &spplugb.Payload_Metric{
		Name: proto.String("setpoint"), Value: &spplugb.Payload_Metric_DoubleValue{DoubleValue: 19},
	}))
	if len(commands.calls) != 1 || commands.calls[0].settings["setpoint"] != "19" {
		t.Errorf("unexpected SET commands: %+v", commands.calls)
	}
}

func TestCommandValue(t *testing.T) {
	tests := []struct {
		name     string
		dataType spplugb.DataType
		metric   *spplugb.Payload_Metric
		want     string
		wantErr  bool
	}{
		{"bool", spplugb.DataType_Boolean, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_BooleanValue{BooleanValue: true}}, "true", false},
		{"string", spplugb.DataType_String, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_StringValue{StringValue: "on"}}, "on", false},
		{"uint16", spplugb.DataType_UInt16, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_IntValue{IntValue: 65535}}, "65535", false},
		{"uint64", spplugb.DataType_UInt64, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_LongValue{LongValue: 1 << 40}}, "1099511627776", false},
		{"int8 negative", spplugb.DataType_Int8, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_IntValue{IntValue: 0xFFFFFFFF}}, "-1", false},
		{"int32 in long", spplugb.DataType_Int32, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_LongValue{LongValue: 0xFFFFFFFFFFFFFFFE}}, "-2", false},
		{"int64 negative", spplugb.DataType_Int64, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_LongValue{LongValue: 0xFFFFFFFFFFFFFFFF}}, "-1", false},
		{"float", spplugb.DataType_Float, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_FloatValue{FloatValue: 1.5}}, "1.5", false},
		{"double from float", spplugb.DataType_Double, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_FloatValue{FloatValue: 2.25}}, "2.25", false},
		{"bytes unsupported", spplugb.DataType_Bytes, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_BytesValue{BytesValue: []byte{1}}}, "", true},
		{"type mismatch", spplugb.DataType_Double, &spplugb.Payload_Metric{Value: &spplugb.Payload_Metric_StringValue{StringValue: "x"}}, "", true},
		{"null", spplugb.DataType_Double, &spplugb.Payload_Metric{IsNull: proto.Bool(true)}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := commandValue(tt.metric, tt.dataType)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("commandValue() = %q, %v; want %q, error %v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}
