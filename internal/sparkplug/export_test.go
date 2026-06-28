package sparkplug

import (
	"testing"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces/mocks"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
)

func testContext() *mocks.AppFunctionContext {
	ctx := &mocks.AppFunctionContext{}
	ctx.On("LoggingClient").Return(logger.NewMockClient())
	return ctx
}

func TestNewExportFunction_PublishesReadingsAsDeviceData(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	fn := NewExportFunction(n)

	event := dtos.Event{
		DeviceName: "Thermostat1",
		Readings: []dtos.BaseReading{
			{
				ResourceName: "temperature",
				DeviceName:   "Thermostat1",
				ValueType:    common.ValueTypeFloat64,
				SimpleReading: dtos.SimpleReading{
					Value: "21.5",
				},
			},
		},
	}

	continuePipeline, result := fn(testContext(), event)
	if !continuePipeline {
		t.Fatalf("expected pipeline to continue, got error result: %v", result)
	}

	if len(client.published) != 1 {
		t.Fatalf("expected 1 publish (DBIRTH), got %d", len(client.published))
	}
	got := client.published[0]
	if got.topic != "spBv1.0/TestGroup/DBIRTH/TestNode/Thermostat1" {
		t.Errorf("unexpected topic: %s", got.topic)
	}
	metrics := got.payload.GetMetrics()
	if len(metrics) != 1 || metrics[0].GetName() != "temperature" {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
	if metrics[0].GetDoubleValue() != 21.5 {
		t.Errorf("metric value = %v, want 21.5", metrics[0].GetDoubleValue())
	}
}

func TestNewExportFunction_SkipsUnsupportedReadingsButPublishesRest(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	fn := NewExportFunction(n)

	event := dtos.Event{
		DeviceName: "Thermostat1",
		Readings: []dtos.BaseReading{
			{
				ResourceName: "tags",
				DeviceName:   "Thermostat1",
				ValueType:    common.ValueTypeStringArray,
				SimpleReading: dtos.SimpleReading{
					Value: `["a","b"]`,
				},
			},
			{
				ResourceName: "temperature",
				DeviceName:   "Thermostat1",
				ValueType:    common.ValueTypeFloat64,
				SimpleReading: dtos.SimpleReading{
					Value: "21.5",
				},
			},
		},
	}

	continuePipeline, result := fn(testContext(), event)
	if !continuePipeline {
		t.Fatalf("expected pipeline to continue, got error result: %v", result)
	}

	if len(client.published) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(client.published))
	}
	metrics := client.published[0].payload.GetMetrics()
	if len(metrics) != 1 || metrics[0].GetName() != "temperature" {
		t.Fatalf("expected only the supported reading to be published, got %+v", metrics)
	}
}

func TestNewExportFunction_NoSupportedReadingsPublishesNothing(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	fn := NewExportFunction(n)

	event := dtos.Event{
		DeviceName: "Thermostat1",
		Readings: []dtos.BaseReading{
			{
				ResourceName: "tags",
				DeviceName:   "Thermostat1",
				ValueType:    common.ValueTypeStringArray,
				SimpleReading: dtos.SimpleReading{
					Value: `["a","b"]`,
				},
			},
		},
	}

	continuePipeline, result := fn(testContext(), event)
	if !continuePipeline {
		t.Fatalf("expected pipeline to continue, got error result: %v", result)
	}
	if len(client.published) != 0 {
		t.Fatalf("expected no publishes, got %d", len(client.published))
	}
}

func TestNewExportFunction_NilDataReturnsError(t *testing.T) {
	fn := NewExportFunction(testNode(newFakeClient()))

	continuePipeline, _ := fn(testContext(), nil)
	if continuePipeline {
		t.Fatal("expected pipeline to stop on nil data")
	}
}

func TestNewExportFunction_WrongTypeReturnsError(t *testing.T) {
	fn := NewExportFunction(testNode(newFakeClient()))

	continuePipeline, _ := fn(testContext(), "not an event")
	if continuePipeline {
		t.Fatal("expected pipeline to stop on wrong data type")
	}
}
