package sparkplug

import (
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	"github.com/edgexfoundry/app-service-configurable/internal/sparkplug/spplugb"
)

func TestMetricFromReading(t *testing.T) {
	tests := []struct {
		name      string
		valueType string
		value     string
		binary    []byte
		wantType  spplugb.DataType
		wantValue any
	}{
		{name: "bool true", valueType: common.ValueTypeBool, value: "true", wantType: spplugb.DataType_Boolean, wantValue: true},
		{name: "string", valueType: common.ValueTypeString, value: "hello", wantType: spplugb.DataType_String, wantValue: "hello"},
		{name: "uint8", valueType: common.ValueTypeUint8, value: "200", wantType: spplugb.DataType_UInt8, wantValue: uint64(200)},
		{name: "uint16", valueType: common.ValueTypeUint16, value: "60000", wantType: spplugb.DataType_UInt16, wantValue: uint64(60000)},
		{name: "uint32", valueType: common.ValueTypeUint32, value: "4000000000", wantType: spplugb.DataType_UInt32, wantValue: uint64(4000000000)},
		{name: "uint64", valueType: common.ValueTypeUint64, value: "18000000000000000000", wantType: spplugb.DataType_UInt64, wantValue: uint64(18000000000000000000)},
		{name: "int8", valueType: common.ValueTypeInt8, value: "-100", wantType: spplugb.DataType_Int8, wantValue: int64(-100)},
		{name: "int16", valueType: common.ValueTypeInt16, value: "-30000", wantType: spplugb.DataType_Int16, wantValue: int64(-30000)},
		{name: "int32", valueType: common.ValueTypeInt32, value: "-2000000000", wantType: spplugb.DataType_Int32, wantValue: int64(-2000000000)},
		{name: "int64", valueType: common.ValueTypeInt64, value: "-9000000000000000000", wantType: spplugb.DataType_Int64, wantValue: int64(-9000000000000000000)},
		{name: "float32", valueType: common.ValueTypeFloat32, value: "3.5", wantType: spplugb.DataType_Float, wantValue: float32(3.5)},
		{name: "float64", valueType: common.ValueTypeFloat64, value: "3.14159", wantType: spplugb.DataType_Double, wantValue: 3.14159},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reading := dtos.BaseReading{
				ResourceName: "resource",
				DeviceName:   "device",
				ValueType:    tc.valueType,
				SimpleReading: dtos.SimpleReading{
					Value: tc.value,
				},
			}

			metric, err := MetricFromReading(reading)
			if err != nil {
				t.Fatalf("MetricFromReading() error = %v", err)
			}
			if metric.Name != "resource" {
				t.Errorf("Name = %q, want %q", metric.Name, "resource")
			}
			if metric.DataType != tc.wantType {
				t.Errorf("DataType = %v, want %v", metric.DataType, tc.wantType)
			}
			if metric.Value != tc.wantValue {
				t.Errorf("Value = %#v (%T), want %#v (%T)", metric.Value, metric.Value, tc.wantValue, tc.wantValue)
			}
		})
	}
}

func TestMetricFromReading_Binary(t *testing.T) {
	reading := dtos.BaseReading{
		ResourceName: "resource",
		DeviceName:   "device",
		ValueType:    common.ValueTypeBinary,
		BinaryReading: dtos.BinaryReading{
			BinaryValue: []byte{1, 2, 3},
		},
	}

	metric, err := MetricFromReading(reading)
	if err != nil {
		t.Fatalf("MetricFromReading() error = %v", err)
	}
	if metric.DataType != spplugb.DataType_Bytes {
		t.Errorf("DataType = %v, want %v", metric.DataType, spplugb.DataType_Bytes)
	}
	got, ok := metric.Value.([]byte)
	if !ok || string(got) != string([]byte{1, 2, 3}) {
		t.Errorf("Value = %#v, want %#v", metric.Value, []byte{1, 2, 3})
	}
}

func TestMetricFromReading_UnsupportedValueTypeReturnsError(t *testing.T) {
	reading := dtos.BaseReading{
		ResourceName: "resource",
		DeviceName:   "device",
		ValueType:    common.ValueTypeStringArray,
		SimpleReading: dtos.SimpleReading{
			Value: `["a","b"]`,
		},
	}

	if _, err := MetricFromReading(reading); err == nil {
		t.Fatal("expected error for unsupported value type, got nil")
	}
}

func TestMetricFromReading_InvalidValueReturnsError(t *testing.T) {
	reading := dtos.BaseReading{
		ResourceName: "resource",
		DeviceName:   "device",
		ValueType:    common.ValueTypeInt32,
		SimpleReading: dtos.SimpleReading{
			Value: "not-a-number",
		},
	}

	if _, err := MetricFromReading(reading); err == nil {
		t.Fatal("expected error for unparsable value, got nil")
	}
}

func TestMetricFromReading_Timestamp(t *testing.T) {
	origin := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano()
	reading := dtos.BaseReading{
		ResourceName: "resource",
		Origin:       origin,
		ValueType:    common.ValueTypeBool,
		SimpleReading: dtos.SimpleReading{
			Value: "true",
		},
	}

	metric, err := MetricFromReading(reading)
	if err != nil {
		t.Fatalf("MetricFromReading() error = %v", err)
	}
	if !metric.Timestamp.Equal(time.Unix(0, origin)) {
		t.Errorf("Timestamp = %v, want %v", metric.Timestamp, time.Unix(0, origin))
	}
}

func TestSetProtoValue_AllSupportedTypes(t *testing.T) {
	tests := []struct {
		name     string
		dataType spplugb.DataType
		value    any
	}{
		{"bool", spplugb.DataType_Boolean, true},
		{"string", spplugb.DataType_String, "hi"},
		{"uint8", spplugb.DataType_UInt8, uint64(1)},
		{"uint16", spplugb.DataType_UInt16, uint64(1)},
		{"uint32", spplugb.DataType_UInt32, uint64(1)},
		{"uint64", spplugb.DataType_UInt64, uint64(1)},
		{"int8", spplugb.DataType_Int8, int64(-1)},
		{"int16", spplugb.DataType_Int16, int64(-1)},
		{"int32", spplugb.DataType_Int32, int64(-1)},
		{"int64", spplugb.DataType_Int64, int64(-1)},
		{"float", spplugb.DataType_Float, float32(1.5)},
		{"double", spplugb.DataType_Double, 1.5},
		{"bytes", spplugb.DataType_Bytes, []byte{1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pm := &spplugb.Payload_Metric{}
			if err := setProtoValue(pm, tc.dataType, tc.value); err != nil {
				t.Fatalf("setProtoValue() error = %v", err)
			}
			if pm.Value == nil {
				t.Fatal("expected Value to be set")
			}
		})
	}
}

func TestSetProtoValue_UnsupportedDataType(t *testing.T) {
	pm := &spplugb.Payload_Metric{}
	if err := setProtoValue(pm, spplugb.DataType_DataSet, nil); err == nil {
		t.Fatal("expected error for unsupported data type")
	}
}

func TestSetProtoValue_NilValueIsNull(t *testing.T) {
	pm := &spplugb.Payload_Metric{}
	if err := setProtoValue(pm, spplugb.DataType_Double, nil); err != nil {
		t.Fatalf("setProtoValue(nil) error = %v", err)
	}
	if !pm.GetIsNull() || pm.Value != nil {
		t.Errorf("expected is_null with no value, got is_null=%v value=%v", pm.GetIsNull(), pm.Value)
	}
	if err := setProtoValue(&spplugb.Payload_Metric{}, spplugb.DataType_DataSet, nil); err == nil {
		t.Error("expected error for a nil value of an unsupported data type")
	}
}

func TestDataTypeFor(t *testing.T) {
	if got, ok := dataTypeFor(common.ValueTypeFloat32); !ok || got != spplugb.DataType_Float {
		t.Errorf("dataTypeFor(Float32) = %v, %v", got, ok)
	}
	if _, ok := dataTypeFor(common.ValueTypeObject); ok {
		t.Error("expected Object to be unsupported")
	}
}
