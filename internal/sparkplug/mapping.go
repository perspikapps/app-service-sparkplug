package sparkplug

import (
	"fmt"
	"strconv"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"google.golang.org/protobuf/proto"

	"github.com/edgexfoundry/app-service-configurable/internal/sparkplug/spplugb"
)

// Metric is the package's intermediate representation of a single Sparkplug metric value,
// produced from an EdgeX reading and later turned into a *spplugb.Payload_Metric by the
// node's payload builder.
type Metric struct {
	// Name is the Sparkplug metric name, built from Payload.MetricNameFormat.
	Name string
	// ResourceName is the EdgeX device resource the metric came from, used to route DCMD writes.
	ResourceName string
	Timestamp    time.Time
	DataType     spplugb.DataType
	// Value is nil for a metric that is declared but has no value yet (published with is_null).
	Value any
}

// MetricFromReading converts a single EdgeX reading into a Sparkplug Metric, named by the bare
// resource name; callers apply Payload.MetricNameFormat afterwards (see NewExportFunction).
// Object and Array value types are not currently supported and result in an error so the
// caller can skip just that reading rather than the whole event.
func MetricFromReading(reading dtos.BaseReading) (Metric, error) {
	dataType, value, err := mapValue(reading)
	if err != nil {
		return Metric{}, fmt.Errorf("reading '%s' on device '%s': %w", reading.ResourceName, reading.DeviceName, err)
	}

	return Metric{
		Name:         reading.ResourceName,
		ResourceName: reading.ResourceName,
		Timestamp:    time.Unix(0, reading.Origin),
		DataType:     dataType,
		Value:        value,
	}, nil
}

// dataTypeFor returns the Sparkplug DataType for an EdgeX value type, or false if the value type
// isn't supported for Sparkplug export.
func dataTypeFor(valueType string) (spplugb.DataType, bool) {
	dataType, ok := valueTypeDataTypes[valueType]
	return dataType, ok
}

var valueTypeDataTypes = map[string]spplugb.DataType{
	common.ValueTypeBool:    spplugb.DataType_Boolean,
	common.ValueTypeString:  spplugb.DataType_String,
	common.ValueTypeUint8:   spplugb.DataType_UInt8,
	common.ValueTypeUint16:  spplugb.DataType_UInt16,
	common.ValueTypeUint32:  spplugb.DataType_UInt32,
	common.ValueTypeUint64:  spplugb.DataType_UInt64,
	common.ValueTypeInt8:    spplugb.DataType_Int8,
	common.ValueTypeInt16:   spplugb.DataType_Int16,
	common.ValueTypeInt32:   spplugb.DataType_Int32,
	common.ValueTypeInt64:   spplugb.DataType_Int64,
	common.ValueTypeFloat32: spplugb.DataType_Float,
	common.ValueTypeFloat64: spplugb.DataType_Double,
	common.ValueTypeBinary:  spplugb.DataType_Bytes,
}

// mapValue maps an EdgeX reading's ValueType/Value to a Sparkplug DataType and a native Go
// value of the type expected by toProtoValue for that DataType.
func mapValue(reading dtos.BaseReading) (spplugb.DataType, any, error) {
	switch reading.ValueType {
	case common.ValueTypeBool:
		v, err := strconv.ParseBool(reading.Value)
		return spplugb.DataType_Boolean, v, err
	case common.ValueTypeString:
		return spplugb.DataType_String, reading.Value, nil
	case common.ValueTypeUint8:
		v, err := strconv.ParseUint(reading.Value, 10, 8)
		return spplugb.DataType_UInt8, v, err
	case common.ValueTypeUint16:
		v, err := strconv.ParseUint(reading.Value, 10, 16)
		return spplugb.DataType_UInt16, v, err
	case common.ValueTypeUint32:
		v, err := strconv.ParseUint(reading.Value, 10, 32)
		return spplugb.DataType_UInt32, v, err
	case common.ValueTypeUint64:
		v, err := strconv.ParseUint(reading.Value, 10, 64)
		return spplugb.DataType_UInt64, v, err
	case common.ValueTypeInt8:
		v, err := strconv.ParseInt(reading.Value, 10, 8)
		return spplugb.DataType_Int8, v, err
	case common.ValueTypeInt16:
		v, err := strconv.ParseInt(reading.Value, 10, 16)
		return spplugb.DataType_Int16, v, err
	case common.ValueTypeInt32:
		v, err := strconv.ParseInt(reading.Value, 10, 32)
		return spplugb.DataType_Int32, v, err
	case common.ValueTypeInt64:
		v, err := strconv.ParseInt(reading.Value, 10, 64)
		return spplugb.DataType_Int64, v, err
	case common.ValueTypeFloat32:
		v, err := strconv.ParseFloat(reading.Value, 32)
		return spplugb.DataType_Float, float32(v), err
	case common.ValueTypeFloat64:
		v, err := strconv.ParseFloat(reading.Value, 64)
		return spplugb.DataType_Double, v, err
	case common.ValueTypeBinary:
		return spplugb.DataType_Bytes, reading.BinaryValue, nil
	default:
		return spplugb.DataType_Unknown, nil, fmt.Errorf("unsupported value type '%s' for sparkplug export", reading.ValueType)
	}
}

// setProtoValue sets the oneof Value field on pm for the given DataType/value, as produced by
// mapValue/MetricFromReading. The oneof's interface type is unexported by protoc-gen-go, so the
// concrete *spplugb.Payload_Metric_XxxValue wrapper is assigned to pm.Value directly in each case
// rather than being returned, which is the normal way to set a proto oneof from another package.
//
// A nil value marks the metric as declared but without a value (Sparkplug's is_null), used when a
// device is born from its core-metadata profile before any reading has arrived.
func setProtoValue(pm *spplugb.Payload_Metric, dataType spplugb.DataType, value any) error {
	if value == nil {
		if _, ok := protoDataTypes[dataType]; !ok {
			return fmt.Errorf("unsupported sparkplug data type '%s'", dataType)
		}
		pm.IsNull = proto.Bool(true)
		return nil
	}
	switch dataType {
	case spplugb.DataType_Boolean:
		pm.Value = &spplugb.Payload_Metric_BooleanValue{BooleanValue: value.(bool)}
	case spplugb.DataType_String:
		pm.Value = &spplugb.Payload_Metric_StringValue{StringValue: value.(string)}
	case spplugb.DataType_UInt8, spplugb.DataType_UInt16, spplugb.DataType_UInt32:
		// value was parsed by strconv.ParseUint with a bit size matching dataType, so it always
		// fits in 32 bits.
		pm.Value = &spplugb.Payload_Metric_IntValue{IntValue: uint32(value.(uint64))} //nolint:gosec
	case spplugb.DataType_UInt64:
		pm.Value = &spplugb.Payload_Metric_LongValue{LongValue: value.(uint64)}
	case spplugb.DataType_Int8, spplugb.DataType_Int16, spplugb.DataType_Int32:
		// Sparkplug B carries 8/16/32-bit signed integers in the same uint32 "int_value"
		// field as the unsigned variants; the sign bit pattern is preserved by the cast
		// and reinterpreted by the consumer based on the metric's declared DataType.
		pm.Value = &spplugb.Payload_Metric_IntValue{IntValue: uint32(int32(value.(int64)))} //nolint:gosec
	case spplugb.DataType_Int64:
		// Same bit-pattern reinterpretation as above, for the 64-bit "long_value" field.
		pm.Value = &spplugb.Payload_Metric_LongValue{LongValue: uint64(value.(int64))} //nolint:gosec
	case spplugb.DataType_Float:
		pm.Value = &spplugb.Payload_Metric_FloatValue{FloatValue: value.(float32)}
	case spplugb.DataType_Double:
		pm.Value = &spplugb.Payload_Metric_DoubleValue{DoubleValue: value.(float64)}
	case spplugb.DataType_Bytes:
		pm.Value = &spplugb.Payload_Metric_BytesValue{BytesValue: value.([]byte)}
	default:
		return fmt.Errorf("unsupported sparkplug data type '%s'", dataType)
	}
	return nil
}

// protoDataTypes is the set of DataTypes setProtoValue supports.
var protoDataTypes = func() map[spplugb.DataType]bool {
	types := make(map[spplugb.DataType]bool, len(valueTypeDataTypes))
	for _, dataType := range valueTypeDataTypes {
		types[dataType] = true
	}
	return types
}()
