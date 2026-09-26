package sparkplug

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"google.golang.org/protobuf/proto"

	"github.com/edgexfoundry/app-service-configurable/internal/sparkplug/spplugb"
)

// commandTimeout bounds each SET command issued to core-command for a DCMD.
const commandTimeout = 10 * time.Second

// commandIssuer is the part of the SDK's CommandClient used to forward DCMD writes.
type commandIssuer interface {
	IssueSetCommandByName(ctx context.Context, deviceName string, commandName string, settings map[string]any) (dtoCommon.BaseResponse, errors.EdgeX)
}

// EnableCommands lets DCMD messages write to devices through core-command. Call it before Start so
// the DCMD subscription is made on connect. Both clients are required: profiles is how a DCMD is
// checked to target a writable resource. A nil client leaves commands disabled.
func (n *Node) EnableCommands(commands commandIssuer, profiles profileGetter) {
	if commands == nil || profiles == nil {
		n.lc.Warn("sparkplug: Commands.Enabled is true but core-command/core-metadata clients are not configured; DCMD will be ignored")
		return
	}
	n.commands = commands
	n.profiles = profiles
}

// dcmdWrite is one DCMD metric resolved to the EdgeX resource it writes.
type dcmdWrite struct {
	metricName   string
	resourceName string
	value        string
}

// handleDCmd forwards each metric of a DCMD as an EdgeX SET command on the device resource it was
// born from. Metrics that are unknown, have no value, or target a resource the device profile
// doesn't mark writable are logged and skipped. The written value reaches the host as normal DDATA
// once the device reports it.
func (n *Node) handleDCmd(_ mqtt.Client, msg mqtt.Message) {
	parts := strings.Split(msg.Topic(), "/")
	if len(parts) != 5 {
		n.lc.Warnf("sparkplug: ignoring DCMD on unexpected topic '%s'", msg.Topic())
		return
	}
	deviceName := parts[4]

	var payload spplugb.Payload
	if err := proto.Unmarshal(msg.Payload(), &payload); err != nil {
		n.lc.Errorf("sparkplug: failed to decode DCMD payload for device '%s': %v", deviceName, err)
		return
	}

	writes, profileName := n.resolveDCmd(deviceName, payload.GetMetrics())
	if len(writes) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()

	response, err := n.profiles.DeviceProfileByName(ctx, profileName)
	if err != nil {
		n.lc.Errorf("sparkplug: failed to get profile '%s' to check DCMD for device '%s': %v", profileName, deviceName, err)
		return
	}
	writable := make(map[string]bool, len(response.Profile.DeviceResources))
	for _, resource := range response.Profile.DeviceResources {
		switch resource.Properties.ReadWrite {
		case common.ReadWrite_W, common.ReadWrite_RW, common.ReadWrite_WR:
			writable[resource.Name] = true
		}
	}

	for _, w := range writes {
		if !writable[w.resourceName] {
			n.lc.Warnf("sparkplug: ignoring DCMD metric '%s' for device '%s': resource '%s' is not writable", w.metricName, deviceName, w.resourceName)
			continue
		}
		settings := map[string]any{w.resourceName: w.value}
		if _, err := n.commands.IssueSetCommandByName(ctx, deviceName, w.resourceName, settings); err != nil {
			n.lc.Errorf("sparkplug: failed to issue SET command '%s' on device '%s': %v", w.resourceName, deviceName, err)
			continue
		}
		n.lc.Infof("sparkplug: successfully issued SET command '%s' on device '%s' with value '%s'", w.resourceName, deviceName, w.value)
	}
}

// resolveDCmd maps DCMD metrics, by name or alias, to the device's known metrics and converts each
// value to the string form EdgeX SET commands take.
func (n *Node) resolveDCmd(deviceName string, metrics []*spplugb.Payload_Metric) ([]dcmdWrite, string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	d, ok := n.devices[deviceName]
	if !ok {
		n.lc.Warnf("sparkplug: ignoring DCMD for unknown device '%s'", deviceName)
		return nil, ""
	}

	writes := make([]dcmdWrite, 0, len(metrics))
	for _, pm := range metrics {
		known, ok := d.metric(pm.GetName(), pm.Alias)
		if !ok {
			n.lc.Warnf("sparkplug: ignoring DCMD for unknown metric '%s' (alias %d) on device '%s'", pm.GetName(), pm.GetAlias(), deviceName)
			continue
		}
		value, err := commandValue(pm, known.DataType)
		if err != nil {
			n.lc.Warnf("sparkplug: ignoring DCMD metric '%s' on device '%s': %v", known.Name, deviceName, err)
			continue
		}
		writes = append(writes, dcmdWrite{metricName: known.Name, resourceName: known.ResourceName, value: value})
	}
	return writes, d.profileName
}

// commandValue converts a DCMD metric's value to EdgeX's string representation for dataType, the
// type the metric was born with (a host may omit the datatype in a DCMD).
func commandValue(pm *spplugb.Payload_Metric, dataType spplugb.DataType) (string, error) {
	if pm.GetIsNull() || pm.Value == nil {
		return "", fmt.Errorf("no value")
	}

	switch dataType {
	case spplugb.DataType_Boolean:
		if v, ok := pm.Value.(*spplugb.Payload_Metric_BooleanValue); ok {
			return strconv.FormatBool(v.BooleanValue), nil
		}
	case spplugb.DataType_String:
		if v, ok := pm.Value.(*spplugb.Payload_Metric_StringValue); ok {
			return v.StringValue, nil
		}
	case spplugb.DataType_UInt8, spplugb.DataType_UInt16, spplugb.DataType_UInt32, spplugb.DataType_UInt64:
		if v, ok := integerBits(pm); ok {
			return strconv.FormatUint(v, 10), nil
		}
	case spplugb.DataType_Int8, spplugb.DataType_Int16, spplugb.DataType_Int32:
		// 8/16/32-bit signed values travel as the two's-complement bit pattern in int_value.
		if v, ok := integerBits(pm); ok {
			return strconv.FormatInt(int64(int32(uint32(v))), 10), nil //nolint:gosec // bit-pattern reinterpretation.
		}
	case spplugb.DataType_Int64:
		if v, ok := integerBits(pm); ok {
			return strconv.FormatInt(int64(v), 10), nil //nolint:gosec // bit-pattern reinterpretation.
		}
	case spplugb.DataType_Float, spplugb.DataType_Double:
		bits := 64
		if dataType == spplugb.DataType_Float {
			bits = 32
		}
		switch v := pm.Value.(type) {
		case *spplugb.Payload_Metric_FloatValue:
			return strconv.FormatFloat(float64(v.FloatValue), 'g', -1, bits), nil
		case *spplugb.Payload_Metric_DoubleValue:
			return strconv.FormatFloat(v.DoubleValue, 'g', -1, bits), nil
		}
	default:
		return "", fmt.Errorf("writing %s values is not supported", dataType)
	}
	return "", fmt.Errorf("value does not match the metric's %s data type", dataType)
}

// integerBits returns the raw integer carried in int_value or long_value.
func integerBits(pm *spplugb.Payload_Metric) (uint64, bool) {
	switch v := pm.Value.(type) {
	case *spplugb.Payload_Metric_IntValue:
		return uint64(v.IntValue), true
	case *spplugb.Payload_Metric_LongValue:
		return v.LongValue, true
	}
	return 0, false
}
