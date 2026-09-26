package sparkplug

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	"github.com/edgexfoundry/app-service-configurable/internal/sparkplug/spplugb"
)

type fakeMetadata struct {
	devices      []dtos.Device
	profiles     map[string]dtos.DeviceProfile
	listErr      errors.EdgeX
	profileCalls int
}

func (f *fakeMetadata) AllDevices(_ context.Context, _ []string, offset int, limit int) (responses.MultiDevicesResponse, errors.EdgeX) {
	if f.listErr != nil {
		return responses.MultiDevicesResponse{}, f.listErr
	}
	end := min(offset+limit, len(f.devices))
	var response responses.MultiDevicesResponse
	response.TotalCount = int64(len(f.devices))
	if offset < end {
		response.Devices = f.devices[offset:end]
	}
	return response, nil
}

func (f *fakeMetadata) DeviceProfileByName(_ context.Context, name string) (responses.DeviceProfileResponse, errors.EdgeX) {
	f.profileCalls++
	profile, ok := f.profiles[name]
	if !ok {
		return responses.DeviceProfileResponse{}, errors.NewCommonEdgeX(errors.KindEntityDoesNotExist, "no such profile", nil)
	}
	return responses.DeviceProfileResponse{Profile: profile}, nil
}

func thermostatProfile() dtos.DeviceProfile {
	return dtos.DeviceProfile{
		DeviceProfileBasicInfo: dtos.DeviceProfileBasicInfo{Name: "ThermostatProfile"},
		DeviceResources: []dtos.DeviceResource{
			{Name: "temperature", Properties: dtos.ResourceProperties{ValueType: common.ValueTypeFloat64, ReadWrite: common.ReadWrite_R},
				Tags: map[string]any{"metric_level1": "HVAC"}},
			{Name: "setpoint", Properties: dtos.ResourceProperties{ValueType: common.ValueTypeFloat64, ReadWrite: common.ReadWrite_RW}},
			{Name: "enabled", Properties: dtos.ResourceProperties{ValueType: common.ValueTypeBool, ReadWrite: common.ReadWrite_W}},
			{Name: "config", Properties: dtos.ResourceProperties{ValueType: common.ValueTypeObject, ReadWrite: common.ReadWrite_R}},
			{Name: "internal", IsHidden: true, Properties: dtos.ResourceProperties{ValueType: common.ValueTypeInt32, ReadWrite: common.ReadWrite_R}},
		},
	}
}

func newFakeMetadata(deviceNames ...string) *fakeMetadata {
	f := &fakeMetadata{profiles: map[string]dtos.DeviceProfile{"ThermostatProfile": thermostatProfile()}}
	for _, name := range deviceNames {
		f.devices = append(f.devices, dtos.Device{Name: name, ProfileName: "ThermostatProfile", Tags: map[string]any{"metric_level2": "Floor1"}})
	}
	return f
}

func TestPollDevices_BirthsRegisteredDeviceWithNullMetrics(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	metadata := newFakeMetadata("Thermostat1")

	n.pollDevices(context.Background(), metadata, metadata)

	if len(client.published) != 1 || client.published[0].topic != "spBv1.0/TestGroup/DBIRTH/TestNode/Thermostat1" {
		t.Fatalf("expected one DBIRTH, got %+v", client.published)
	}
	metrics := client.published[0].payload.GetMetrics()
	var names []string
	for _, m := range metrics {
		names = append(names, m.GetName())
		if !m.GetIsNull() {
			t.Errorf("metric %s: expected is_null", m.GetName())
		}
	}
	if got := strings.Join(names, ","); got != "HVAC/Floor1/temperature,Floor1/setpoint,Floor1/enabled" {
		t.Errorf("DBIRTH metric names = %s (hidden and unsupported resources must be skipped)", got)
	}
	if metrics[2].GetDatatype() != uint32(spplugb.DataType_Boolean) {
		t.Errorf("enabled datatype = %d, want Boolean", metrics[2].GetDatatype())
	}

	// A second poll with nothing changed publishes nothing and doesn't refetch profiles.
	calls := metadata.profileCalls
	n.pollDevices(context.Background(), metadata, metadata)
	if len(client.published) != 1 || metadata.profileCalls != calls {
		t.Errorf("expected an unchanged poll to be a no-op, got %d publishes, %d profile calls", len(client.published), metadata.profileCalls-calls)
	}
}

func TestPollDevices_ReadingAfterRegistrationIsDData(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	metadata := newFakeMetadata("Thermostat1")
	n.pollDevices(context.Background(), metadata, metadata)

	err := n.PublishDeviceData("Thermostat1", "ThermostatProfile", []Metric{
		{Name: "Floor1/setpoint", ResourceName: "setpoint", Timestamp: time.Now(), DataType: spplugb.DataType_Double, Value: 20.0},
	})
	if err != nil {
		t.Fatalf("PublishDeviceData() error = %v", err)
	}
	if len(client.published) != 2 || !strings.Contains(client.published[1].topic, "/DDATA/") {
		t.Fatalf("expected DBIRTH then DDATA, got %d publishes", len(client.published))
	}
}

func TestPollDevices_RemovedDeviceGetsDDeathAndCleanRebirth(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	metadata := newFakeMetadata("Thermostat1", "Thermostat2")
	n.pollDevices(context.Background(), metadata, metadata)
	client.published = nil

	metadata.devices = metadata.devices[1:]
	n.pollDevices(context.Background(), metadata, metadata)

	if len(client.published) != 1 || client.published[0].topic != "spBv1.0/TestGroup/DDEATH/TestNode/Thermostat1" {
		t.Fatalf("expected one DDEATH for Thermostat1, got %+v", client.published)
	}
	death := client.published[0].payload
	if len(death.GetMetrics()) != 0 || death.Seq == nil || death.Timestamp == nil {
		t.Errorf("DDEATH must carry only timestamp and seq, got %+v", death)
	}
	if _, ok := n.devices["Thermostat1"]; ok {
		t.Error("expected removed device to be forgotten")
	}

	metadata.devices = append(metadata.devices, dtos.Device{Name: "Thermostat1", ProfileName: "ThermostatProfile"})
	n.pollDevices(context.Background(), metadata, metadata)
	if len(client.published) != 2 || !strings.Contains(client.published[1].topic, "/DBIRTH/TestNode/Thermostat1") {
		t.Errorf("expected re-registered device to be born again, got %+v", client.published)
	}
}

func TestPollDevices_ListFailureChangesNothing(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	metadata := newFakeMetadata("Thermostat1")
	n.pollDevices(context.Background(), metadata, metadata)
	client.published = nil

	metadata.listErr = errors.NewCommonEdgeX(errors.KindServiceUnavailable, "core-metadata down", nil)
	n.pollDevices(context.Background(), metadata, metadata)

	if len(client.published) != 0 {
		t.Errorf("a failed device list must not DDEATH anything, got %+v", client.published)
	}
}

func TestPollDevices_DataBornDeviceIsNotKilledUnlessRegistered(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	if err := n.PublishDeviceData("Unregistered", "", []Metric{
		{Name: "x", ResourceName: "x", Timestamp: time.Now(), DataType: spplugb.DataType_Double, Value: 1.0},
	}); err != nil {
		t.Fatalf("PublishDeviceData() error = %v", err)
	}
	client.published = nil

	metadata := newFakeMetadata()
	n.pollDevices(context.Background(), metadata, metadata)
	if len(client.published) != 0 {
		t.Errorf("a device never seen in core-metadata must not get DDEATH, got %+v", client.published)
	}
}

func TestPollDevices_DataBornDeviceGainsProfileMetrics(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	if err := n.PublishDeviceData("Thermostat1", "ThermostatProfile", []Metric{
		{Name: "Floor1/setpoint", ResourceName: "setpoint", Timestamp: time.Now(), DataType: spplugb.DataType_Double, Value: 20.0},
	}); err != nil {
		t.Fatalf("PublishDeviceData() error = %v", err)
	}

	metadata := newFakeMetadata("Thermostat1")
	n.pollDevices(context.Background(), metadata, metadata)

	if len(client.published) != 2 {
		t.Fatalf("expected a second DBIRTH declaring the profile's other metrics, got %d publishes", len(client.published))
	}
	for _, m := range client.published[1].payload.GetMetrics() {
		if m.GetName() == "Floor1/setpoint" && (m.GetIsNull() || m.GetDoubleValue() != 20.0) {
			t.Errorf("a known value must survive registration, got %+v", m)
		}
	}
}

func TestListAllDevices_Pages(t *testing.T) {
	metadata := &fakeMetadata{}
	for i := 0; i < devicePageSize*2+5; i++ {
		metadata.devices = append(metadata.devices, dtos.Device{Name: string(rune('a' + i%26))})
	}
	all, err := listAllDevices(context.Background(), metadata)
	if err != nil || len(all) != devicePageSize*2+5 {
		t.Errorf("listAllDevices() = %d devices, %v", len(all), err)
	}
}

func TestRunDeviceLifecycle_DisabledWithoutClientsOrInterval(t *testing.T) {
	client := newFakeClient()
	n := testNode(client)
	n.s.pollInterval = time.Second
	n.RunDeviceLifecycle(context.Background(), nil, nil)

	n.s.pollInterval = 0
	metadata := newFakeMetadata("Thermostat1")
	n.RunDeviceLifecycle(context.Background(), metadata, metadata)

	time.Sleep(50 * time.Millisecond)
	if len(client.published) != 0 {
		t.Errorf("expected no polling, got %d publishes", len(client.published))
	}
}
