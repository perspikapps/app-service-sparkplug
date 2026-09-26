package sparkplug

import (
	"context"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// devicePageSize is how many devices are requested from core-metadata per call.
const devicePageSize = 100

// deviceLister is the part of the SDK's DeviceClient the lifecycle poller uses.
type deviceLister interface {
	AllDevices(ctx context.Context, labels []string, offset int, limit int) (responses.MultiDevicesResponse, errors.EdgeX)
}

// profileGetter is the part of the SDK's DeviceProfileClient used to declare a registered device's
// metrics and to check a DCMD target resource is writable.
type profileGetter interface {
	DeviceProfileByName(ctx context.Context, name string) (responses.DeviceProfileResponse, errors.EdgeX)
}

// RunDeviceLifecycle starts a goroutine that keeps the Sparkplug device set in step with
// core-metadata until ctx is cancelled: immediately, and then every DeviceLifecycle.PollInterval,
// a newly registered device is born (DBIRTH declaring every resource of its profile, with no
// values yet) and a device removed from core-metadata is sent DDEATH. It does nothing when the
// interval is 0 or core-metadata clients aren't configured; devices are then still born on their
// first reading, as before.
func (n *Node) RunDeviceLifecycle(ctx context.Context, devices deviceLister, profiles profileGetter) {
	if n.s.pollInterval == 0 {
		n.lc.Info("sparkplug: device lifecycle polling disabled (DeviceLifecycle.PollInterval is 0); no DDEATH will be sent for removed devices")
		return
	}
	if devices == nil || profiles == nil {
		n.lc.Warn("sparkplug: core-metadata clients are not configured, device lifecycle polling disabled; no DDEATH will be sent for removed devices")
		return
	}

	go func() {
		ticker := time.NewTicker(n.s.pollInterval)
		defer ticker.Stop()
		for {
			n.pollDevices(ctx, devices, profiles)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// pollDevices performs one core-metadata sync. If the device list can't be fetched completely,
// nothing is changed: a core-metadata outage must never look like every device was deleted.
func (n *Node) pollDevices(ctx context.Context, lister deviceLister, profiles profileGetter) {
	listed, err := listAllDevices(ctx, lister)
	if err != nil {
		n.lc.Warnf("sparkplug: failed to list devices from core-metadata, skipping this lifecycle poll: %v", err)
		return
	}

	// Profiles are fetched without holding n.mu, so publishing never waits on core-metadata.
	births := make(map[string][]Metric)
	fetched := make(map[string]*dtos.DeviceProfile)
	for _, dev := range listed {
		if n.isRegistered(dev.Name) {
			continue
		}
		profile, ok := fetched[dev.ProfileName]
		if !ok {
			response, err := profiles.DeviceProfileByName(ctx, dev.ProfileName)
			if err != nil {
				n.lc.Warnf("sparkplug: failed to get profile '%s' for device '%s', will retry on next poll: %v", dev.ProfileName, dev.Name, err)
				fetched[dev.ProfileName] = nil
				continue
			}
			profile = &response.Profile
			fetched[dev.ProfileName] = profile
		}
		if profile != nil {
			births[dev.Name] = n.profileMetrics(dev, *profile)
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, dev := range listed {
		metrics, ok := births[dev.Name]
		if !ok {
			continue
		}
		d, known := n.devices[dev.Name]
		if !known {
			d = newDevice(dev.ProfileName)
			n.devices[dev.Name] = d
		}
		d.registered = true
		if added := d.merge(metrics); added || !d.born {
			if err := n.publishDeviceBirth(dev.Name, d); err != nil {
				n.lc.Errorf("sparkplug: failed to publish DBIRTH for registered device '%s': %v", dev.Name, err)
			}
		}
	}

	present := make(map[string]bool, len(listed))
	for _, dev := range listed {
		present[dev.Name] = true
	}
	for name, d := range n.devices {
		if d.registered && !present[name] {
			n.lc.Infof("sparkplug: device '%s' was removed from core-metadata, publishing DDEATH", name)
			if err := n.publishDeviceDeath(name); err != nil {
				n.lc.Errorf("sparkplug: failed to publish DDEATH for device '%s': %v", name, err)
			}
		}
	}
}

func (n *Node) isRegistered(deviceName string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	d, ok := n.devices[deviceName]
	return ok && d.registered
}

// profileMetrics declares one value-less metric per visible device resource of a supported type,
// named exactly as a reading of that resource would be (see NewExportFunction).
func (n *Node) profileMetrics(dev dtos.Device, profile dtos.DeviceProfile) []Metric {
	now := time.Now()
	metrics := make([]Metric, 0, len(profile.DeviceResources))
	for _, resource := range profile.DeviceResources {
		if resource.IsHidden {
			continue
		}
		dataType, ok := dataTypeFor(resource.Properties.ValueType)
		if !ok {
			continue
		}
		builtins := builtinNameKeys(resource.Name, dev.Name, dev.ProfileName, resource.Name)
		metrics = append(metrics, Metric{
			Name:         n.metricName(resource.Name, builtins, resource.Tags, dev.Tags),
			ResourceName: resource.Name,
			Timestamp:    now,
			DataType:     dataType,
		})
	}
	return metrics
}

// listAllDevices pages through core-metadata's device list.
func listAllDevices(ctx context.Context, lister deviceLister) ([]dtos.Device, error) {
	var all []dtos.Device
	for offset := 0; ; offset += devicePageSize {
		response, err := lister.AllDevices(ctx, nil, offset, devicePageSize)
		if err != nil {
			return nil, err
		}
		all = append(all, response.Devices...)
		if len(response.Devices) < devicePageSize || int64(len(all)) >= response.TotalCount {
			return all, nil
		}
	}
}
