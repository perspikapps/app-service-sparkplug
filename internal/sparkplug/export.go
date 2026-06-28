package sparkplug

import (
	"fmt"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
)

// NewExportFunction returns a pipeline AppFunction that converts each EdgeX event's readings into
// Sparkplug metrics and publishes them through node, which auto-births the event's device on
// first sighting. Readings whose value type isn't supported for Sparkplug export are skipped
// individually rather than failing the whole event.
func NewExportFunction(node *Node) interfaces.AppFunction {
	return func(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
		if data == nil {
			return false, fmt.Errorf("sparkplug export: no data received")
		}

		event, ok := data.(dtos.Event)
		if !ok {
			return false, fmt.Errorf("sparkplug export: type received is not a dtos.Event")
		}

		metrics := make([]Metric, 0, len(event.Readings))
		for _, reading := range event.Readings {
			metric, err := MetricFromReading(reading)
			if err != nil {
				ctx.LoggingClient().Warnf("sparkplug export: skipping reading: %v", err)
				continue
			}
			metrics = append(metrics, metric)
		}

		if len(metrics) == 0 {
			return true, nil
		}

		if err := node.PublishDeviceData(event.DeviceName, metrics); err != nil {
			return false, fmt.Errorf("sparkplug export: failed to publish device '%s' data: %w", event.DeviceName, err)
		}

		return true, nil
	}
}
