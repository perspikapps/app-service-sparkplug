package sparkplug

import "testing"

func TestMetricName(t *testing.T) {
	builtins := builtinNameKeys("temperature", "Thermostat1", "ThermostatProfile", "temperature")
	tests := []struct {
		name    string
		format  string
		sources []map[string]any
		want    string
	}{
		{"edge xpert default with levels", defaultMetricNameFormat,
			[]map[string]any{{"metric_level1": "Building4", "metric_level2": "Zone2"}, builtins}, "Building4/Zone2/temperature"},
		{"edge xpert default without tags collapses", defaultMetricNameFormat,
			[]map[string]any{builtins}, "temperature"},
		{"partial levels collapse", defaultMetricNameFormat,
			[]map[string]any{{"metric_level2": "Zone2"}, builtins}, "Zone2/temperature"},
		{"first source wins", "{site}/{resourceName}",
			[]map[string]any{{"site": "reading"}, {"site": "device"}, builtins}, "reading/temperature"},
		{"later source fills gaps", "{site}/{resourceName}",
			[]map[string]any{{}, {"site": "device"}, builtins}, "device/temperature"},
		{"non-string tag values", "{line}/{resourceName}",
			[]map[string]any{{"line": 3}, builtins}, "3/temperature"},
		{"literal text kept", "edgex/{deviceName}/{resourceName}",
			[]map[string]any{builtins}, "edgex/Thermostat1/temperature"},
		{"nothing resolves", "{a}/{b}", []map[string]any{builtins}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MetricName(tt.format, tt.sources...); got != tt.want {
				t.Errorf("MetricName(%q) = %q, want %q", tt.format, got, tt.want)
			}
		})
	}
}

func TestNodeMetricName_FallsBackToResourceName(t *testing.T) {
	n := testNode(newFakeClient())
	n.s.metricNameFormat = "{missing}"
	if got := n.metricName("temperature", builtinNameKeys("temperature", "d", "p", "s")); got != "temperature" {
		t.Errorf("metricName() = %q, want resource name fallback", got)
	}
}
