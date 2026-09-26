package sparkplug

import (
	"fmt"
	"regexp"
	"strings"
)

var placeholderPattern = regexp.MustCompile(`\{([^{}]+)\}`)

// MetricName builds a Sparkplug metric name from format (Payload.MetricNameFormat), replacing each
// {key} placeholder with the value of key in the first source that has it. A placeholder no source
// resolves becomes empty, and empty '/'-separated segments are then dropped, so the default
// "{metric_level1}/{metric_level2}/{resourceName}" yields just the resource name for a resource
// without metric_level tags instead of a name like "//temperature".
func MetricName(format string, sources ...map[string]any) string {
	name := placeholderPattern.ReplaceAllStringFunc(format, func(placeholder string) string {
		key := placeholder[1 : len(placeholder)-1]
		for _, source := range sources {
			if value, ok := source[key]; ok && value != nil {
				return fmt.Sprint(value)
			}
		}
		return ""
	})

	segments := strings.Split(name, "/")
	kept := segments[:0]
	for _, segment := range segments {
		if segment = strings.TrimSpace(segment); segment != "" {
			kept = append(kept, segment)
		}
	}
	return strings.Join(kept, "/")
}

// builtinNameKeys returns the placeholders every metric name format can use regardless of tags.
func builtinNameKeys(resourceName, deviceName, profileName, sourceName string) map[string]any {
	return map[string]any{
		"resourceName": resourceName,
		"deviceName":   deviceName,
		"profileName":  profileName,
		"sourceName":   sourceName,
	}
}

// metricName applies the Node's MetricNameFormat, falling back to the bare resource name if the
// format resolves to nothing, since Sparkplug metric names must be non-empty.
func (n *Node) metricName(resourceName string, builtins map[string]any, tags ...map[string]any) string {
	name := MetricName(n.s.metricNameFormat, append(tags, builtins)...)
	if name == "" {
		return resourceName
	}
	return name
}
