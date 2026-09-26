package sparkplug

import (
	"fmt"
	"strings"
	"time"
)

// Configuration defaults. See docs/sparkplug-export.md.
const (
	defaultNamespace        = "spBv1.0"
	defaultClientIdPrefix   = "edgex-sparkplug"
	defaultConnectTimeout   = 30 * time.Second
	defaultKeepAlive        = 60 * time.Second
	defaultRetryDuration    = 600 * time.Second
	defaultRetryInterval    = 5 * time.Second
	defaultMetricNameFormat = "{metric_level1}/{metric_level2}/{resourceName}"
	defaultPollInterval     = 30 * time.Second
)

// Config holds the settings for this service's single Sparkplug B Edge Node, loaded from the
// "Sparkplug" custom configuration section. Every key can be overridden by an environment variable
// named after its path (SPARKPLUG_GROUPID, SPARKPLUG_MQTTBROKER_URL, ...).
type Config struct {
	// Enabled turns the Sparkplug B export on or off; when false, no Node is created and the
	// pipeline behaves exactly as configured in YAML.
	Enabled bool
	// Namespace is the Sparkplug topic namespace; empty means "spBv1.0".
	Namespace string
	// GroupId and EdgeNodeId identify this Edge Node in the topic namespace:
	// {Namespace}/{GroupId}/{msgType}/{EdgeNodeId}[/{device}].
	GroupId    string
	EdgeNodeId string
	// BdSeqStatePath, if set, is a local file used to persist bdSeq across process restarts so a
	// Primary Host Application can tell a genuine new session apart from a replay. When empty,
	// bdSeq starts at 0 on every restart.
	BdSeqStatePath  string
	MqttBroker      MqttBrokerConfig
	Payload         PayloadConfig
	DeviceLifecycle DeviceLifecycleConfig
	Commands        CommandsConfig
}

// MqttBrokerConfig configures the connection to the Sparkplug MQTT broker. Durations use Go
// duration syntax ("30s"); empty values fall back to the defaults above.
type MqttBrokerConfig struct {
	// Url is the complete broker address, e.g. "tcp://localhost:1883" or "tcps://host:8883".
	Url string
	// ClientIdPrefix gets a random suffix appended at runtime, so restarts never collide with a
	// broker session still held by the previous process.
	ClientIdPrefix string
	ConnectTimeout string
	KeepAlive      string
	QoS            int
	AutoReconnect  bool
	// Retain sets the MQTT retained flag on published messages. Sparkplug 3.0 requires BIRTH, DATA
	// and DEATH messages to be non-retained, so leave it false.
	Retain         bool
	SkipCertVerify bool
	// SecretPath is the SecretStore secret holding broker credentials/certs.
	SecretPath string
	// AuthMode is one of "none", "usernamepassword", "cacert" or "clientcert".
	AuthMode string
	// RetryDuration bounds how long Start keeps retrying the initial connection, trying every
	// RetryInterval.
	RetryDuration string
	RetryInterval string
}

// PayloadConfig configures how EdgeX readings become Sparkplug metrics.
type PayloadConfig struct {
	// MetricNameFormat builds each metric's name. Any {key} is replaced from the reading's or
	// device resource's tags, then the device's tags, then the built-ins resourceName, deviceName,
	// profileName and sourceName; path segments left empty are dropped. See MetricName.
	MetricNameFormat string
}

// DeviceLifecycleConfig configures how device registration and removal in core-metadata are
// reflected as DBIRTH and DDEATH.
type DeviceLifecycleConfig struct {
	// PollInterval is how often core-metadata's device list is checked. "0" disables polling, in
	// which case devices are still born on their first reading but never sent DDEATH.
	PollInterval string
}

// CommandsConfig configures inbound DCMD handling.
type CommandsConfig struct {
	// Enabled lets DCMD messages from a Sparkplug host issue EdgeX SET commands to writable device
	// resources. Off by default, since it allows a SCADA host to write to physical equipment.
	Enabled bool
}

// ServiceConfig is the top-level struct passed to ApplicationService.LoadCustomConfig. Its
// Sparkplug field is populated from the "Sparkplug" section of configuration.yaml.
type ServiceConfig struct {
	Sparkplug Config
}

// UpdateFromRaw implements interfaces.UpdatableConfig so the SDK can update this struct when
// custom configuration is loaded from the Configuration Provider.
func (c *ServiceConfig) UpdateFromRaw(rawConfig interface{}) bool {
	configuration, ok := rawConfig.(*ServiceConfig)
	if ok {
		*c = *configuration
	}
	return ok
}

// settings is Config after defaults are applied and durations parsed.
type settings struct {
	namespace        string
	clientIdPrefix   string
	connectTimeout   time.Duration
	keepAlive        time.Duration
	retryDuration    time.Duration
	retryInterval    time.Duration
	pollInterval     time.Duration
	metricNameFormat string
}

// resolve validates c and returns it with defaults applied.
func (c Config) resolve() (settings, error) {
	if c.GroupId == "" || c.EdgeNodeId == "" || c.MqttBroker.Url == "" {
		return settings{}, fmt.Errorf("sparkplug: GroupId, EdgeNodeId and MqttBroker.Url are all required")
	}
	for field, value := range map[string]string{"GroupId": c.GroupId, "EdgeNodeId": c.EdgeNodeId, "Namespace": c.Namespace} {
		if strings.ContainsAny(value, "+/#") {
			return settings{}, fmt.Errorf("sparkplug: %s '%s' must not contain '+', '/' or '#'", field, value)
		}
	}
	if !validAuthModes[c.MqttBroker.AuthMode] {
		return settings{}, fmt.Errorf("sparkplug: MqttBroker.AuthMode '%s' is not one of 'none', 'usernamepassword', 'cacert' or 'clientcert'", c.MqttBroker.AuthMode)
	}
	if c.MqttBroker.QoS < 0 || c.MqttBroker.QoS > 2 {
		return settings{}, fmt.Errorf("sparkplug: MqttBroker.QoS must be 0, 1 or 2, got %d", c.MqttBroker.QoS)
	}

	s := settings{
		namespace:        orDefault(c.Namespace, defaultNamespace),
		clientIdPrefix:   orDefault(c.MqttBroker.ClientIdPrefix, defaultClientIdPrefix),
		metricNameFormat: orDefault(c.Payload.MetricNameFormat, defaultMetricNameFormat),
	}
	durations := []struct {
		name  string
		value string
		def   time.Duration
		dst   *time.Duration
	}{
		{"MqttBroker.ConnectTimeout", c.MqttBroker.ConnectTimeout, defaultConnectTimeout, &s.connectTimeout},
		{"MqttBroker.KeepAlive", c.MqttBroker.KeepAlive, defaultKeepAlive, &s.keepAlive},
		{"MqttBroker.RetryDuration", c.MqttBroker.RetryDuration, defaultRetryDuration, &s.retryDuration},
		{"MqttBroker.RetryInterval", c.MqttBroker.RetryInterval, defaultRetryInterval, &s.retryInterval},
		{"DeviceLifecycle.PollInterval", c.DeviceLifecycle.PollInterval, defaultPollInterval, &s.pollInterval},
	}
	for _, d := range durations {
		if d.value == "" {
			*d.dst = d.def
			continue
		}
		parsed, err := time.ParseDuration(d.value)
		if err != nil || parsed < 0 {
			return settings{}, fmt.Errorf("sparkplug: %s '%s' is not a valid non-negative duration", d.name, d.value)
		}
		*d.dst = parsed
	}
	return s, nil
}

func orDefault(value, def string) string {
	if value == "" {
		return def
	}
	return value
}
