package sparkplug

import (
	"strings"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		GroupId:    "G",
		EdgeNodeId: "N",
		MqttBroker: MqttBrokerConfig{Url: "tcp://localhost:1883", AuthMode: "none"},
	}
}

func TestResolve_AppliesEdgeXpertDefaults(t *testing.T) {
	s, err := validConfig().resolve()
	if err != nil {
		t.Fatalf("resolve() error = %v", err)
	}
	want := settings{
		namespace:        "spBv1.0",
		clientIdPrefix:   "edgex-sparkplug",
		connectTimeout:   30 * time.Second,
		keepAlive:        60 * time.Second,
		retryDuration:    600 * time.Second,
		retryInterval:    5 * time.Second,
		pollInterval:     30 * time.Second,
		metricNameFormat: "{metric_level1}/{metric_level2}/{resourceName}",
	}
	if s != want {
		t.Errorf("resolve() = %+v, want %+v", s, want)
	}
}

func TestResolve_ParsesOverrides(t *testing.T) {
	cfg := validConfig()
	cfg.Namespace = "spBv1.0"
	cfg.MqttBroker.KeepAlive = "15s"
	cfg.DeviceLifecycle.PollInterval = "0"
	cfg.Payload.MetricNameFormat = "{resourceName}"
	s, err := cfg.resolve()
	if err != nil {
		t.Fatalf("resolve() error = %v", err)
	}
	if s.keepAlive != 15*time.Second || s.pollInterval != 0 || s.metricNameFormat != "{resourceName}" {
		t.Errorf("unexpected settings: %+v", s)
	}
}

func TestResolve_RejectsInvalidConfig(t *testing.T) {
	tests := map[string]func(*Config){
		"missing url":        func(c *Config) { c.MqttBroker.Url = "" },
		"missing group":      func(c *Config) { c.GroupId = "" },
		"wildcard in group":  func(c *Config) { c.GroupId = "a+b" },
		"slash in edge node": func(c *Config) { c.EdgeNodeId = "a/b" },
		"hash in namespace":  func(c *Config) { c.Namespace = "sp#" },
		"bad auth mode":      func(c *Config) { c.MqttBroker.AuthMode = "bogus" },
		"qos out of range":   func(c *Config) { c.MqttBroker.QoS = 3 },
		"bad duration":       func(c *Config) { c.MqttBroker.ConnectTimeout = "soon" },
		"negative duration":  func(c *Config) { c.MqttBroker.RetryInterval = "-1s" },
		"bad poll interval":  func(c *Config) { c.DeviceLifecycle.PollInterval = "30" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(&cfg)
			if _, err := cfg.resolve(); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

func TestTopic_UsesConfiguredNamespace(t *testing.T) {
	n := testNode(newFakeClient())
	n.s.namespace = "spBv1.0-test"
	if got := n.topic(msgTypeDData, "D1"); got != "spBv1.0-test/TestGroup/DDATA/TestNode/D1" {
		t.Errorf("topic() = %s", got)
	}
}

func TestClientID_AppendsRandomSuffix(t *testing.T) {
	a, b := clientID("edgex-sparkplug"), clientID("edgex-sparkplug")
	if !strings.HasPrefix(a, "edgex-sparkplug-") || len(a) != len("edgex-sparkplug-")+8 {
		t.Errorf("clientID() = %s", a)
	}
	if a == b {
		t.Error("expected two client IDs to differ")
	}
}
