package sparkplug

// Config holds the settings for this service's single Sparkplug B Edge Node, loaded from the
// "SparkplugConfig" custom configuration section (see ServiceConfig).
type Config struct {
	// Enabled turns the Sparkplug B export on or off; when false, no Node is created and the
	// pipeline behaves exactly as configured in YAML.
	Enabled bool
	// GroupId and EdgeNodeId together identify this Edge Node in the Sparkplug topic namespace:
	// spBv1.0/{GroupId}/{msgType}/{EdgeNodeId}[/{device}].
	GroupId    string
	EdgeNodeId string
	// BrokerAddress is the complete MQTT broker address, e.g. "tcp://localhost:1883".
	BrokerAddress string
	ClientId      string
	// SecretName is the name of the secret in the SecretStore holding broker credentials/certs.
	SecretName string
	// AuthMode is one of "none", "usernamepassword", "cacert" or "clientcert".
	AuthMode       string
	QoS            int
	SkipCertVerify bool
}

// ServiceConfig is the top-level struct passed to ApplicationService.LoadCustomConfig. Its
// SparkplugConfig field is populated from the "SparkplugConfig" section of configuration.yaml.
type ServiceConfig struct {
	SparkplugConfig Config
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
