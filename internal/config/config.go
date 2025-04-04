package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all configuration for the Fletch service
type Config struct {
	Server ServerConfig `mapstructure:"server"`
	Qdrant QdrantConfig `mapstructure:"qdrant"`
	Log    LogConfig    `mapstructure:"log"`
}

// ServerConfig holds configuration for the Arrow Flight server
type ServerConfig struct {
	Host          string `mapstructure:"host"`
	Port          int    `mapstructure:"port"`
	TLSEnabled    bool   `mapstructure:"tls_enabled"`
	CertFile      string `mapstructure:"cert_file"`
	KeyFile       string `mapstructure:"key_file"`
	MaxConcurrent int    `mapstructure:"max_concurrent"`
}

// QdrantConfig holds configuration for the Qdrant client
type QdrantConfig struct {
	Host      string `mapstructure:"host"`
	Port      int    `mapstructure:"port"`
	UseTLS    bool   `mapstructure:"use_tls"`
	APIKey    string `mapstructure:"api_key"`
	PoolSize  int    `mapstructure:"pool_size"`
	BatchSize int    `mapstructure:"batch_size"` // Size of batches for processing large requests
}

// LogConfig holds logging configuration
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// LoadConfig loads the configuration from a file or environment variables
func LoadConfig(configPath string) (*Config, error) {
	v := viper.New()

	// Set default values
	setDefaults(v)

	// Read from environment variables
	v.SetEnvPrefix("FLETCH")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Read from config file if specified
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				return nil, fmt.Errorf("config file not found at %s", configPath)
			}
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	} else {
		// Look for config file in default locations
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("$HOME/.fletch")
		v.AddConfigPath("/etc/fletch")

		if err := v.ReadInConfig(); err != nil {
			// It's okay if we can't find a config file, we'll use defaults and env vars
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return nil, fmt.Errorf("error reading config file: %w", err)
			}
		}
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	return &config, nil
}

// setDefaults sets default configuration values
func setDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.host", "localhost")
	v.SetDefault("server.port", 8815)
	v.SetDefault("server.tls_enabled", false)
	v.SetDefault("server.max_concurrent", 10)

	// Qdrant defaults
	v.SetDefault("qdrant.host", "localhost")
	v.SetDefault("qdrant.port", 6334)
	v.SetDefault("qdrant.use_tls", false)
	v.SetDefault("qdrant.pool_size", 10)
	v.SetDefault("qdrant.batch_size", 100)

	// Logging defaults
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Server.TLSEnabled {
		if c.Server.CertFile == "" || c.Server.KeyFile == "" {
			return fmt.Errorf("TLS is enabled but cert_file or key_file is not set")
		}
		if _, err := os.Stat(c.Server.CertFile); os.IsNotExist(err) {
			return fmt.Errorf("cert file not found: %s", c.Server.CertFile)
		}
		if _, err := os.Stat(c.Server.KeyFile); os.IsNotExist(err) {
			return fmt.Errorf("key file not found: %s", c.Server.KeyFile)
		}
	}

	return nil
}
