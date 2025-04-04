package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_DefaultValues(t *testing.T) {
	// Test loading with no config file should use defaults
	cfg, err := LoadConfig("")
	require.NoError(t, err)

	// Check default values
	assert.Equal(t, "localhost", cfg.Server.Host)
	assert.Equal(t, 8815, cfg.Server.Port)
	assert.Equal(t, false, cfg.Server.TLSEnabled)
	assert.Equal(t, 10, cfg.Server.MaxConcurrent)

	assert.Equal(t, "localhost", cfg.Qdrant.Host)
	assert.Equal(t, 6334, cfg.Qdrant.Port)
	assert.Equal(t, false, cfg.Qdrant.UseTLS)
	assert.Equal(t, 10, cfg.Qdrant.PoolSize)

	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
}

func TestLoadConfig_EnvVars(t *testing.T) {
	// Backup original env vars
	origHost := os.Getenv("FLETCH_SERVER_HOST")
	origPort := os.Getenv("FLETCH_SERVER_PORT")
	defer func() {
		// Restore original env vars
		os.Setenv("FLETCH_SERVER_HOST", origHost)
		os.Setenv("FLETCH_SERVER_PORT", origPort)
	}()

	// Set env vars for testing
	os.Setenv("FLETCH_SERVER_HOST", "test-host")
	os.Setenv("FLETCH_SERVER_PORT", "9000")

	// Load config with env vars
	cfg, err := LoadConfig("")
	require.NoError(t, err)

	// Check that env vars were used
	assert.Equal(t, "test-host", cfg.Server.Host)
	assert.Equal(t, 9000, cfg.Server.Port)
}

func TestConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &Config{
			Server: ServerConfig{
				Host: "localhost",
				Port: 8815,
			},
		}

		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("invalid TLS config", func(t *testing.T) {
		cfg := &Config{
			Server: ServerConfig{
				Host:       "localhost",
				Port:       8815,
				TLSEnabled: true,
				// Missing cert and key files
			},
		}

		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "TLS is enabled but cert_file or key_file is not set")
	})

	t.Run("non-existent cert file", func(t *testing.T) {
		cfg := &Config{
			Server: ServerConfig{
				Host:       "localhost",
				Port:       8815,
				TLSEnabled: true,
				CertFile:   "/path/to/nonexistent/cert.pem",
				KeyFile:    "testdata/key.pem",
			},
		}

		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cert file not found")
	})
}

func TestLoadConfig_InvalidPath(t *testing.T) {
	// Test loading with non-existent config file
	_, err := LoadConfig("/path/to/nonexistent/config.yaml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config file not found")
}
