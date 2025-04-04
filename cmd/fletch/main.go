package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/TFMV/fletch/internal/config"
	"github.com/TFMV/fletch/internal/server"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func main() {
	// Create root command
	rootCmd := &cobra.Command{
		Use:   "fletch",
		Short: "Fletch: A high-throughput gateway connecting Arrow Flight with Qdrant",
		Long: `Fletch is a high-throughput, columnar-native data gateway for managing 
vector embeddings and metadata using Arrow Flight for transport and 
Qdrant as the vector database.`,
	}

	// Create serve command
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the Fletch server",
		Long:  "Start the Fletch server with Arrow Flight for communication and Qdrant for vector storage",
		RunE:  runServe,
	}

	// Add flags to serve command
	serveCmd.Flags().String("host", "localhost", "Host address to bind")
	serveCmd.Flags().Int("port", 8815, "Port for Flight server")
	serveCmd.Flags().String("qdrant-url", "localhost:6334", "Qdrant server URL")
	serveCmd.Flags().Bool("tls", false, "Enable TLS encryption")
	serveCmd.Flags().String("cert", "", "TLS certificate path")
	serveCmd.Flags().String("key", "", "TLS key path")
	serveCmd.Flags().String("log-level", "info", "Logging level (debug, info, warn, error)")
	serveCmd.Flags().String("config", "", "Path to config file")

	// Add serve command to root command
	rootCmd.AddCommand(serveCmd)

	// Execute root command
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runServe(cmd *cobra.Command, args []string) error {
	// Get config file path
	configPath, _ := cmd.Flags().GetString("config")

	// Load config
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Override config with command line flags
	host, _ := cmd.Flags().GetString("host")
	if host != "localhost" {
		cfg.Server.Host = host
	}

	port, _ := cmd.Flags().GetInt("port")
	if port != 8815 {
		cfg.Server.Port = port
	}

	qdrantURL, _ := cmd.Flags().GetString("qdrant-url")
	if qdrantURL != "localhost:6334" {
		// Parse host and port from URL
		host, port, err := parseHostPort(qdrantURL)
		if err != nil {
			return fmt.Errorf("invalid qdrant-url: %w", err)
		}
		cfg.Qdrant.Host = host
		cfg.Qdrant.Port = port
	}

	tls, _ := cmd.Flags().GetBool("tls")
	if tls {
		cfg.Server.TLSEnabled = true
	}

	cert, _ := cmd.Flags().GetString("cert")
	if cert != "" {
		cfg.Server.CertFile = cert
	}

	key, _ := cmd.Flags().GetString("key")
	if key != "" {
		cfg.Server.KeyFile = key
	}

	logLevel, _ := cmd.Flags().GetString("log-level")
	if logLevel != "info" {
		cfg.Log.Level = logLevel
	}

	// Validate config
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Create logger
	logCfg := zap.NewProductionConfig()
	logCfg.Level = getZapLogLevel(cfg.Log.Level)
	logger, err := logCfg.Build()
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer logger.Sync()

	// Create server
	srv, err := server.NewServer(cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	defer srv.Close()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("Shutting down server...")
		srv.Close()
	}()

	// Start server
	logger.Info("Starting Fletch server...")
	if err := srv.Serve(); err != nil {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

func parseHostPort(url string) (string, int, error) {
	// Simple parsing for host:port format
	var host string
	var port int

	if _, err := fmt.Sscanf(url, "%s:%d", &host, &port); err != nil {
		return "", 0, err
	}

	return host, port, nil
}

func getZapLogLevel(level string) zap.AtomicLevel {
	switch level {
	case "debug":
		return zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		return zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		return zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		return zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		return zap.NewAtomicLevelAt(zap.InfoLevel)
	}
}
