package config

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config represents the application configuration
type Config struct {
	// Server configuration
	Server ServerConfig `env:"SERVER"`

	// Storage configuration
	Storage StorageConfig `env:"STORAGE"`

	// Logging configuration
	Logging LoggingConfig `env:"LOGGING"`

	// Metrics configuration
	Metrics MetricsConfig `env:"METRICS"`

	// Configuration file path
	ConfigFile string `env:"CONFIG_FILE"`
}

// ServerConfig holds server-related configuration
type ServerConfig struct {
	// gRPC server address
	GRPCAddr string `env:"GRPC_ADDR" envDefault:":50051"`

	// HTTP server address
	HTTPAddr string `env:"HTTP_ADDR" envDefault:":8080"`

	// Enable TLS
	TLSEnabled bool `env:"TLS_ENABLED" envDefault:"false"`

	// TLS certificate file
	TLSCertFile string `env:"TLS_CERT_FILE"`

	// TLS key file
	TLSKeyFile string `env:"TLS_KEY_FILE"`
}

// StorageConfig holds storage-related configuration
type StorageConfig struct {
	// Data directory path
	DataDir string `env:"DATA_DIR" envDefault:"./data"`

	// Enable persistence
	Persist bool `env:"PERSIST" envDefault:"true"`

	// Fsync policy: "always", "batch", "interval"
	FsyncPolicy string `env:"FSYNC_POLICY" envDefault:"batch"`

	// Fsync interval (for interval policy)
	FsyncInterval time.Duration `env:"FSYNC_INTERVAL" envDefault:"10ms"`
}

// LoggingConfig holds logging-related configuration
type LoggingConfig struct {
	// Log level: "debug", "info", "warn", "error"
	Level string `env:"LOG_LEVEL" envDefault:"info"`

	// Log format: "json", "text"
	Format string `env:"LOG_FORMAT" envDefault:"json"`

	// Log file path (empty for stdout)
	Output string `env:"LOG_OUTPUT" envDefault:""`

	// Enable log rotation
	Rotation bool `env:"LOG_ROTATION" envDefault:"true"`

	// Max log file size in MB
	MaxSize int `env:"LOG_MAX_SIZE" envDefault:"100"`

	// Number of backup files to keep
	MaxBackups int `env:"LOG_MAX_BACKUPS" envDefault:"7"`

	// Max age in days
	MaxAge int `env:"LOG_MAX_AGE" envDefault:"30"`
}

// MetricsConfig holds metrics-related configuration
type MetricsConfig struct {
	// Enable Prometheus metrics
	Enabled bool `env:"METRICS_ENABLED" envDefault:"true"`

	// Metrics server address
	Addr string `env:"METRICS_ADDR" envDefault:":9090"`

	// Metrics path
	Path string `env:"METRICS_PATH" envDefault:"/metrics"`

	// Enable OpenTelemetry tracing
	TracingEnabled bool `env:"TRACING_ENABLED" envDefault:"false"`

	// OpenTelemetry endpoint
	TracingEndpoint string `env:"TRACING_ENDPOINT" envDefault:""`
}

// Load loads configuration from multiple sources:
// 1. Default values
// 2. Environment variables
// 3. Configuration file (YAML/TOML)
// 4. Command line flags
func Load() (*Config, error) {
	cfg := &Config{}

	// Load from environment variables
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse environment variables: %w", err)
	}

	// Parse command line flags
	flag.StringVar(&cfg.ConfigFile, "config", "", "Path to configuration file")
	flag.StringVar(&cfg.Server.GRPCAddr, "grpc-addr", cfg.Server.GRPCAddr, "gRPC server address")
	flag.StringVar(&cfg.Server.HTTPAddr, "http-addr", cfg.Server.HTTPAddr, "HTTP server address")
	flag.StringVar(&cfg.Storage.DataDir, "data-dir", cfg.Storage.DataDir, "Data directory path")
	flag.StringVar(&cfg.Logging.Level, "log-level", cfg.Logging.Level, "Log level (debug, info, warn, error)")
	flag.StringVar(&cfg.Logging.Format, "log-format", cfg.Logging.Format, "Log format (json, text)")
	flag.Parse()

	// Load from config file if specified
	if cfg.ConfigFile != "" {
		if err := loadFromFile(cfg, cfg.ConfigFile); err != nil {
			return nil, fmt.Errorf("failed to load config file: %w", err)
		}
	}

	// Normalize paths
	cfg.Storage.DataDir = filepath.Clean(cfg.Storage.DataDir)

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Server.GRPCAddr == "" {
		return fmt.Errorf("grpc server address cannot be empty")
	}

	if c.Server.HTTPAddr == "" {
		return fmt.Errorf("http server address cannot be empty")
	}

	if c.Storage.DataDir == "" {
		return fmt.Errorf("data directory cannot be empty")
	}

	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLogLevels[strings.ToLower(c.Logging.Level)] {
		return fmt.Errorf("invalid log level: %s", c.Logging.Level)
	}

	validLogFormats := map[string]bool{
		"json": true,
		"text": true,
	}
	if !validLogFormats[strings.ToLower(c.Logging.Format)] {
		return fmt.Errorf("invalid log format: %s", c.Logging.Format)
	}

	validFsyncPolicies := map[string]bool{
		"always":   true,
		"batch":    true,
		"interval": true,
	}
	if !validFsyncPolicies[strings.ToLower(c.Storage.FsyncPolicy)] {
		return fmt.Errorf("invalid fsync policy: %s", c.Storage.FsyncPolicy)
	}

	if c.Server.TLSEnabled {
		if c.Server.TLSCertFile == "" {
			return fmt.Errorf("tls cert file is required when tls is enabled")
		}
		if c.Server.TLSKeyFile == "" {
			return fmt.Errorf("tls key file is required when tls is enabled")
		}
	}

	return nil
}

// loadFromFile loads configuration from a file
// Currently supports basic key=value format
// Future: add YAML/TOML support
func loadFromFile(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	// For now, skip file loading - will be implemented with YAML/TOML support
	_ = data
	return nil
}

