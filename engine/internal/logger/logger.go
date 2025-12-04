package logger

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Config holds logger configuration
type Config struct {
	Level      string
	Format     string
	Output     string
	Rotation   bool
	MaxSize    int
	MaxBackups int
	MaxAge     int
}

// Init initializes the global logger based on configuration
func Init(cfg *Config) error {
	// Set log level
	level, err := zerolog.ParseLevel(strings.ToLower(cfg.Level))
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	// Set time format
	zerolog.TimeFieldFormat = time.RFC3339Nano

	var writer io.Writer

	// Configure output
	if cfg.Output == "" || cfg.Output == "stdout" {
		writer = os.Stdout
	} else {
		if cfg.Rotation {
			writer = &lumberjack.Logger{
				Filename:   cfg.Output,
				MaxSize:    cfg.MaxSize,
				MaxBackups: cfg.MaxBackups,
				MaxAge:     cfg.MaxAge,
				Compress:   true,
			}
		} else {
			file, err := os.OpenFile(cfg.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				return err
			}
			writer = file
		}
	}

	// Configure format
	if strings.EqualFold(cfg.Format, "text") {
		writer = zerolog.ConsoleWriter{
			Out:        writer,
			TimeFormat: time.RFC3339,
		}
	}

	// Set global logger
	log.Logger = zerolog.New(writer).With().
		Timestamp().
		Str("component", "flowmesh").
		Logger()

	return nil
}

// Logger returns a logger instance with additional context
func Logger() zerolog.Logger {
	return log.Logger
}

// WithComponent returns a logger with a component name
func WithComponent(component string) zerolog.Logger {
	return log.Logger.With().Str("component", component).Logger()
}
