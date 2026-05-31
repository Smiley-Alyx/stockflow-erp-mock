package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHTTPAddress            = ":8080"
	defaultLogLevel               = "info"
	defaultRabbitMQConsumerTag    = "stockflow-erp-mock"
	defaultRabbitMQPrefetchCount  = 10
	defaultRabbitMQPublishTimeout = 5 * time.Second
	defaultRabbitMQURL            = "amqp://stockflow:stockflow@localhost:5672/"
	defaultShutdownTimeout        = 10 * time.Second
)

type Config struct {
	HTTPAddress            string
	LogLevel               slog.Level
	RabbitMQConsumerTag    string
	RabbitMQPrefetchCount  int
	RabbitMQPublishTimeout time.Duration
	RabbitMQURL            string
	ShutdownTimeout        time.Duration
}

func Load() (Config, error) {
	shutdownTimeout, err := durationFromEnv("ERP_SHUTDOWN_TIMEOUT", defaultShutdownTimeout)
	if err != nil {
		return Config{}, err
	}

	logLevel, err := parseLogLevel(stringFromEnv("ERP_LOG_LEVEL", defaultLogLevel))
	if err != nil {
		return Config{}, err
	}

	rabbitMQPrefetchCount, err := positiveIntFromEnv("ERP_RABBITMQ_PREFETCH_COUNT", defaultRabbitMQPrefetchCount)
	if err != nil {
		return Config{}, err
	}

	rabbitMQPublishTimeout, err := durationFromEnv("ERP_RABBITMQ_PUBLISH_TIMEOUT", defaultRabbitMQPublishTimeout)
	if err != nil {
		return Config{}, err
	}

	return Config{
		HTTPAddress:            stringFromEnv("ERP_HTTP_ADDRESS", defaultHTTPAddress),
		LogLevel:               logLevel,
		RabbitMQConsumerTag:    stringFromEnv("ERP_RABBITMQ_CONSUMER_TAG", defaultRabbitMQConsumerTag),
		RabbitMQPrefetchCount:  rabbitMQPrefetchCount,
		RabbitMQPublishTimeout: rabbitMQPublishTimeout,
		RabbitMQURL:            stringFromEnv("ERP_RABBITMQ_URL", defaultRabbitMQURL),
		ShutdownTimeout:        shutdownTimeout,
	}, nil
}

func stringFromEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

func durationFromEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}

	return duration, nil
}

func positiveIntFromEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	parsedValue, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	if parsedValue <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}

	return parsedValue, nil
}

func parseLogLevel(value string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(value)); err != nil {
		return 0, fmt.Errorf("parse ERP_LOG_LEVEL: %w", err)
	}

	return level, nil
}
