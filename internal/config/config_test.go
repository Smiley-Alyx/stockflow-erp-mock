package config

import (
	"log/slog"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("ERP_HTTP_ADDRESS", "")
	t.Setenv("ERP_LOG_LEVEL", "")
	t.Setenv("ERP_RABBITMQ_CONSUMER_TAG", "")
	t.Setenv("ERP_RABBITMQ_PREFETCH_COUNT", "")
	t.Setenv("ERP_RABBITMQ_PUBLISH_TIMEOUT", "")
	t.Setenv("ERP_RABBITMQ_URL", "")
	t.Setenv("ERP_SHUTDOWN_TIMEOUT", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.HTTPAddress != ":8080" {
		t.Errorf("HTTPAddress = %q, want %q", cfg.HTTPAddress, ":8080")
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, slog.LevelInfo)
	}
	if cfg.RabbitMQConsumerTag != "stockflow-erp-mock" {
		t.Errorf("RabbitMQConsumerTag = %q, want %q", cfg.RabbitMQConsumerTag, "stockflow-erp-mock")
	}
	if cfg.RabbitMQPrefetchCount != 10 {
		t.Errorf("RabbitMQPrefetchCount = %d, want %d", cfg.RabbitMQPrefetchCount, 10)
	}
	if cfg.RabbitMQPublishTimeout != 5*time.Second {
		t.Errorf("RabbitMQPublishTimeout = %v, want %v", cfg.RabbitMQPublishTimeout, 5*time.Second)
	}
	if cfg.RabbitMQURL != "amqp://stockflow:stockflow@localhost:5672/" {
		t.Errorf("RabbitMQURL = %q", cfg.RabbitMQURL)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, 10*time.Second)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("ERP_HTTP_ADDRESS", "127.0.0.1:9090")
	t.Setenv("ERP_LOG_LEVEL", "debug")
	t.Setenv("ERP_RABBITMQ_CONSUMER_TAG", "erp-mock-test")
	t.Setenv("ERP_RABBITMQ_PREFETCH_COUNT", "25")
	t.Setenv("ERP_RABBITMQ_PUBLISH_TIMEOUT", "2s")
	t.Setenv("ERP_RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	t.Setenv("ERP_SHUTDOWN_TIMEOUT", "3s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.HTTPAddress != "127.0.0.1:9090" {
		t.Errorf("HTTPAddress = %q, want %q", cfg.HTTPAddress, "127.0.0.1:9090")
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, slog.LevelDebug)
	}
	if cfg.RabbitMQConsumerTag != "erp-mock-test" {
		t.Errorf("RabbitMQConsumerTag = %q, want %q", cfg.RabbitMQConsumerTag, "erp-mock-test")
	}
	if cfg.RabbitMQPrefetchCount != 25 {
		t.Errorf("RabbitMQPrefetchCount = %d, want %d", cfg.RabbitMQPrefetchCount, 25)
	}
	if cfg.RabbitMQPublishTimeout != 2*time.Second {
		t.Errorf("RabbitMQPublishTimeout = %v, want %v", cfg.RabbitMQPublishTimeout, 2*time.Second)
	}
	if cfg.RabbitMQURL != "amqp://guest:guest@rabbitmq:5672/" {
		t.Errorf("RabbitMQURL = %q", cfg.RabbitMQURL)
	}
	if cfg.ShutdownTimeout != 3*time.Second {
		t.Errorf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, 3*time.Second)
	}
}

func TestLoadRejectsInvalidShutdownTimeout(t *testing.T) {
	t.Setenv("ERP_SHUTDOWN_TIMEOUT", "later")

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want an error")
	}
}

func TestLoadRejectsInvalidRabbitMQPrefetchCount(t *testing.T) {
	t.Setenv("ERP_RABBITMQ_PREFETCH_COUNT", "0")

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want an error")
	}
}
