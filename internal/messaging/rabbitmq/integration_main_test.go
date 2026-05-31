//go:build integration

package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

const integrationPublishTimeout = 5 * time.Second

var integrationRabbitURL string

func TestMain(m *testing.M) {
	if url := os.Getenv("ERP_RABBITMQ_URL"); url != "" {
		integrationRabbitURL = url
		os.Exit(m.Run())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := tcrabbitmq.Run(ctx, "rabbitmq:4.1-management-alpine")
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration tests skipped: start RabbitMQ or set ERP_RABBITMQ_URL: %v\n", err)
		os.Exit(0)
	}

	defer func() {
		terminateCtx, terminateCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer terminateCancel()
		_ = container.Terminate(terminateCtx)
	}()

	url, err := container.AmqpURL(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration tests skipped: resolve RabbitMQ URL: %v\n", err)
		os.Exit(0)
	}

	integrationRabbitURL = url
	os.Exit(m.Run())
}

func requireIntegrationRabbitURL(t *testing.T) string {
	t.Helper()

	if integrationRabbitURL == "" {
		t.Fatal("integration RabbitMQ URL is not configured")
	}

	return integrationRabbitURL
}
