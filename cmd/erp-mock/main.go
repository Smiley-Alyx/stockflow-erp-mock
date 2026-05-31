package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/config"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	httpapi "github.com/Smiley-Alyx/stockflow-erp-mock/internal/http"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/messaging/rabbitmq"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/storage/memory"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load configuration", "error", err)
		return 1
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	inventoryRepository, err := memory.NewInventoryRepository(inventory.NewService(), memory.DefaultStockSeed())
	if err != nil {
		logger.Error("initialize inventory repository", "error", err)
		return 1
	}

	rabbitMQConsumer, err := rabbitmq.NewConsumer(rabbitmq.ConsumerConfig{
		URL:           cfg.RabbitMQURL,
		ConsumerTag:   cfg.RabbitMQConsumerTag,
		PrefetchCount: cfg.RabbitMQPrefetchCount,
	}, logger)
	if err != nil {
		logger.Error("initialize RabbitMQ consumer", "error", err)
		return 1
	}
	defer func() {
		if err := rabbitMQConsumer.Close(); err != nil {
			logger.Error("close RabbitMQ consumer", "error", err)
		}
	}()

	consumerContext, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()

	consumerErrors := make(chan error, 1)
	go func() {
		consumerErrors <- rabbitMQConsumer.Consume(
			consumerContext,
			app.NewInventoryReservationHandler(inventoryRepository),
		)
	}()

	server := httpapi.New(
		cfg.HTTPAddress,
		logger,
		inventoryRepository,
		app.NewFailureModeController(),
	)
	server.SetReady(true)

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("http server started", "address", cfg.HTTPAddress)
		serverErrors <- server.ListenAndServe()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	exitCode := 0

	select {
	case sig := <-signals:
		logger.Info("shutdown signal received", "signal", sig.String())
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped unexpectedly", "error", err)
			exitCode = 1
		}
	case err := <-consumerErrors:
		if err != nil {
			logger.Error("RabbitMQ consumer stopped unexpectedly", "error", err)
			exitCode = 1
		}
	}

	server.SetReady(false)
	cancelConsumer()
	if err := rabbitMQConsumer.Close(); err != nil {
		logger.Error("close RabbitMQ consumer", "error", err)
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("http server shutdown failed", "error", err)
		return 1
	}

	logger.Info("service stopped")

	return exitCode
}
