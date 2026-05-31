package app

import (
	"fmt"
	"sync"
	"time"
)

type FailureMode string

const (
	FailureModeNormal            FailureMode = "normal"
	FailureModeAlwaysReject      FailureMode = "always_reject"
	FailureModeRandomReject      FailureMode = "random_reject"
	FailureModeProcessingDelay   FailureMode = "processing_delay"
	FailureModePublishFailure    FailureMode = "publish_failure"
	FailureModeDuplicateResponse FailureMode = "duplicate_response"
)

type FailureModeSettings struct {
	Mode                    FailureMode
	RandomRejectProbability float64
	ProcessingDelay         time.Duration
}

type FailureModeController struct {
	mu       sync.RWMutex
	settings FailureModeSettings
}

func NewFailureModeController() *FailureModeController {
	return &FailureModeController{
		settings: FailureModeSettings{Mode: FailureModeNormal},
	}
}

func (c *FailureModeController) Get() FailureModeSettings {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.settings
}

func (c *FailureModeController) Set(settings FailureModeSettings) error {
	if err := validateFailureModeSettings(settings); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.settings = settings

	return nil
}

func validateFailureModeSettings(settings FailureModeSettings) error {
	switch settings.Mode {
	case FailureModeNormal, FailureModeAlwaysReject, FailureModePublishFailure, FailureModeDuplicateResponse:
		return nil
	case FailureModeRandomReject:
		if settings.RandomRejectProbability <= 0 || settings.RandomRejectProbability > 1 {
			return fmt.Errorf("random reject probability must be greater than 0 and less than or equal to 1")
		}

		return nil
	case FailureModeProcessingDelay:
		if settings.ProcessingDelay <= 0 {
			return fmt.Errorf("processing delay must be positive")
		}

		return nil
	default:
		return fmt.Errorf("unsupported failure mode %q", settings.Mode)
	}
}
