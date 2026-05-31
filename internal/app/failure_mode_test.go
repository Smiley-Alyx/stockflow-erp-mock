package app

import (
	"testing"
	"time"
)

func TestFailureModeController(t *testing.T) {
	controller := NewFailureModeController()

	if settings := controller.Get(); settings.Mode != FailureModeNormal {
		t.Fatalf("Get().Mode = %q, want %q", settings.Mode, FailureModeNormal)
	}

	settings := FailureModeSettings{
		Mode:            FailureModeProcessingDelay,
		ProcessingDelay: 250 * time.Millisecond,
	}
	if err := controller.Set(settings); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if actualSettings := controller.Get(); actualSettings != settings {
		t.Errorf("Get() = %+v, want %+v", actualSettings, settings)
	}
}

func TestFailureModeControllerRejectsInvalidSettings(t *testing.T) {
	tests := []struct {
		name     string
		settings FailureModeSettings
	}{
		{name: "unsupported mode", settings: FailureModeSettings{Mode: "unknown"}},
		{name: "missing random probability", settings: FailureModeSettings{Mode: FailureModeRandomReject}},
		{name: "invalid random probability", settings: FailureModeSettings{Mode: FailureModeRandomReject, RandomRejectProbability: 1.1}},
		{name: "missing processing delay", settings: FailureModeSettings{Mode: FailureModeProcessingDelay}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := NewFailureModeController()

			if err := controller.Set(tt.settings); err == nil {
				t.Fatal("Set() error = nil, want an error")
			}
			if settings := controller.Get(); settings.Mode != FailureModeNormal {
				t.Errorf("Get().Mode = %q, want %q", settings.Mode, FailureModeNormal)
			}
		})
	}
}
