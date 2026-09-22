package simulator

import (
	"sync/atomic"
	"testing"
	"time"

	"telemetry-simulation/internal/telemetry"
)

func TestSimulationEngine(t *testing.T) {
	col := telemetry.NewCollector()
	var updateCount uint64
	var alertCount uint64

	engine := NewEngine(
		Config{InitialNodes: 4, TickInterval: 50 * time.Millisecond},
		col,
		func(node *telemetry.DeviceTelemetry) {
			atomic.AddUint64(&updateCount, 1)
		},
		func(alert *telemetry.AlertEvent) {
			atomic.AddUint64(&alertCount, 1)
		},
	)

	if engine.NodeCount() != 4 {
		t.Fatalf("expected 4 nodes, got %d", engine.NodeCount())
	}

	engine.Start()
	if !engine.IsActive() {
		t.Fatal("expected engine to be active")
	}

	time.Sleep(160 * time.Millisecond)

	count := atomic.LoadUint64(&updateCount)
	if count == 0 {
		t.Error("expected telemetry updates during simulation")
	}

	// Test Scaling
	engine.ScaleNodes(10)
	if engine.NodeCount() != 10 {
		t.Errorf("expected 10 nodes after scale, got %d", engine.NodeCount())
	}

	// Test Alert Trigger
	alert := engine.TriggerAlert()
	if alert == nil {
		t.Fatal("expected non-nil alert event")
	}
	if alert.Severity != telemetry.StatusCritical {
		t.Errorf("expected critical severity, got %s", alert.Severity)
	}

	// Test Stop
	engine.Stop()
	if engine.IsActive() {
		t.Fatal("expected engine to be inactive after Stop")
	}
}
