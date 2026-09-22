package telemetry

import (
	"testing"
	"time"
)

func TestMetricsCollector(t *testing.T) {
	col := NewCollector()
	if col == nil {
		t.Fatal("expected non-nil collector")
	}

	col.IncPacketsIn(10)
	col.IncPacketsOut(20)
	col.RecordLatency(0.45)

	snap := col.Snapshot(2, 8, true)

	if snap.ActiveClients != 2 {
		t.Errorf("expected 2 active clients, got %d", snap.ActiveClients)
	}
	if snap.SimulatedNodes != 8 {
		t.Errorf("expected 8 simulated nodes, got %d", snap.SimulatedNodes)
	}
	if snap.TotalPacketsIn != 10 {
		t.Errorf("expected 10 total packets in, got %d", snap.TotalPacketsIn)
	}
	if snap.TotalPacketsOut != 20 {
		t.Errorf("expected 20 total packets out, got %d", snap.TotalPacketsOut)
	}
	if !snap.SimulationActive {
		t.Error("expected simulationActive to be true")
	}
	if snap.AvgLatencyMs <= 0 {
		t.Errorf("expected positive average latency, got %f", snap.AvgLatencyMs)
	}
}

func TestCollectorRate(t *testing.T) {
	col := NewCollector()
	col.IncPacketsIn(50)
	col.IncPacketsOut(100)

	time.Sleep(1100 * time.Millisecond)

	snap := col.Snapshot(1, 1, false)
	if snap.PacketsPerSecIn != 50 {
		t.Errorf("expected 50 pkts/s in, got %d", snap.PacketsPerSecIn)
	}
	if snap.PacketsPerSecOut != 100 {
		t.Errorf("expected 100 pkts/s out, got %d", snap.PacketsPerSecOut)
	}
}
