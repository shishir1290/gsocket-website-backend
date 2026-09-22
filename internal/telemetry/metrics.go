package telemetry

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ServerMetrics represents the global live telemetry snapshot of the system.
type ServerMetrics struct {
	UptimeSeconds    float64 `json:"uptimeSeconds"`
	ActiveClients    int     `json:"activeClients"`
	SimulatedNodes   int     `json:"simulatedNodes"`
	TotalPacketsIn   uint64  `json:"totalPacketsIn"`
	TotalPacketsOut  uint64  `json:"totalPacketsOut"`
	PacketsPerSecIn  uint64  `json:"packetsPerSecIn"`
	PacketsPerSecOut uint64  `json:"packetsPerSecOut"`
	AllocMB          float64 `json:"allocMB"`
	TotalAllocMB     float64 `json:"totalAllocMB"`
	SysMB            float64 `json:"sysMB"`
	NumGC            uint32  `json:"numGC"`
	Goroutines       int     `json:"goroutines"`
	AvgLatencyMs     float64 `json:"avgLatencyMs"`
	SimulationActive bool    `json:"simulationActive"`
	Timestamp        time.Time `json:"timestamp"`
}

// Collector tracks and calculates real-time system metrics.
type Collector struct {
	startTime        time.Time
	packetsInCounter uint64
	packetsOutCounter uint64

	prevPacketsIn    uint64
	prevPacketsOut   uint64
	currPacketsInRate uint64
	currPacketsOutRate uint64

	latencySumMs     float64
	latencyCount     uint64
	mu               sync.RWMutex
}

// NewCollector initializes a metrics collector.
func NewCollector() *Collector {
	c := &Collector{
		startTime: time.Now(),
	}
	go c.rateLoop()
	return c
}

// IncPacketsIn increments received packet counter.
func (c *Collector) IncPacketsIn(count uint64) {
	atomic.AddUint64(&c.packetsInCounter, count)
}

// IncPacketsOut increments transmitted packet counter.
func (c *Collector) IncPacketsOut(count uint64) {
	atomic.AddUint64(&c.packetsOutCounter, count)
}

// RecordLatency records a roundtrip latency sample.
func (c *Collector) RecordLatency(latencyMs float64) {
	c.mu.Lock()
	c.latencySumMs += latencyMs
	c.latencyCount++
	// Keep rolling average from overflowing or growing stale
	if c.latencyCount > 1000 {
		c.latencySumMs = c.latencySumMs / 2
		c.latencyCount = c.latencyCount / 2
	}
	c.mu.Unlock()
}

// rateLoop computes packets per second every second.
func (c *Collector) rateLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		totalIn := atomic.LoadUint64(&c.packetsInCounter)
		totalOut := atomic.LoadUint64(&c.packetsOutCounter)

		inDelta := totalIn - c.prevPacketsIn
		outDelta := totalOut - c.prevPacketsOut

		c.prevPacketsIn = totalIn
		c.prevPacketsOut = totalOut

		c.mu.Lock()
		c.currPacketsInRate = inDelta
		c.currPacketsOutRate = outDelta
		c.mu.Unlock()
	}
}

// Snapshot returns the current snapshot of all real-time telemetry metrics.
func (c *Collector) Snapshot(activeClients int, simulatedNodes int, simActive bool) ServerMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	c.mu.RLock()
	avgLatency := 0.15 // Default sub-millisecond baseline
	if c.latencyCount > 0 {
		avgLatency = c.latencySumMs / float64(c.latencyCount)
	}
	pktsInRate := c.currPacketsInRate
	pktsOutRate := c.currPacketsOutRate
	c.mu.RUnlock()

	return ServerMetrics{
		UptimeSeconds:    time.Since(c.startTime).Seconds(),
		ActiveClients:    activeClients,
		SimulatedNodes:   simulatedNodes,
		TotalPacketsIn:   atomic.LoadUint64(&c.packetsInCounter),
		TotalPacketsOut:  atomic.LoadUint64(&c.packetsOutCounter),
		PacketsPerSecIn:  pktsInRate,
		PacketsPerSecOut: pktsOutRate,
		AllocMB:          float64(m.Alloc) / 1024 / 1024,
		TotalAllocMB:     float64(m.TotalAlloc) / 1024 / 1024,
		SysMB:            float64(m.Sys) / 1024 / 1024,
		NumGC:            m.NumGC,
		Goroutines:       runtime.NumGoroutine(),
		AvgLatencyMs:     avgLatency,
		SimulationActive: simActive,
		Timestamp:        time.Now(),
	}
}
