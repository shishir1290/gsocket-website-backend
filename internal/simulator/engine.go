package simulator

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"telemetry-simulation/internal/telemetry"
)

// Engine manages the virtual devices simulation loop.
type Engine struct {
	mu           sync.RWMutex
	rnd          *rand.Rand
	active       bool
	tickInterval time.Duration
	nodes        []*telemetry.DeviceTelemetry
	collector    *telemetry.Collector
	onNodeUpdate func(node *telemetry.DeviceTelemetry)
	onAlert      func(alert *telemetry.AlertEvent)
	stopChan     chan struct{}
}

// Config holds simulation parameters.
type Config struct {
	InitialNodes int
	TickInterval time.Duration
}

// NewEngine creates a new simulation engine.
func NewEngine(cfg Config, collector *telemetry.Collector, onNodeUpdate func(*telemetry.DeviceTelemetry), onAlert func(*telemetry.AlertEvent)) *Engine {
	if cfg.InitialNodes <= 0 {
		cfg.InitialNodes = 8
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 500 * time.Millisecond
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	e := &Engine{
		rnd:          rng,
		tickInterval: cfg.TickInterval,
		collector:    collector,
		onNodeUpdate: onNodeUpdate,
		onAlert:      onAlert,
		stopChan:     make(chan struct{}),
	}

	nodeTypes := []string{"drone-edge", "iot-sensor", "gateway-node", "server-pod"}
	for i := 1; i <= cfg.InitialNodes; i++ {
		t := nodeTypes[(i-1)%len(nodeTypes)]
		e.nodes = append(e.nodes, &telemetry.DeviceTelemetry{
			NodeID:       fmt.Sprintf("node-%03d", i),
			NodeName:     fmt.Sprintf("Telemetry-%s-%02d", t, i),
			NodeType:     t,
			Status:       telemetry.StatusNominal,
			BatteryPct:   85.0 + rng.Float64()*15.0,
			TemperatureC: 36.0 + rng.Float64()*6.0,
			CPULoadPct:   15.0 + rng.Float64()*20.0,
			MemoryMB:     256.0 + rng.Float64()*512.0,
			LatencyMs:    0.15 + rng.Float64()*0.4,
			ThroughputKb: 50.0 + rng.Float64()*150.0,
			PacketsSent:  0,
			SeqNumber:    0,
			Timestamp:    time.Now(),
		})
	}

	return e
}

// Start begins the live simulation loop.
func (e *Engine) Start() {
	e.mu.Lock()
	if e.active {
		e.mu.Unlock()
		return
	}
	e.active = true
	e.stopChan = make(chan struct{})
	e.mu.Unlock()

	go e.runLoop()
}

// Stop pauses the live simulation loop.
func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.active {
		return
	}
	e.active = false
	close(e.stopChan)
}

// IsActive returns whether simulation is running.
func (e *Engine) IsActive() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.active
}

// NodeCount returns current count of simulated nodes.
func (e *Engine) NodeCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.nodes)
}

// GetNodes returns snapshot copy of all nodes.
func (e *Engine) GetNodes() []telemetry.DeviceTelemetry {
	e.mu.RLock()
	defer e.mu.RUnlock()
	res := make([]telemetry.DeviceTelemetry, len(e.nodes))
	for i, n := range e.nodes {
		res[i] = *n
	}
	return res
}

// SetInterval adjusts the telemetry generation frequency.
func (e *Engine) SetInterval(interval time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	e.tickInterval = interval
}

// ScaleNodes dynamically adds or removes simulated nodes.
func (e *Engine) ScaleNodes(count int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if count < 1 {
		count = 1
	}
	if count > 200 {
		count = 200
	}

	curr := len(e.nodes)
	if count == curr {
		return
	}

	if count < curr {
		e.nodes = e.nodes[:count]
		return
	}

	nodeTypes := []string{"drone-edge", "iot-sensor", "gateway-node", "server-pod"}
	for i := curr + 1; i <= count; i++ {
		t := nodeTypes[(i-1)%len(nodeTypes)]
		e.nodes = append(e.nodes, &telemetry.DeviceTelemetry{
			NodeID:       fmt.Sprintf("node-%03d", i),
			NodeName:     fmt.Sprintf("Telemetry-%s-%02d", t, i),
			NodeType:     t,
			Status:       telemetry.StatusNominal,
			BatteryPct:   70.0 + e.rnd.Float64()*30.0,
			TemperatureC: 35.0 + e.rnd.Float64()*10.0,
			CPULoadPct:   10.0 + e.rnd.Float64()*25.0,
			MemoryMB:     256.0 + e.rnd.Float64()*512.0,
			LatencyMs:    0.2 + e.rnd.Float64()*0.5,
			ThroughputKb: 60.0 + e.rnd.Float64()*180.0,
			PacketsSent:  0,
			SeqNumber:    0,
			Timestamp:    time.Now(),
		})
	}
}

// TriggerBurst rapidly simulates generating a burst of packets.
func (e *Engine) TriggerBurst(packetCount int) {
	go func() {
		e.mu.RLock()
		nodesCopy := make([]*telemetry.DeviceTelemetry, len(e.nodes))
		copy(nodesCopy, e.nodes)
		e.mu.RUnlock()

		if len(nodesCopy) == 0 {
			return
		}

		for i := 0; i < packetCount; i++ {
			node := nodesCopy[i%len(nodesCopy)]
			e.mu.Lock()
			node.PacketsSent++
			node.SeqNumber++
			node.CPULoadPct = 85.0 + e.rnd.Float64()*14.0
			node.Timestamp = time.Now()
			updateCopy := *node
			e.mu.Unlock()

			if e.collector != nil {
				e.collector.IncPacketsIn(1)
			}
			if e.onNodeUpdate != nil {
				e.onNodeUpdate(&updateCopy)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()
}

// TriggerAlert forces an anomaly event on a random node.
func (e *Engine) TriggerAlert() *telemetry.AlertEvent {
	e.mu.Lock()
	if len(e.nodes) == 0 {
		e.mu.Unlock()
		return nil
	}

	target := e.nodes[e.rnd.Intn(len(e.nodes))]
	target.Status = telemetry.StatusCritical
	target.TemperatureC = 88.5 + e.rnd.Float64()*6.0
	target.CPULoadPct = 96.0 + e.rnd.Float64()*3.5

	alert := &telemetry.AlertEvent{
		ID:        fmt.Sprintf("alt-%d", time.Now().UnixNano()),
		NodeID:    target.NodeID,
		Severity:  telemetry.StatusCritical,
		Message:   fmt.Sprintf("Critical core temperature spike detected on %s", target.NodeName),
		Metric:    "temperatureC",
		Value:     target.TemperatureC,
		Threshold: 80.0,
		Timestamp: time.Now(),
	}
	e.mu.Unlock()

	if e.onAlert != nil {
		e.onAlert(alert)
	}
	return alert
}

// runLoop executes the heartbeat generation tick.
func (e *Engine) runLoop() {
	e.mu.RLock()
	interval := e.tickInterval
	e.mu.RUnlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopChan:
			return
		case <-ticker.C:
			e.mu.Lock()
			// Check if interval was changed dynamically
			if e.tickInterval != interval {
				interval = e.tickInterval
				ticker.Reset(interval)
			}

			updates := make([]telemetry.DeviceTelemetry, len(e.nodes))

			for i, node := range e.nodes {
				// Mean-reverting random walk towards nominal steady-state
				targetCPU := 28.0
				targetTemp := 39.0

				node.CPULoadPct += (targetCPU-node.CPULoadPct)*0.06 + (e.rnd.Float64()-0.5)*8.0
				if node.CPULoadPct < 5.0 {
					node.CPULoadPct = 5.0
				} else if node.CPULoadPct > 99.0 {
					node.CPULoadPct = 99.0
				}

				node.TemperatureC += (targetTemp-node.TemperatureC)*0.04 + (e.rnd.Float64()-0.5)*1.8
				if node.TemperatureC < 25.0 {
					node.TemperatureC = 25.0
				} else if node.TemperatureC > 96.0 {
					node.TemperatureC = 96.0
				}

				// Gradual battery drain with auto-recharge/swap at low power
				node.BatteryPct -= 0.015
				if node.BatteryPct <= 5.0 {
					node.BatteryPct = 100.0
				}

				node.LatencyMs = 0.12 + e.rnd.Float64()*0.35
				node.PacketsSent++
				node.SeqNumber++
				node.Timestamp = time.Now()

				// Dynamic status calculation
				if node.TemperatureC > 82.0 || node.CPULoadPct > 90.0 {
					node.Status = telemetry.StatusCritical
				} else if node.TemperatureC > 65.0 || node.CPULoadPct > 70.0 {
					node.Status = telemetry.StatusWarning
				} else {
					node.Status = telemetry.StatusNominal
				}

				updates[i] = *node
			}
			e.mu.Unlock()

			// Dispatch callbacks and record telemetry metrics outside the lock
			if e.collector != nil {
				e.collector.IncPacketsIn(uint64(len(updates)))
			}
			if e.onNodeUpdate != nil {
				for i := range updates {
					if e.collector != nil {
						e.collector.RecordLatency(updates[i].LatencyMs)
					}
					e.onNodeUpdate(&updates[i])
				}
			}
		}
	}
}
