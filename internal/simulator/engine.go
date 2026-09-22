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

	e := &Engine{
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
			BatteryPct:   85.0 + rand.Float64()*15.0,
			TemperatureC: 38.0 + rand.Float64()*8.0,
			CPULoadPct:   15.0 + rand.Float64()*25.0,
			MemoryMB:     256.0 + rand.Float64()*512.0,
			LatencyMs:    0.15 + rand.Float64()*0.4,
			ThroughputKb: 50.0 + rand.Float64()*150.0,
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
			BatteryPct:   70.0 + rand.Float64()*30.0,
			TemperatureC: 35.0 + rand.Float64()*12.0,
			CPULoadPct:   10.0 + rand.Float64()*30.0,
			MemoryMB:     256.0 + rand.Float64()*512.0,
			LatencyMs:    0.2 + rand.Float64()*0.5,
			ThroughputKb: 60.0 + rand.Float64()*180.0,
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
			node.PacketsSent++
			node.SeqNumber++
			node.CPULoadPct = 85.0 + rand.Float64()*14.0
			node.Timestamp = time.Now()

			e.collector.IncPacketsIn(1)
			if e.onNodeUpdate != nil {
				updateCopy := *node
				e.onNodeUpdate(&updateCopy)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()
}

// TriggerAlert forces an anomaly event on a random node.
func (e *Engine) TriggerAlert() *telemetry.AlertEvent {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.nodes) == 0 {
		return nil
	}

	target := e.nodes[rand.Intn(len(e.nodes))]
	target.Status = telemetry.StatusCritical
	target.TemperatureC = 88.5 + rand.Float64()*10.0
	target.CPULoadPct = 99.2

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

	if e.onAlert != nil {
		e.onAlert(alert)
	}
	return alert
}

// runLoop executes the heartbeat generation tick.
func (e *Engine) runLoop() {
	ticker := time.NewTicker(e.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopChan:
			return
		case <-ticker.C:
			e.mu.Lock()
			currentInterval := e.tickInterval
			e.mu.Unlock()

			// Check if interval changed
			ticker.Reset(currentInterval)

			e.mu.Lock()
			for _, node := range e.nodes {
				// Random gentle drift
				node.CPULoadPct += (rand.Float64() - 0.48) * 4.0
				if node.CPULoadPct < 5.0 {
					node.CPULoadPct = 5.0
				} else if node.CPULoadPct > 98.0 {
					node.CPULoadPct = 98.0
				}

				node.TemperatureC += (rand.Float64() - 0.48) * 0.8
				if node.TemperatureC < 30.0 {
					node.TemperatureC = 30.0
				} else if node.TemperatureC > 95.0 {
					node.TemperatureC = 95.0
				}

				node.BatteryPct -= 0.02
				if node.BatteryPct <= 0 {
					node.BatteryPct = 100.0 // Simulated battery recharge/swap
				}

				node.LatencyMs = 0.12 + rand.Float64()*0.4
				node.PacketsSent++
				node.SeqNumber++
				node.Timestamp = time.Now()

				// Status calculation
				if node.TemperatureC > 82.0 || node.CPULoadPct > 92.0 {
					node.Status = telemetry.StatusCritical
				} else if node.TemperatureC > 65.0 || node.CPULoadPct > 75.0 {
					node.Status = telemetry.StatusWarning
				} else {
					node.Status = telemetry.StatusNominal
				}

				// Count packet into telemetry
				e.collector.IncPacketsIn(1)
				e.collector.RecordLatency(node.LatencyMs)

				if e.onNodeUpdate != nil {
					nodeCopy := *node
					e.onNodeUpdate(&nodeCopy)
				}
			}
			e.mu.Unlock()
		}
	}
}
