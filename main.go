package main

import (
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/shishir1290/gsocketio"
	"telemetry-simulation/internal/simulator"
	"telemetry-simulation/internal/telemetry"
)

//go:embed web/*
var webFS embed.FS

func main() {
	defaultPort := 8080
	if envPort := os.Getenv("PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil {
			defaultPort = p
		}
	}

	port := flag.Int("port", defaultPort, "Port to listen on")
	initialNodes := flag.Int("nodes", 8, "Initial number of simulated telemetry nodes")
	autoStart := flag.Bool("autostart", true, "Auto-start simulation on boot")
	flag.Parse()

	log.Println("==================================================================")
	log.Println("⚡ Starting gsocketio Realtime Live Telemetry & Simulation Platform")
	log.Println("==================================================================")

	// 1. Initialize Metrics Collector
	collector := telemetry.NewCollector()

	// 2. Initialize gsocketio Server (Pure Go Socket.IO v4, RFC 6455)
	srv := gsocketio.New(&gsocketio.Options{
		PingInterval: 25 * time.Second,
		PingTimeout:  20 * time.Second,
		MaxPayload:   2_000_000,
	})

	// 3. Initialize Simulation Engine
	simEngine := simulator.NewEngine(
		simulator.Config{
			InitialNodes: *initialNodes,
			TickInterval: 500 * time.Millisecond,
		},
		collector,
		func(node *telemetry.DeviceTelemetry) {
			collector.IncPacketsOut(1)
			// Broadcast node telemetry to "dashboard" room subscribers
			srv.ToRoom("/", "dashboard", "telemetry:node_update", nil, node)
		},
		func(alert *telemetry.AlertEvent) {
			collector.IncPacketsOut(1)
			srv.ToNamespace("/", "telemetry:alert", alert)
		},
	)

	// 4. Register Socket.IO Event Handlers
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		log.Printf("[+] Client connected: sid=%s", c.ID())
		collector.IncPacketsIn(1)
		return nil
	})

	srv.OnDisconnect("/", func(c gsocketio.Conn, reason string) {
		log.Printf("[-] Client disconnected: sid=%s, reason=%s", c.ID(), reason)
	})

	srv.OnError("/", func(c gsocketio.Conn, err error) {
		log.Printf("[!] Socket error: sid=%s, err=%v", c.ID(), err)
	})

	// Client subscribes to the live telemetry dashboard stream
	srv.OnEvent("/", "telemetry:subscribe", func(c gsocketio.Conn, args []json.RawMessage) {
		c.Join("dashboard")
		collector.IncPacketsIn(1)

		// Send initial full fleet snapshot to this subscriber
		nodes := simEngine.GetNodes()
		collector.IncPacketsOut(1)
		_ = c.Emit("telemetry:nodes_init", nodes)
		log.Printf("[*] Client %s joined 'dashboard' room (nodes snapshot sent: %d nodes)", c.ID(), len(nodes))
	})

	// Round-trip latency ping/pong
	srv.OnEvent("/", "telemetry:ping", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		collector.IncPacketsOut(1)
		var val interface{}
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &val)
		}
		_ = c.Emit("telemetry:pong", val)
	})

	// Ingest external client telemetry packet
	srv.OnEvent("/", "telemetry:ingest", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		if len(args) > 0 {
			var d telemetry.DeviceTelemetry
			if err := json.Unmarshal(args[0], &d); err == nil {
				collector.IncPacketsOut(1)
				srv.ToRoom("/", "dashboard", "telemetry:node_update", nil, d)
			}
		}
	})

	// Broadcast chat/message to everyone connected
	srv.OnEvent("/", "chat", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		collector.IncPacketsOut(1)
		var payload interface{}
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &payload)
		}
		log.Printf("[Broadcast] chat from %s: %v", c.ID(), payload)
		srv.ToNamespace("/", "chat", map[string]interface{}{
			"sender":    c.ID(),
			"payload":   payload,
			"timestamp": time.Now().Format("15:04:05"),
		})
	})

	srv.OnEvent("/", "message", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		collector.IncPacketsOut(1)
		var payload interface{}
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &payload)
		}
		log.Printf("[Broadcast] message from %s: %v", c.ID(), payload)
		srv.ToNamespace("/", "message", map[string]interface{}{
			"sender":    c.ID(),
			"payload":   payload,
			"timestamp": time.Now().Format("15:04:05"),
		})
	})

	srv.OnEvent("/", "simulation:message", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		collector.IncPacketsOut(1)
		var payload interface{}
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &payload)
		}
		log.Printf("[Broadcast] simulation:message from %s: %v", c.ID(), payload)
		srv.ToNamespace("/", "simulation:message", map[string]interface{}{
			"sender":    c.ID(),
			"payload":   payload,
			"timestamp": time.Now().Format("15:04:05"),
		})
	})

	srv.OnEvent("/", "broadcast", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		collector.IncPacketsOut(1)
		var payload interface{}
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &payload)
		}
		log.Printf("[Broadcast] broadcast from %s: %v", c.ID(), payload)
		srv.ToNamespace("/", "broadcast", map[string]interface{}{
			"sender":    c.ID(),
			"payload":   payload,
			"timestamp": time.Now().Format("15:04:05"),
		})
	})

	srv.OnEvent("/", "join_room", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		var room string
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &room)
		}
		if room == "" {
			room = "lobby"
		}
		c.Join(room)
		log.Printf("[*] Client %s joined room %s", c.ID(), room)
		srv.ToNamespace("/", "room_notification", map[string]interface{}{
			"type":      "join",
			"user":      c.ID(),
			"room":      room,
			"message":   fmt.Sprintf("Client %s joined room '%s'", c.ID(), room),
			"timestamp": time.Now().Format("15:04:05"),
		})
	})

	// Toggle simulation state
	srv.OnEvent("/", "simulation:toggle", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		var req struct {
			Active bool `json:"active"`
		}
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &req)
		}
		if req.Active {
			simEngine.Start()
			log.Println("[Simulation] Engine started/resumed")
		} else {
			simEngine.Stop()
			log.Println("[Simulation] Engine paused")
		}
	})

	// Dynamically scale simulated nodes
	srv.OnEvent("/", "simulation:scale", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		var req struct {
			Count int `json:"count"`
		}
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &req)
		}
		simEngine.ScaleNodes(req.Count)
		log.Printf("[Simulation] Fleet scaled to %d nodes", req.Count)

		// Send updated nodes list
		nodes := simEngine.GetNodes()
		collector.IncPacketsOut(1)
		srv.ToRoom("/", "dashboard", "telemetry:nodes_init", nil, nodes)
	})

	// Adjust simulation interval
	srv.OnEvent("/", "simulation:interval", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		var req struct {
			IntervalMs int `json:"intervalMs"`
		}
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &req)
		}
		simEngine.SetInterval(time.Duration(req.IntervalMs) * time.Millisecond)
		log.Printf("[Simulation] Telemetry interval set to %d ms", req.IntervalMs)
	})

	// Trigger packet burst
	srv.OnEvent("/", "simulation:burst", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		var req struct {
			Count int `json:"count"`
		}
		req.Count = 500
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &req)
		}
		log.Printf("[Simulation] Triggering burst of %d packets", req.Count)
		simEngine.TriggerBurst(req.Count)
	})

	// Trigger simulated anomaly alert
	srv.OnEvent("/", "simulation:alert_trigger", func(c gsocketio.Conn, args []json.RawMessage) {
		collector.IncPacketsIn(1)
		log.Println("[Simulation] Triggering random anomaly alert")
		simEngine.TriggerAlert()
	})

	// 5. Background Broadcast Loop (Emits global telemetry metrics every 1s)
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			snap := collector.Snapshot(srv.Count(), simEngine.NodeCount(), simEngine.IsActive())
			collector.IncPacketsOut(1)
			srv.ToRoom("/", "dashboard", "telemetry:metrics", nil, snap)
		}
	}()

	// 6. Start simulation if autoStart is true
	if *autoStart {
		simEngine.Start()
		log.Printf("[Simulation] Engine started with %d simulated nodes", *initialNodes)
	}

	// 7. Background gsocketio worker
	go func() {
		if err := srv.Serve(); err != nil {
			log.Printf("[Server] gsocketio Serve stopped: %v", err)
		}
	}()

	// 8. Setup HTTP Router & Web UI
	mux := http.NewServeMux()
	mux.Handle("/socket.io/", srv)

	// Static Web Dashboard from embed.FS
	subFS, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatalf("Failed to create sub filesystem: %v", err)
	}
	fileServer := http.FileServer(http.FS(subFS))
	mux.Handle("/", fileServer)

	// JSON REST API endpoint for quick health and metrics inspection
	mux.HandleFunc("/api/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		snap := collector.Snapshot(srv.Count(), simEngine.NodeCount(), simEngine.IsActive())
		_ = json.NewEncoder(w).Encode(snap)
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("🚀 Web Dashboard: http://localhost:%d", *port)
	log.Printf("⚡ Socket.IO Endpoint: ws://localhost:%d/socket.io/?EIO=4&transport=websocket", *port)
	log.Printf("📊 REST API: http://localhost:%d/api/metrics", *port)

	handler := corsMiddleware(mux)

	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

// corsMiddleware allows external frontends (like gosocketio-website) to connect via CORS
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Accept")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
