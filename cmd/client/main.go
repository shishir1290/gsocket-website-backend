package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"telemetry-simulation/internal/telemetry"
)

type HandshakeResponse struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
}

type ClientSession struct {
	BaseURL    string
	SID        string
	HTTPClient *http.Client
}

func NewSession(baseURL string) (*ClientSession, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	handshakeURL := fmt.Sprintf("%s/socket.io/?EIO=4&transport=polling", baseURL)

	resp, err := client.Get(handshakeURL)
	if err != nil {
		return nil, fmt.Errorf("handshake request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading handshake response: %w", err)
	}

	bodyStr := string(body)
	if !strings.HasPrefix(bodyStr, "0") {
		return nil, fmt.Errorf("unexpected handshake frame: %s", bodyStr)
	}

	var hs HandshakeResponse
	if err := json.Unmarshal([]byte(bodyStr[1:]), &hs); err != nil {
		return nil, fmt.Errorf("parsing handshake json: %w", err)
	}

	session := &ClientSession{
		BaseURL:    baseURL,
		SID:        hs.SID,
		HTTPClient: client,
	}

	// Send Socket.IO Connect packet "40"
	connectPacket := "40"
	if err := session.sendPacket(connectPacket); err != nil {
		return nil, fmt.Errorf("sending connect packet: %w", err)
	}

	return session, nil
}

func (s *ClientSession) sendPacket(packet string) error {
	postURL := fmt.Sprintf("%s/socket.io/?EIO=4&transport=polling&sid=%s", s.BaseURL, url.QueryEscape(s.SID))
	resp, err := s.HTTPClient.Post(postURL, "text/plain;charset=UTF-8", strings.NewReader(packet))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("post status %d", resp.StatusCode)
	}
	return nil
}

// EmitEvent sends an Engine.IO (4) + Socket.IO (2) EVENT packet: 42["event", payload]
func (s *ClientSession) EmitEvent(event string, payload interface{}) error {
	dataBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	var packetBuf bytes.Buffer
	packetBuf.WriteString(fmt.Sprintf("42[%q,", event))
	packetBuf.Write(dataBytes)
	packetBuf.WriteString("]")

	return s.sendPacket(packetBuf.String())
}

func main() {
	serverURL := flag.String("server", "http://localhost:8080", "Server base URL")
	concurrency := flag.Int("clients", 5, "Number of concurrent simulated clients")
	duration := flag.Duration("duration", 15*time.Second, "Load test duration (e.g. 10s, 30s)")
	tickInterval := flag.Duration("interval", 300*time.Millisecond, "Packet emission interval per client")
	flag.Parse()

	log.Println("==================================================================")
	log.Printf("🚀 Starting Standalone Go Telemetry Simulation Test Client")
	log.Printf("   Target: %s | Clients: %d | Duration: %s | Interval: %s", *serverURL, *concurrency, *duration, *tickInterval)
	log.Println("==================================================================")

	var (
		totalSent uint64
		totalErr  uint64
		wg        sync.WaitGroup
	)

	stopChan := make(chan struct{})
	time.AfterFunc(*duration, func() {
		close(stopChan)
	})

	startTime := time.Now()

	for i := 1; i <= *concurrency; i++ {
		wg.Add(1)
		clientID := fmt.Sprintf("ext-sensor-%03d", i)

		go func(id string, index int) {
			defer wg.Done()

			session, err := NewSession(*serverURL)
			if err != nil {
				log.Printf("[%s] Handshake error: %v", id, err)
				atomic.AddUint64(&totalErr, 1)
				return
			}
			log.Printf("[%s] Connected successfully (SID: %s)", id, session.SID)

			ticker := time.NewTicker(*tickInterval)
			defer ticker.Stop()

			var seq uint64

			for {
				select {
				case <-stopChan:
					return
				case <-ticker.C:
					seq++
					reading := telemetry.DeviceTelemetry{
						NodeID:       id,
						NodeName:     fmt.Sprintf("External-Node-%03d", index),
						NodeType:     "remote-iot",
						Status:       telemetry.StatusNominal,
						BatteryPct:   90.0 - float64(seq%50)*0.1,
						TemperatureC: 42.0 + (rand.Float64()-0.5)*5.0,
						CPULoadPct:   25.0 + (rand.Float64()-0.5)*15.0,
						MemoryMB:     128.0,
						LatencyMs:    0.25,
						PacketsSent:  seq,
						SeqNumber:    seq,
						Timestamp:    time.Now(),
					}

					if err := session.EmitEvent("telemetry:ingest", reading); err != nil {
						atomic.AddUint64(&totalErr, 1)
					} else {
						atomic.AddUint64(&totalSent, 1)
					}
				}
			}
		}(clientID, i)
	}

	// Live stats monitor in terminal
	go func() {
		statTicker := time.NewTicker(1 * time.Second)
		defer statTicker.Stop()
		var lastSent uint64

		for {
			select {
			case <-stopChan:
				return
			case <-statTicker.C:
				curr := atomic.LoadUint64(&totalSent)
				errs := atomic.LoadUint64(&totalErr)
				rate := curr - lastSent
				lastSent = curr
				fmt.Printf("⏱  Elapsed: %-4s | Sent: %-6d pkts | Rate: %-4d pkts/s | Errors: %d\n",
					time.Since(startTime).Round(time.Second), curr, rate, errs)
			}
		}
	}()

	wg.Wait()
	elapsed := time.Since(startTime).Seconds()
	sent := atomic.LoadUint64(&totalSent)
	errs := atomic.LoadUint64(&totalErr)

	log.Println("==================================================================")
	log.Println("🏁 Load Simulation Test Completed!")
	log.Printf("   Total Packets Sent: %d", sent)
	log.Printf("   Total Errors:       %d", errs)
	log.Printf("   Average Throughput: %.1f pkts/second", float64(sent)/elapsed)
	log.Printf("   Duration:           %.2fs", elapsed)
	log.Println("==================================================================")
}
