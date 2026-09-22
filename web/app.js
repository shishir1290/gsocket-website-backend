// Live Telemetry & Simulation Frontend Client
(function () {
  const socketStatusDot = document.getElementById("socketStatusDot");
  const socketStatusText = document.getElementById("socketStatusText");
  const pingValue = document.getElementById("pingValue");
  const uptimeValue = document.getElementById("uptimeValue");

  // KPI elements
  const pktsInSec = document.getElementById("pktsInSec");
  const pktsOutSec = document.getElementById("pktsOutSec");
  const totalPktsIn = document.getElementById("totalPktsIn");
  const totalPktsOut = document.getElementById("totalPktsOut");
  const simNodesCount = document.getElementById("simNodesCount");
  const activeClientsCount = document.getElementById("activeClientsCount");
  const heapAllocMB = document.getElementById("heapAllocMB");
  const goroutinesCount = document.getElementById("goroutinesCount");
  const nodesCountTag = document.getElementById("nodesCountTag");

  // Controls
  const toggleSimBtn = document.getElementById("toggleSimBtn");
  const toggleSimText = document.getElementById("toggleSimText");
  const nodeScaleSelect = document.getElementById("nodeScaleSelect");
  const intervalSelect = document.getElementById("intervalSelect");
  const burstBtn = document.getElementById("burstBtn");
  const alertBtn = document.getElementById("alertBtn");

  // Panels
  const nodesContainer = document.getElementById("nodesContainer");
  const terminalLogs = document.getElementById("terminalLogs");
  const clearLogsBtn = document.getElementById("clearLogsBtn");
  const alertBanner = document.getElementById("alertBanner");
  const alertBannerText = document.getElementById("alertBannerText");
  const dismissAlertBtn = document.getElementById("dismissAlertBtn");

  let simulationActive = true;
  const nodesMap = new Map();

  // Helper: Append log
  function addLog(type, text) {
    const timeStr = new Date().toLocaleTimeString();
    const div = document.createElement("div");
    div.className = `log-entry ${type}`;
    div.innerHTML = `<span class="log-time">[${timeStr}]</span> ${escapeHtml(text)}`;
    terminalLogs.appendChild(div);

    // Keep log max 150 items
    while (terminalLogs.children.length > 150) {
      terminalLogs.removeChild(terminalLogs.firstChild);
    }
    terminalLogs.scrollTop = terminalLogs.scrollHeight;
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  // Helper: Render or update node card
  function renderNode(node) {
    let card = document.getElementById(`card-${node.nodeId}`);
    if (!card) {
      card = document.createElement("div");
      card.id = `card-${node.nodeId}`;
      nodesContainer.appendChild(card);
    }

    card.className = `node-card ${node.status}`;
    card.innerHTML = `
      <div class="node-top">
        <div class="node-id">${escapeHtml(node.nodeName)}</div>
        <div class="node-status-badge ${node.status}">${node.status}</div>
      </div>
      <div class="node-meta">
        <span>Type: ${escapeHtml(node.nodeType)}</span>
        <span>${node.latencyMs.toFixed(2)} ms</span>
      </div>
      <div class="bar-container">
        <div class="bar-label">
          <span>CPU Load</span>
          <span>${node.cpuLoadPct.toFixed(1)}%</span>
        </div>
        <div class="bar-track">
          <div class="bar-fill ${node.status}" style="width: ${Math.min(100, Math.max(5, node.cpuLoadPct))}%"></div>
        </div>
      </div>
      <div class="node-meta">
        <span>Temp: ${node.temperatureC.toFixed(1)}°C</span>
        <span>Bat: ${node.batteryPct.toFixed(0)}%</span>
        <span>Seq: #${node.seqNumber}</span>
      </div>
    `;
  }

  // Clear logs button
  clearLogsBtn.addEventListener("click", () => {
    terminalLogs.innerHTML = "";
    addLog("sys", "Logs console cleared.");
  });

  // Dismiss alert
  dismissAlertBtn.addEventListener("click", () => {
    alertBanner.classList.add("hidden");
  });

  // Connect to pure-Go gsocketio server
  addLog("sys", "Connecting to gsocketio WebSocket at " + window.location.origin + "/socket.io/");
  
  const socket = io({
    transports: ["websocket", "polling"],
    reconnection: true,
    reconnectionAttempts: 10,
    reconnectionDelay: 1000,
  });

  // Socket Lifecycle
  socket.on("connect", () => {
    socketStatusDot.className = "status-dot connected";
    socketStatusText.textContent = `Online (SID: ${socket.id.substring(0, 8)})`;
    addLog("sys", `Connected to gsocketio! Transport: ${socket.io.engine.transport.name}, SID: ${socket.id}`);

    // Join telemetry dashboard room
    socket.emit("telemetry:subscribe", { client: "web-dashboard" });
    addLog("out", "Sent telemetry:subscribe request");
  });

  socket.on("disconnect", (reason) => {
    socketStatusDot.className = "status-dot disconnected";
    socketStatusText.textContent = "Disconnected";
    addLog("sys", `Disconnected from server. Reason: ${reason}`);
  });

  socket.on("connect_error", (err) => {
    socketStatusDot.className = "status-dot disconnected";
    socketStatusText.textContent = "Connection Error";
    addLog("alert", `Connection error: ${err.message}`);
  });

  // Ping Latency Measurement Loop
  setInterval(() => {
    if (socket.connected) {
      const start = performance.now();
      socket.emit("telemetry:ping", start, (resp) => {
        const latency = performance.now() - start;
        pingValue.textContent = `${latency.toFixed(2)} ms`;
      });
    }
  }, 2000);

  // Receive initial fleet snapshot
  socket.on("telemetry:nodes_init", (nodes) => {
    if (Array.isArray(nodes)) {
      nodesContainer.innerHTML = "";
      nodesMap.clear();
      nodes.forEach((n) => {
        nodesMap.set(n.nodeId, n);
        renderNode(n);
      });
      nodesCountTag.textContent = `${nodes.length} active`;
      simNodesCount.textContent = nodes.length;
      addLog("in", `Received fleet initial state: ${nodes.length} simulated nodes`);
    }
  });

  // Receive single node telemetry update
  socket.on("telemetry:node_update", (node) => {
    nodesMap.set(node.nodeId, node);
    renderNode(node);
  });

  // Receive global server telemetry metrics
  socket.on("telemetry:metrics", (metrics) => {
    pktsInSec.textContent = metrics.packetsPerSecIn.toLocaleString();
    pktsOutSec.textContent = metrics.packetsPerSecOut.toLocaleString();
    totalPktsIn.textContent = `Total In: ${metrics.totalPacketsIn.toLocaleString()} pkts`;
    totalPktsOut.textContent = `Total Out: ${metrics.totalPacketsOut.toLocaleString()} pkts`;
    simNodesCount.textContent = metrics.simulatedNodes;
    activeClientsCount.textContent = `Connected Sockets: ${metrics.activeClients}`;
    heapAllocMB.textContent = metrics.allocMB.toFixed(2);
    goroutinesCount.textContent = `Goroutines: ${metrics.goroutines} • GC Cycles: ${metrics.numGC}`;
    uptimeValue.textContent = `${Math.floor(metrics.uptimeSeconds)}s`;

    simulationActive = metrics.simulationActive;
    if (simulationActive) {
      toggleSimText.textContent = "Pause Simulation";
      toggleSimBtn.firstElementChild.textContent = "⏸";
    } else {
      toggleSimText.textContent = "Resume Simulation";
      toggleSimBtn.firstElementChild.textContent = "▶";
    }
  });

  // Receive critical alert
    // Receive broadcast messages from live simulation
  socket.on("chat", (data) => {
    const text = typeof data.payload === "object" ? JSON.stringify(data.payload) : data.payload;
    addLog("in", "💬 [Chat from " + (data.sender || "Peer") + "]: " + text);
  });

  socket.on("message", (data) => {
    const text = typeof data.payload === "object" ? JSON.stringify(data.payload) : data.payload;
    addLog("in", "✉️ [Message from " + (data.sender || "Peer") + "]: " + text);
  });

  socket.on("simulation:message", (data) => {
    const text = typeof data.payload === "object" ? JSON.stringify(data.payload) : data.payload;
    addLog("in", "📡 [Live Simulation from " + (data.sender || "Peer") + "]: " + text);
  });

  socket.on("broadcast", (data) => {
    const text = typeof data.payload === "object" ? JSON.stringify(data.payload) : data.payload;
    addLog("in", "📢 [Broadcast from " + (data.sender || "Peer") + "]: " + text);
  });

  socket.on("room_notification", (notif) => {
    addLog("sys", "🚪 " + notif.message);
  });

  socket.on("telemetry:alert", (alert) => {
    addLog("alert", `[ALERT] ${alert.nodeId}: ${alert.message} (${alert.metric}=${alert.value.toFixed(1)})`);
    alertBannerText.textContent = `[${alert.severity.toUpperCase()}] ${alert.message}`;
    alertBanner.classList.remove("hidden");
  });

  // Controls Event Listeners
  toggleSimBtn.addEventListener("click", () => {
    const newState = !simulationActive;
    socket.emit("simulation:toggle", { active: newState });
    addLog("out", `Emitted simulation:toggle -> ${newState ? "RUNNING" : "PAUSED"}`);
  });

  nodeScaleSelect.addEventListener("change", (e) => {
    const count = parseInt(e.target.value, 10);
    socket.emit("simulation:scale", { count });
    addLog("out", `Emitted simulation:scale -> ${count} nodes`);
  });

  intervalSelect.addEventListener("change", (e) => {
    const intervalMs = parseInt(e.target.value, 10);
    socket.emit("simulation:interval", { intervalMs });
    addLog("out", `Emitted simulation:interval -> ${intervalMs} ms`);
  });

  burstBtn.addEventListener("click", () => {
    socket.emit("simulation:burst", { count: 500 });
    addLog("out", "Emitted simulation:burst (500 packets)");
  });

  alertBtn.addEventListener("click", () => {
    socket.emit("simulation:alert_trigger", {});
    addLog("out", "Emitted simulation:alert_trigger");
  });
})();
