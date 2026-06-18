
'use strict';
const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const { spawn } = require('child_process');
const path      = require('path');
const net       = require('net');
const { Kafka, logLevel } = require('kafkajs');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });
const PORT   = process.env.PORT || 3333;
const SCRIPT_PATH = path.resolve(__dirname, '../run-kafka-to-kof.sh');

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// Replication state
let activeProc = null;
let replicationState = {
  status: 'idle',
  topics: [],
  stats: { planned: 0, published: 0 },
  startTime: null
};

// Cluster state
let kafkaCluster = {
  bootstrap: 'localhost:9092,localhost:9094,localhost:9096',
  brokers: [
    { id: 1, host: 'localhost', port: 9092, status: 'unknown' },
    { id: 2, host: 'localhost', port: 9094, status: 'unknown' },
    { id: 3, host: 'localhost', port: 9096, status: 'unknown' }
  ],
  topics: [],
  lastUpdated: null
};

let kofCluster = {
  bootstrap: 'localhost:5663,localhost:5673,localhost:5683',
  nodes: [
    { id: 1, host: 'localhost', port: 5663, ftlPort: 5663, status: 'unknown' },
    { id: 2, host: 'localhost', port: 5673, ftlPort: 5673, status: 'unknown' },
    { id: 3, host: 'localhost', port: 5683, ftlPort: 5683, status: 'unknown' }
  ],
  topics: [],
  lastUpdated: null
};

// Broadcast
function broadcast(msg) {
  const data = JSON.stringify(msg);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
}

// TCP health check
function tcpCheck(host, port, timeoutMs = 2000) {
  return new Promise(resolve => {
    const sock = new net.Socket();
    let done = false;
    const finish = ok => {
      if (!done) { done = true; sock.destroy(); resolve(ok); }
    };
    sock.setTimeout(timeoutMs);
    sock.connect(port, host, () => finish(true));
    sock.on('error', () => finish(false));
    sock.on('timeout', () => finish(false));
  });
}

// Get real Kafka metadata via kafkajs
async function fetchKafkaMetadata(bootstrap) {
  const kafka = new Kafka({
    clientId: 'kof-ui-probe',
    brokers: bootstrap.split(',').map(b => b.trim()),
    connectionTimeout: 4000,
    requestTimeout: 6000,
    logLevel: logLevel.NOTHING
  });
  const admin = kafka.admin();
  try {
    await admin.connect();
    const clusterInfo = await admin.describeCluster();
    const topicList   = await admin.listTopics();
    // Get topic offsets for sizing estimate
    let topicDetails = [];
    if (topicList.length > 0) {
      const meta = await admin.fetchTopicMetadata({ topics: topicList.slice(0, 20) });
      for (const tm of meta.topics) {
        const offsets = await admin.fetchTopicOffsets(tm.name);
        const totalMsgs = offsets.reduce((sum, p) => sum + (parseInt(p.high) - parseInt(p.low)), 0);
        topicDetails.push({
          name: tm.name,
          partitions: tm.partitions.length,
          leader: tm.partitions[0] ? tm.partitions[0].leader : -1,
          messages: totalMsgs
        });
      }
    }
    await admin.disconnect();
    return { ok: true, cluster: clusterInfo, topics: topicDetails };
  } catch (err) {
    try { await admin.disconnect(); } catch(_) {}
    return { ok: false, error: err.message };
  }
}

// Update Kafka cluster state
async function refreshKafkaCluster() {
  // First do TCP checks on each broker
  const checks = await Promise.all(
    kafkaCluster.brokers.map(b => tcpCheck(b.host, b.port))
  );
  kafkaCluster.brokers.forEach((b, i) => {
    b.status = checks[i] ? 'online' : 'offline';
  });

  const anyOnline = checks.some(Boolean);
  if (anyOnline) {
    const result = await fetchKafkaMetadata(kafkaCluster.bootstrap);
    if (result.ok) {
      // Merge real broker info
      result.cluster.brokers.forEach(rb => {
        const local = kafkaCluster.brokers.find(b => b.id === rb.nodeId || b.port === rb.port);
        if (local) {
          local.id = rb.nodeId;
          local.host = rb.host;
          local.port = rb.port;
          local.rack = rb.rack || null;
          local.status = 'online';
        }
      });
      kafkaCluster.topics = result.topics;
      kafkaCluster.controllerId = result.cluster.controller;
      kafkaCluster.clusterId    = result.cluster.clusterId;
    }
  }
  kafkaCluster.lastUpdated = Date.now();
  broadcast({ type: 'kafka_cluster', cluster: kafkaCluster });
}

// Update KOF cluster state
async function refreshKofCluster() {
  const checks = await Promise.all(
    kofCluster.nodes.map(n => tcpCheck(n.host, n.port))
  );
  kofCluster.nodes.forEach((n, i) => {
    n.status = checks[i] ? 'online' : 'offline';
  });

  const anyOnline = checks.some(Boolean);
  if (anyOnline) {
    // KOF presents Kafka-compatible API - use same probe
    const result = await fetchKafkaMetadata(kofCluster.bootstrap);
    if (result.ok) {
      kofCluster.topics = result.topics;
      kofCluster.clusterId = result.cluster.clusterId;
    }
  }
  kofCluster.lastUpdated = Date.now();
  broadcast({ type: 'kof_cluster', cluster: kofCluster });
}

// Log parser
function parseLine(line) {
  const topicMatch = line.match(/Topic:\s+(\S+)/i) || line.match(/topic[=\s]+['"]?([a-zA-Z0-9._-]+)/i);
  if (topicMatch) {
    const t = topicMatch[1];
    if (!replicationState.topics.includes(t)) {
      replicationState.topics.push(t);
      broadcast({ type: 'topic_discovered', topic: t });
    }
  }
  const pubMatch = line.match(/Replicated\s+(\d+)/i) || line.match(/published[:\s]+(\d+)/i);
  if (pubMatch) {
    replicationState.stats.published = parseInt(pubMatch[1]);
    broadcast({ type: 'stats', stats: replicationState.stats });
  }
  const planMatch = line.match(/planned.*?(\d+)/i);
  if (planMatch) {
    replicationState.stats.planned = parseInt(planMatch[1]);
  }
  if (/Starting Kafka.*KOF|KafkaToKof.*start/i.test(line)) {
    replicationState.status = 'running';
    broadcast({ type: 'status', status: 'running' });
  }
  if (/Replication complete|All done|successfully|Finished/i.test(line)) {
    replicationState.status = 'done';
    broadcast({ type: 'status', status: 'done' });
  }
  if (replicationState.topics.length > 0 && replicationState.status === 'running') {
    const t = replicationState.topics[Math.floor(Math.random() * replicationState.topics.length)];
    broadcast({ type: 'flow', topic: t, ts: Date.now() });
  }
}

// REST: Run replication
app.post('/api/run', (req, res) => {
  if (activeProc) return res.status(409).json({ error: 'Already running' });
  const { args, kafkaClasspath } = req.body;
  if (!kafkaClasspath) return res.status(400).json({ error: 'KAFKA_CLASSPATH required' });

  replicationState = { status: 'running', topics: [], stats: { planned: 0, published: 0 }, startTime: Date.now() };
  broadcast({ type: 'status', status: 'running' });

  const env = { ...process.env, KAFKA_CLASSPATH: kafkaClasspath };
  activeProc = spawn('bash', [SCRIPT_PATH, ...(args || [])], { env, cwd: path.dirname(SCRIPT_PATH) });

  activeProc.stdout.on('data', d => {
    d.toString().split('\n').forEach(line => {
      if (line.trim()) { broadcast({ type: 'log', stream: 'stdout', line }); parseLine(line); }
    });
  });
  activeProc.stderr.on('data', d => {
    d.toString().split('\n').forEach(line => {
      if (line.trim()) { broadcast({ type: 'log', stream: 'stderr', line }); parseLine(line); }
    });
  });
  activeProc.on('close', code => {
    activeProc = null;
    replicationState.status = code === 0 ? 'done' : 'error';
    broadcast({ type: 'status', status: replicationState.status, code });
    broadcast({ type: 'log', stream: 'system', line: `Process exited (code ${code})` });
    // Refresh KOF stats after run completes
    setTimeout(refreshKofCluster, 2000);
  });

  res.json({ started: true });
});

app.post('/api/stop', (req, res) => {
  if (activeProc) activeProc.kill('SIGTERM');
  res.json({ stopped: true });
});

app.get('/api/state', (req, res) => res.json(replicationState));

// REST: Kafka cluster stats
app.get('/api/kafka-stats', async (req, res) => {
  // Update bootstrap from query param if provided
  if (req.query.bootstrap) {
    kafkaCluster.bootstrap = req.query.bootstrap;
    const parts = req.query.bootstrap.split(',').map(b => {
      const [h, p] = b.trim().split(':');
      return { host: h, port: parseInt(p), status: 'unknown' };
    });
    if (parts.length === 3) {
      kafkaCluster.brokers = parts.map((p, i) => ({ id: i+1, ...p }));
    }
  }
  await refreshKafkaCluster();
  res.json(kafkaCluster);
});

// REST: KOF cluster stats
app.get('/api/kof-stats', async (req, res) => {
  if (req.query.bootstrap) {
    kofCluster.bootstrap = req.query.bootstrap;
    const parts = req.query.bootstrap.split(',').map(b => {
      const [h, p] = b.trim().split(':');
      return { host: h, port: parseInt(p), status: 'unknown' };
    });
    if (parts.length > 0) {
      kofCluster.nodes = parts.map((p, i) => ({ id: i+1, ...p, ftlPort: 8585 + i*100 }));
    }
  }
  await refreshKofCluster();
  res.json(kofCluster);
});

// REST: Update bootstrap addresses
app.post('/api/config', (req, res) => {
  if (req.body.sourceBootstrap) kafkaCluster.bootstrap = req.body.sourceBootstrap;
  if (req.body.targetBootstrap) kofCluster.bootstrap   = req.body.targetBootstrap;
  res.json({ ok: true });
});

// WebSocket: send full state on connect
wss.on('connection', ws => {
  ws.send(JSON.stringify({ type: 'state', state: replicationState }));
  ws.send(JSON.stringify({ type: 'kafka_cluster', cluster: kafkaCluster }));
  ws.send(JSON.stringify({ type: 'kof_cluster',   cluster: kofCluster   }));
});

// Periodic health polling (every 10s)
setInterval(() => {
  refreshKafkaCluster().catch(() => {});
  refreshKofCluster().catch(() => {});
}, 10000);

// Initial probe after 2s startup
setTimeout(() => {
  refreshKafkaCluster().catch(() => {});
  refreshKofCluster().catch(() => {});
}, 2000);

server.listen(PORT, () => {
  console.log('Kafka-to-KOF UI running at http://localhost:' + PORT);
});
