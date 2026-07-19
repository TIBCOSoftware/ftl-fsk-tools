'use strict';
const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const { spawn } = require('child_process');
const path      = require('path');
const fs        = require('fs');
const net       = require('net');
const { Kafka, logLevel } = require('kafkajs');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });
const PORT   = process.env.PORT || 3000;

// run-kafka-to-kof.sh lives one directory above ui/
const SCRIPT_PATH = path.resolve(__dirname, '../run-kafka-to-kof.sh');

// Read a property value from a .properties file (ignores comment lines)
function readProp(file, key) {
  try {
    for (const line of fs.readFileSync(file, 'utf8').split('\n')) {
      if (line.trim().startsWith('#')) continue;
      const m = line.match(new RegExp('^' + key.replace(/\./g, '\\.') + '\\s*=\\s*(.+)'));
      if (m) return m[1].trim();
    }
  } catch (_) {}
  return null;
}

const MIGRATION_CONFIG = path.resolve(__dirname, '../kof-output/kafka-to-kof.properties');
const sourceBootstrap =
  readProp(MIGRATION_CONFIG, 'source.bootstrap.servers') ||
  process.env.SOURCE_BOOTSTRAP ||
  'localhost:9092,localhost:9093,localhost:9094';
const targetBootstrap =
  readProp(MIGRATION_CONFIG, 'target.bootstrap.servers') ||
  process.env.TARGET_BOOTSTRAP ||
  'localhost:9092,localhost:9093,localhost:9094';

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ── State ─────────────────────────────────────────────────────────────────────

let activeProc = null;
let replicationState = {
  status: 'idle',
  topics: [],
  stats: { planned: 0, published: 0 },
  startTime: null
};

// Kafka cluster: TCP health + KafkaJS on sourceBootstrap
let kafkaCluster = {
  bootstrap: sourceBootstrap,
  brokers: [
    { id: 1, host: 'localhost', port: 9092, status: 'unknown' },
    { id: 2, host: 'localhost', port: 9093, status: 'unknown' },
    { id: 3, host: 'localhost', port: 9094, status: 'unknown' }
  ],
  topics: [],
  lastUpdated: null
};

// KOF cluster: TCP health on FTL core.servers ports (5635/5620/5623) turns nodes green
// as soon as tibftlserver processes start. KafkaJS topic probe uses targetBootstrap
// and only succeeds once KOF's Kafka layer is fully initialised.
let kofCluster = {
  bootstrap: targetBootstrap,
  nodes: [
    { id: 1, host: 'localhost', port: 5635, status: 'unknown' },
    { id: 2, host: 'localhost', port: 5620, status: 'unknown' },
    { id: 3, host: 'localhost', port: 5623, status: 'unknown' }
  ],
  topics: [],
  lastUpdated: null
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function broadcast(msg) {
  const data = JSON.stringify(msg);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
}

function tcpCheck(host, port, timeoutMs = 2000) {
  return new Promise(resolve => {
    const sock = new net.Socket();
    let done = false;
    const finish = ok => { if (!done) { done = true; sock.destroy(); resolve(ok); } };
    sock.setTimeout(timeoutMs);
    sock.connect(port, host, () => finish(true));
    sock.on('error', () => finish(false));
    sock.on('timeout', () => finish(false));
  });
}

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
    try { await admin.disconnect(); } catch (_) {}
    return { ok: false, error: err.message };
  }
}

// ── Cluster refresh ───────────────────────────────────────────────────────────

async function refreshKafkaCluster() {
  const checks = await Promise.all(kafkaCluster.brokers.map(b => tcpCheck(b.host, b.port)));
  kafkaCluster.brokers.forEach((b, i) => { b.status = checks[i] ? 'online' : 'offline'; });

  if (checks.some(Boolean)) {
    const result = await fetchKafkaMetadata(kafkaCluster.bootstrap);
    if (result.ok) {
      result.cluster.brokers.forEach(rb => {
        const local = kafkaCluster.brokers.find(b => b.id === rb.nodeId || b.port === rb.port);
        if (local) {
          local.id = rb.nodeId; local.host = rb.host; local.port = rb.port;
          local.rack = rb.rack || null; local.status = 'online';
        }
      });
      kafkaCluster.topics       = result.topics;
      kafkaCluster.controllerId = result.cluster.controller;
      kafkaCluster.clusterId    = result.cluster.clusterId;
    }
  }
  kafkaCluster.lastUpdated = Date.now();
  broadcast({ type: 'kafka_cluster', cluster: kafkaCluster });
}

async function refreshKofCluster() {
  // TCP-check on FTL core.servers ports — turns nodes green as soon as tibftlserver is up
  const checks = await Promise.all(kofCluster.nodes.map(n => tcpCheck(n.host, n.port)));
  kofCluster.nodes.forEach((n, i) => { n.status = checks[i] ? 'online' : 'offline'; });

  if (checks.some(Boolean)) {
    // KafkaJS probe on targetBootstrap (Kafka-compatible port); silently ignored if not ready
    const result = await fetchKafkaMetadata(kofCluster.bootstrap);
    if (result.ok) {
      kofCluster.topics    = result.topics;
      kofCluster.clusterId = result.cluster.clusterId;
    }
  }
  kofCluster.lastUpdated = Date.now();
  broadcast({ type: 'kof_cluster', cluster: kofCluster });
}

// ── Log parser ────────────────────────────────────────────────────────────────

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
  if (planMatch) replicationState.stats.planned = parseInt(planMatch[1]);

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

// ── REST endpoints ────────────────────────────────────────────────────────────

// Demo defaults — read live from kafka-to-kof.properties so the config form pre-populates
app.get('/api/demo/defaults', (req, res) => {
  const cfg = path.resolve(__dirname, '../kof-output/kafka-to-kof.properties');
  const src = readProp(cfg, 'source.bootstrap.servers') || sourceBootstrap;
  const tgt = readProp(cfg, 'target.bootstrap.servers') || targetBootstrap;
  res.json({
    sourceBootstrap: src,
    targetBootstrap: tgt,
    configFile:      '../kof-output/kafka-to-kof.properties',
    kafkaClasspath:  process.env.KAFKA_CLASSPATH || ''
  });
});

// Run replication (streams stdout/stderr via WebSocket)
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
    setTimeout(refreshKofCluster, 2000);
  });

  res.json({ started: true });
});

app.post('/api/stop', (req, res) => {
  if (activeProc) activeProc.kill('SIGTERM');
  res.json({ stopped: true });
});

app.get('/api/state', (req, res) => res.json(replicationState));

// Kafka cluster stats (accepts ?bootstrap= to update live)
app.get('/api/kafka-stats', async (req, res) => {
  if (req.query.bootstrap) {
    kafkaCluster.bootstrap = req.query.bootstrap;
    const parts = req.query.bootstrap.split(',').map(b => {
      const [h, p] = b.trim().split(':');
      return { host: h, port: parseInt(p), status: 'unknown' };
    });
    if (parts.length >= 1) kafkaCluster.brokers = parts.map((p, i) => ({ id: i + 1, ...p }));
  }
  await refreshKafkaCluster();
  res.json(kafkaCluster);
});

// KOF cluster stats
app.get('/api/kof-stats', async (req, res) => {
  if (req.query.bootstrap) {
    kofCluster.bootstrap = req.query.bootstrap;
    const parts = req.query.bootstrap.split(',').map(b => {
      const [h, p] = b.trim().split(':');
      return { host: h, port: parseInt(p), status: 'unknown' };
    });
    if (parts.length >= 1) kofCluster.nodes = parts.map((p, i) => ({ id: i + 1, ...p }));
  }
  await refreshKofCluster();
  res.json(kofCluster);
});

// Update bootstrap config
app.post('/api/config', (req, res) => {
  if (req.body.sourceBootstrap) kafkaCluster.bootstrap = req.body.sourceBootstrap;
  if (req.body.targetBootstrap) kofCluster.bootstrap   = req.body.targetBootstrap;
  res.json({ ok: true });
});

// ── WebSocket ─────────────────────────────────────────────────────────────────

wss.on('connection', ws => {
  ws.send(JSON.stringify({ type: 'state',        state:   replicationState }));
  ws.send(JSON.stringify({ type: 'kafka_cluster', cluster: kafkaCluster    }));
  ws.send(JSON.stringify({ type: 'kof_cluster',   cluster: kofCluster      }));
});

// ── Polling ───────────────────────────────────────────────────────────────────

setInterval(() => {
  refreshKafkaCluster().catch(() => {});
  refreshKofCluster().catch(() => {});
}, 10000);

setTimeout(() => {
  refreshKafkaCluster().catch(() => {});
  refreshKofCluster().catch(() => {});
}, 2000);

// ── Start ─────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log('Kafka → KOF Migration Console: http://localhost:' + PORT);
  console.log('  Source bootstrap:  ' + kafkaCluster.bootstrap);
  console.log('  Target bootstrap:  ' + kofCluster.bootstrap);
  console.log('  KOF health ports:  ' + kofCluster.nodes.map(n => n.port).join(', ') + ' (FTL core.servers)');
});
