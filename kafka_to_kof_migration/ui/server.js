'use strict';

const express  = require('express');
const { execFile, spawn } = require('child_process');
const path  = require('path');
const fs    = require('fs');

const app  = express();
const PORT = process.env.PORT || 3000;

const KAFKA_HOME       = process.env.KAFKA_HOME || '/usr/local/Cellar/kafka/4.2.0/libexec';
const SOURCE_BOOTSTRAP = process.env.SOURCE_BOOTSTRAP || 'localhost:9092';
const TARGET_BOOTSTRAP = process.env.TARGET_BOOTSTRAP || 'localhost:9092';

// Paths relative to the ui/ directory
const UI_DIR        = __dirname;
const MIGRATION_DIR = path.resolve(UI_DIR, '..');
const DEMO_DIR      = path.join(MIGRATION_DIR, 'demo');
const LOG_FILE      = path.join(DEMO_DIR, 'migration.log');

app.use(express.json());
app.use(express.static(path.join(UI_DIR, 'public')));

// ── helpers ────────────────────────────────────────────────────────────────────

function kafkaBin(script) {
  return path.join(KAFKA_HOME, 'bin', script);
}

/**
 * Get end offsets (message counts) for all partitions of a topic.
 * Returns a promise resolving to the sum across partitions.
 */
function getTopicMessageCount(bootstrap, topic) {
  return new Promise((resolve) => {
    execFile(kafkaBin('kafka-run-class.sh'),
      ['kafka.tools.GetOffsetShell',
       '--bootstrap-server', bootstrap,
       '--topic', topic,
       '--time', '-1'],
      { timeout: 10000 },
      (err, stdout) => {
        if (err) { resolve(0); return; }
        let total = 0;
        for (const line of stdout.split('\n')) {
          const parts = line.trim().split(':');
          if (parts.length === 3) total += parseInt(parts[2], 10) || 0;
        }
        resolve(total);
      }
    );
  });
}

/**
 * List topics on a bootstrap server.
 * Returns a promise resolving to string[].
 */
function listTopics(bootstrap) {
  return new Promise((resolve) => {
    execFile(kafkaBin('kafka-topics.sh'),
      ['--bootstrap-server', bootstrap, '--list'],
      { timeout: 10000 },
      (err, stdout) => {
        if (err) { resolve([]); return; }
        const topics = stdout.split('\n')
          .map(t => t.trim())
          .filter(t => t && !t.startsWith('__'));
        resolve(topics);
      }
    );
  });
}

/**
 * Build topic status for a cluster: [{name, messageCount}].
 */
async function clusterStatus(bootstrap) {
  const topics = await listTopics(bootstrap);
  const counts = await Promise.all(topics.map(t => getTopicMessageCount(bootstrap, t)));
  return topics.map((name, i) => ({ name, messageCount: counts[i] }));
}

// ── API endpoints ──────────────────────────────────────────────────────────────

// GET /api/status  — source and target topic counts
app.get('/api/status', async (req, res) => {
  try {
    const [sourceTopics, targetTopics] = await Promise.all([
      clusterStatus(SOURCE_BOOTSTRAP),
      clusterStatus(TARGET_BOOTSTRAP),
    ]);
    res.json({
      source: { bootstrap: SOURCE_BOOTSTRAP, topics: sourceTopics },
      target: { bootstrap: TARGET_BOOTSTRAP, topics: targetTopics },
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/migrate/log  — last 200 lines of migration.log
app.get('/api/migrate/log', (req, res) => {
  if (!fs.existsSync(LOG_FILE)) {
    res.json({ lines: [] });
    return;
  }
  const content = fs.readFileSync(LOG_FILE, 'utf8');
  const lines   = content.split('\n');
  res.json({ lines: lines.slice(-200) });
});

let migrationProcess = null;

function startMigration(dryRunOnly, res) {
  if (migrationProcess) {
    res.status(409).json({ error: 'Migration already running' });
    return;
  }

  const args = ['demo/run-migration.sh'];
  if (dryRunOnly) args.push('--dry-run-only');

  // Clear old log
  fs.writeFileSync(LOG_FILE, '');

  migrationProcess = spawn('bash', args, {
    cwd: MIGRATION_DIR,
    env: { ...process.env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  const logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
  migrationProcess.stdout.pipe(logStream);
  migrationProcess.stderr.pipe(logStream);

  migrationProcess.on('close', () => {
    migrationProcess = null;
  });

  res.json({ started: true, dryRun: dryRunOnly, log: '/api/migrate/log' });
}

// POST /api/migrate/dry-run
app.post('/api/migrate/dry-run', (req, res) => startMigration(true, res));

// POST /api/migrate/run
app.post('/api/migrate/run', (req, res) => startMigration(false, res));

// GET /api/migrate/status
app.get('/api/migrate/status', (req, res) => {
  res.json({ running: migrationProcess !== null });
});

// ── Start ──────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`Kafka → KOF Migration Dashboard`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`  Source: ${SOURCE_BOOTSTRAP}   Target: ${TARGET_BOOTSTRAP}`);
  console.log(`  KAFKA_HOME: ${KAFKA_HOME}`);
});
