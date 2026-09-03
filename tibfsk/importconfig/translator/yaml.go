/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PortMap holds the generated FTL ports for a cluster of N pservers.
type PortMap struct {
	RealmPorts   []int // FTL realm server listening ports (first 3 used for primary cluster)
	PserverPorts []int // FTL ports pservers listen on (one per pserver)
}

// CoreServer is a named FTL realm server address used in globals.core.servers.
type CoreServer struct {
	Name    string // e.g. "SRV1"
	Address string // e.g. "host:5600"
}

// DefaultFTLLogLevel is the loglevel written into each generated pserver in the
// cluster YAML (the output FTL servers' logging) when --ftl-loglevel is not
// supplied. It is not this tool's own logging level.
const DefaultFTLLogLevel = "connections:info;kof:info;durables:info;store:info"

// DROpts holds Disaster Recovery configuration for generated files.
// DR mode is enabled when DRServers is non-empty (i.e. --dr-servers was provided).
type DROpts struct {
	DRServers []CoreServer // DR server list parsed from --dr-servers
	DRDataDir string       // data directory for DR pservers
}

// Enabled returns true when DR mode is active.
func (o DROpts) Enabled() bool { return len(o.DRServers) > 0 }

// ClusterOpts holds the settings that apply to every generated cluster YAML,
// independent of security and DR.
type ClusterOpts struct {
	// LogLevel is written as the loglevel of each pserver.
	LogLevel string

	// DisableDiskIndex writes "default.cluster.disk.index: 'false'" into every
	// realm block, turning off the default cluster's disk index. tibftlserver
	// enables the index by default whenever disk persistence is sync or async,
	// so it has to be switched off explicitly. Set by the undocumented
	// -disable-disk-index flag.
	DisableDiskIndex bool

	// DiskPersistence is the -disk-persistence flag token ("async", "sync" or
	// "in-memory") setting disk_persistence on the generated kof.cluster. The empty
	// string means the default, async. See RealmDiskPersistence.
	DiskPersistence string

	// Tibschemad adds the FTL schema daemon to every server that carries a realm
	// block: a second "- persistence: name: schemaN" plus a "- tibschemad:" entry.
	// No extra FTL servers and no extra ports -- the schema pserver shares the
	// tibftlserver process that already hosts the FSK pserver. Set by --tibschemad.
	Tibschemad bool

	// ReplicationFactor is the number of pservers in each kof.cluster shard, already
	// resolved by main from --replication-factor: 1, 3 or 5, or 1 for a lone broker.
	// main also validates that the pserver count is an exact multiple of it, so no
	// shard is ever short. Zero means the default, 3. See RF.
	ReplicationFactor int
}

// RF returns the resolved replication factor: the number of pservers per
// kof.cluster shard. main always sets a concrete value, so the zero case is only
// reachable from a ClusterOpts literal (the tests build them directly).
func (o ClusterOpts) RF() int {
	if o.ReplicationFactor <= 0 {
		return 3
	}
	return o.ReplicationFactor
}

// clusterYAMLStem returns the base name for every generated tibftlserver YAML.
// A single-pserver deployment is a standalone server, not a cluster, so it gets
// its own stem. n is the primary pserver count (min3 of the broker count).
func clusterYAMLStem(n int) string {
	if n <= 1 {
		return "tibftlserver_standalone"
	}
	return "tibftlserver-cluster"
}

// writeTibschemadBlock appends the schema daemon entries to the server currently
// being written. size is the number of servers in the schemad cluster, which is
// the primary pserver count -- one schema pserver per realm server.
func writeTibschemadBlock(f *os.File, n, size int) {
	fmt.Fprintln(f, "  - persistence:")
	fmt.Fprintf(f, "      name: schema%d\n", n)
	fmt.Fprintln(f, "  - tibschemad:")
	fmt.Fprintln(f, "      auth.type: none")
	fmt.Fprintf(f, "      cluster.size: %d\n", size)
}

// writeServerLogging appends the server-wide logging settings to the
// "- ftlserver.properties:" block currently being written. name is the servers: key,
// so the suggested log file matches the "tibftlserver -n <name>" that starts it;
// dataDir is --data-dir.
//
// loglevel here reaches every service in the process that does not set one of its own.
// The pserver sets its own (see the callers) and keeps it; the realm service does not.
//
// logfile is commented out because tibftlserver logs to stdout by default. max.log.size
// and max.logs are ignored while logfile is unset, and tibftlserver rejects a logfile
// given without them, so the three are commented and uncommented together.
func writeServerLogging(f *os.File, name, dataDir string) {
	fmt.Fprintln(f, "      loglevel: info")
	fmt.Fprintf(f, "      #logfile: %s/%s.log\n", dataDir, name)
	fmt.Fprintln(f, "      #max.log.size: 10240000")
	fmt.Fprintln(f, "      #max.logs: 10")
}

// writeRealmBlock writes a server's "- realm:" entry. Every generated YAML carries
// the realm settings per server rather than in a shared services section, so there
// is no services block in any file this tool writes.
//
// label is "" for a non-DR realm. realmPath is "" for realms that do not seed the realm
// configuration (currently none). extra holds already-formatted "key: value" entries (the
// secure YAML's realm credentials) written between the label and the data path.
func writeRealmBlock(f *os.File, dataDir, realmPath, label string, opts ClusterOpts, extra ...string) {
	fmt.Fprintln(f, "  - realm:")
	if label != "" {
		fmt.Fprintf(f, "      label: %s\n", label)
	}
	for _, e := range extra {
		fmt.Fprintf(f, "      %s\n", e)
	}
	fmt.Fprintf(f, "      data: %s\n", dataDir)
	if realmPath != "" {
		fmt.Fprintf(f, "      initial.realm.config: %s\n", realmPath)
	}
	if opts.DisableDiskIndex {
		fmt.Fprintln(f, "      default.cluster.disk.index: 'false'")
	}
}

// buildDRString formats a CoreServer list as "NAME1@addr1|NAME2@addr2|..." for globals.dr.
func buildDRString(servers []CoreServer) string {
	parts := make([]string, len(servers))
	for i, s := range servers {
		parts[i] = s.Name + "@" + s.Address
	}
	return strings.Join(parts, "|")
}

// WriteKOFClusterYAML generates the one tibftlserver YAML that configures every FTL
// Server, however many shards they are divided into. When drOpts.Enabled(), also
// generates <stem>-dr.yaml. The stem is tibftlserver-cluster, or
// tibftlserver_standalone for a single pserver (see clusterYAMLStem).
//
// The layout follows samples/yaml/kof/scaling: globals.core.servers names only the
// first shard, every server carries its own "- realm:" block, and the servers beyond
// core.servers carry their own address in an "- ftl:" block. Shard membership is
// settled in ftlserver.json rather than by which file a server appears in, so
// splitting the servers across files expressed nothing and is not done.
func WriteKOFClusterYAML(cfg *BrokerConfig, outputDir, dataDir string, propsPaths []string, realmPath string, numPservers int, ports PortMap, coreServers []CoreServer, authUsersFile, kafkaUsersFile string, drOpts DROpts, copts ClusterOpts) error {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	host := resolveHost(cfg.KOFHost)

	// globals.core.servers holds the first shard only; see buildCoreServers.
	cores := buildCoreServers(coreServers, host, ports)

	stem := clusterYAMLStem(numPservers)
	primaryPath := filepath.Join(outputDir, stem+".yaml")
	if err := writePrimaryYAML(primaryPath, cfg, dataDir, propsPaths, realmPath, numPservers, ports, cores, authUsersFile, kafkaUsersFile, drOpts, copts); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "Writing file: %s\n", primaryPath)

	if drOpts.Enabled() {
		drPath := filepath.Join(outputDir, stem+"-dr.yaml")
		if err := writeDRYAML(drPath, cfg, drOpts.DRDataDir, propsPaths, realmPath, numPservers, ports, drOpts.DRServers, cores, copts); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "Writing file: %s\n", drPath)
	}
	return nil
}

// realmDataDir is the per-server directory holding the realm service's own state.
// Each server needs its own -- the scaling sample gives every server a realm block,
// and two realm services cannot share a directory on one host. It is keyed on the
// server's position rather than its name so that -core-servers cannot change it.
func realmDataDir(dataDir string, i int) string {
	return fmt.Sprintf("%s/srv%d", dataDir, i+1)
}

func writePrimaryYAML(path string, cfg *BrokerConfig, dataDir string, propsPaths []string, realmPath string, numPservers int, ports PortMap, cores []CoreServer, authUsersFile, kafkaUsersFile string, drOpts DROpts, copts ClusterOpts) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	host := resolveHost(cfg.KOFHost)

	fmt.Fprintln(f, "# FSK Cluster Configuration")
	fmt.Fprintf(f, "# Generated by tibftlimportconfig from: %s\n", cfg.SourceFile)
	fmt.Fprintln(f, "#")
	fmt.Fprintln(f, "# Start the cluster (one tibftlserver per server entry):")
	for i := 0; i < numPservers; i++ {
		fmt.Fprintf(f, "#   tibftlserver -c %s -n %s\n", filepath.Base(path), primaryServerName(cores, i))
	}
	fmt.Fprintln(f)

	fmt.Fprintln(f, "globals:")
	if numPservers > len(cores) {
		fmt.Fprintln(f, "  # Bootstrap addresses for the FTL backend: the first shard only. The")
		fmt.Fprintln(f, "  # remaining servers carry their own address in an 'ftl:' block below.")
	}
	fmt.Fprintln(f, "  core.servers:")
	for _, c := range cores {
		fmt.Fprintf(f, "    %s: %s\n", c.Name, c.Address)
	}
	// Auto-provisioned FTL server basic auth: present iff a Kafka listener is secured. The realm
	// authenticates the FTL servers against this tool-generated users file. This is the FTL
	// servers' own login, separate from the Kafka listener security. The Kafka client users
	// (the inline JAAS user_X entries, materialized as kafka-users.txt) ride the same
	// provider list as a second file: provider.
	if authUsersFile != "" {
		fmt.Fprintf(f, "  auth.providers: file:%s", authUsersFile)
		if kafkaUsersFile != "" {
			fmt.Fprintf(f, ",file:%s", kafkaUsersFile)
		}
		fmt.Fprintln(f)
	}
	if drOpts.Enabled() {
		fmt.Fprintf(f, "  dr: %s\n", buildDRString(drOpts.DRServers))
		fmt.Fprintln(f, "  auto.init.primary.on.first.startup: true")
	}
	fmt.Fprintln(f)

	fmt.Fprintln(f, "servers:")
	for i := 0; i < numPservers; i++ {
		name := primaryServerName(cores, i)
		fmt.Fprintf(f, "  %s:\n", name)
		// Servers named in globals.core.servers inherit their listen address from it.
		// The rest -- the shards after the first -- have to state their own.
		if i >= len(cores) {
			fmt.Fprintln(f, "  - ftl:")
			fmt.Fprintf(f, "      server: %s:%d\n", host, ports.PserverPorts[i])
		}
		label := ""
		if drOpts.Enabled() {
			label = "PRIMARY_SERVER"
		}
		writeRealmBlock(f, realmDataDir(dataDir, i), realmPath, label, copts)
		fmt.Fprintln(f, "  - ftlserver.properties:")
		// Each server's login (ftl-internal role) for connecting to the other FTL servers, when
		// the FTL servers require authentication.
		if authUsersFile != "" {
			fmt.Fprintf(f, "      user: %s\n", FTLInternalUser)
			fmt.Fprintf(f, "      password: %s\n", FTLInternalPassword)
		}
		writeServerLogging(f, name, dataDir)
		fmt.Fprintln(f, "  - persistence:")
		fmt.Fprintf(f, "      name: pserver%d\n", i+1)
		fmt.Fprintf(f, "      data: %s/pserver%d\n", dataDir, i+1)
		fmt.Fprintf(f, "      kof.broker.properties: %s\n", propsPaths[i%len(propsPaths)])
		fmt.Fprintf(f, "      loglevel: %s\n", copts.LogLevel)
		// The schema daemon is its own small cluster and does not scale with the
		// pservers: it stays on the first 3 servers however many shards there are.
		if copts.Tibschemad && i < min3(numPservers) {
			writeTibschemadBlock(f, i+1, min3(numPservers))
		}
		fmt.Fprintln(f)
	}
	return nil
}

// writeDRYAML writes the DR replica cluster's YAML (<stem>-dr.yaml), covering every
// DR server in one file exactly as the primary YAML does.
// drServers is the list of DR server names/addresses from -dr-servers; primaryCores is
// the primary core.servers list (used as the back-reference in globals.dr).
//
// -dr-servers is normally as long as the pserver count. When it is shorter -- one shard's
// worth of DR servers for a multi-shard primary -- the extra replicas are named DRSRVn and
// carry their own address, the same way the primary YAML handles the servers that
// globals.core.servers does not name. Generating the names keeps the servers: keys unique,
// which YAML requires. Their addresses are the primary's, which only works if the DR
// cluster is on other hosts; list all of them in -dr-servers to set the addresses.
func writeDRYAML(path string, cfg *BrokerConfig, drDataDir string, propsPaths []string, realmPath string, numPservers int, ports PortMap, drServers, primaryCores []CoreServer, copts ClusterOpts) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	host := resolveHost(cfg.KOFHost)

	fmt.Fprintln(f, "# FSK Cluster — DR Configuration")
	fmt.Fprintf(f, "# Generated by tibftlimportconfig from: %s\n", cfg.SourceFile)
	fmt.Fprintln(f)

	fmt.Fprintln(f, "globals:")
	fmt.Fprintln(f, "  core.servers:")
	for _, s := range drServers {
		fmt.Fprintf(f, "    %s: %s\n", s.Name, s.Address)
	}
	fmt.Fprintf(f, "  dr: %s\n", buildDRString(primaryCores))
	fmt.Fprintln(f)

	fmt.Fprintln(f, "servers:")
	for i := 0; i < numPservers; i++ {
		name := fmt.Sprintf("DRSRV%d", i+1)
		if i < len(drServers) {
			name = drServers[i].Name
		}
		fmt.Fprintf(f, "  %s:\n", name)
		if i >= len(drServers) {
			fmt.Fprintln(f, "  - ftl:")
			fmt.Fprintf(f, "      server: %s:%d\n", host, ports.PserverPorts[i])
		}
		writeRealmBlock(f, realmDataDir(drDataDir, i), realmPath, "DR_SERVER", copts)
		fmt.Fprintln(f, "  - ftlserver.properties:")
		writeServerLogging(f, name, drDataDir)
		fmt.Fprintln(f, "  - persistence:")
		fmt.Fprintf(f, "      name: drpserver%d\n", i+1)
		fmt.Fprintf(f, "      data: %s/drpserver%d\n", drDataDir, i+1)
		fmt.Fprintf(f, "      kof.broker.properties: %s\n", propsPaths[i%len(propsPaths)])
		fmt.Fprintf(f, "      loglevel: %s\n", copts.LogLevel)
		fmt.Fprintln(f)
	}
	return nil
}

// primaryServerName returns the servers: key for the i-th server of the cluster.
//
// The servers named in globals.core.servers carry no `ftl:` block, so tibftlserver derives
// their listen address by matching the name against core.servers. Their keys must
// therefore be the core.servers names -- which -core-servers lets the user choose.
//
// The fallback names the servers past core.servers -- the shards after the first. Those
// state their own address in an `ftl:` block, so any unique key will do.
func primaryServerName(cores []CoreServer, i int) string {
	if i < len(cores) {
		return cores[i].Name
	}
	return fmt.Sprintf("SRV%d", i+1)
}

// buildCoreServers returns the globals.core.servers list.
// If explicit entries are provided via -core-servers, they are used as-is.
// Otherwise, auto-generate names SRV1..SRV3 using the primary realm ports.
func buildCoreServers(explicit []CoreServer, host string, ports PortMap) []CoreServer {
	if len(explicit) > 0 {
		return explicit
	}
	count := 3
	if len(ports.RealmPorts) < count {
		count = len(ports.RealmPorts)
	}
	out := make([]CoreServer, count)
	for i := range out {
		out[i] = CoreServer{
			Name:    fmt.Sprintf("SRV%d", i+1),
			Address: fmt.Sprintf("%s:%d", host, ports.RealmPorts[i]),
		}
	}
	return out
}

func min3(n int) int {
	if n < 3 {
		return n
	}
	return 3
}

func resolveHost(h string) string {
	if h == "" || h == "0.0.0.0" {
		return "localhost"
	}
	return h
}
