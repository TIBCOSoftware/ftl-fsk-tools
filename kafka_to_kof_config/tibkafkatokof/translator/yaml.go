package translator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PortMap holds the randomly generated FTL ports for a cluster of N pservers.
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

	// Tibschemad adds the FTL schema daemon to every server that carries a realm
	// block: a second "- persistence: name: schemaN" plus a "- tibschemad:" entry.
	// No extra FTL servers and no extra ports -- the schema pserver shares the
	// tibftlserver process that already hosts the KOF pserver. Set by --tibschemad.
	Tibschemad bool
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

// writeRealmBlock writes a server's "- realm:" entry. Every generated YAML carries
// the realm settings per server rather than in a shared services section, so there
// is no services block in any file this tool writes.
//
// label is "" for a non-DR realm. realmPath is "" for realms that do not seed the
// realm configuration (currently none, but aux files have no realm block at all).
// extra holds already-formatted "key: value" entries (the secure YAML's realm
// credentials) written between the label and the data path.
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

// WriteKOFClusterYAML generates the primary tibftlserver YAML and, when numPservers > 3,
// one or more <stem>-auxN.yaml files for additional pserver groups.
// When drOpts.Enabled(), also generates <stem>-dr.yaml (and aux DR files).
// The stem is tibftlserver-cluster, or tibftlserver_standalone for a single pserver
// (see clusterYAMLStem).
func WriteKOFClusterYAML(cfg *BrokerConfig, outputDir, dataDir string, propsPaths []string, realmPath string, numPservers int, ports PortMap, coreServers []CoreServer, authUsersFile, kafkaUsersFile string, drOpts DROpts, copts ClusterOpts) error {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	host := resolveHost(cfg.KOFHost)

	// Build the globals.core.servers list (shared by primary and all aux files).
	cores := buildCoreServers(coreServers, host, ports)

	// Primary cluster: first 3 pservers with realm servers.
	primaryCount := min3(numPservers)
	stem := clusterYAMLStem(primaryCount)
	primaryPath := filepath.Join(outputDir, stem+".yaml")
	if err := writePrimaryYAML(primaryPath, cfg, dataDir, propsPaths, realmPath, primaryCount, ports, cores, authUsersFile, kafkaUsersFile, drOpts, copts); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "Writing file: %s\n", primaryPath)

	if drOpts.Enabled() {
		drPath := filepath.Join(outputDir, stem+"-dr.yaml")
		if err := writeDRYAML(drPath, cfg, drOpts.DRDataDir, propsPaths, realmPath, 0, primaryCount, drOpts.DRServers, cores, copts); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "Writing file: %s\n", drPath)
	}

	// Auxiliary clusters: groups of up to 3 pservers, no realm sections.
	auxIdx := 1
	for start := primaryCount; start < numPservers; start += 3 {
		end := start + 3
		if end > numPservers {
			end = numPservers
		}
		auxPath := filepath.Join(outputDir, fmt.Sprintf("%s-aux%d.yaml", stem, auxIdx))
		if err := writeAuxYAML(auxPath, cfg, dataDir, propsPaths, start, end, ports, cores, drOpts, copts); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "Writing file: %s\n", auxPath)

		if drOpts.Enabled() {
			drAuxPath := filepath.Join(outputDir, fmt.Sprintf("%s-dr-aux%d.yaml", stem, auxIdx))
			if err := writeDRYAML(drAuxPath, cfg, drOpts.DRDataDir, propsPaths, realmPath, start, end, drOpts.DRServers, cores, copts); err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "Writing file: %s\n", drAuxPath)
		}
		auxIdx++
	}
	return nil
}

func writePrimaryYAML(path string, cfg *BrokerConfig, dataDir string, propsPaths []string, realmPath string, numPservers int, _ PortMap, cores []CoreServer, authUsersFile, kafkaUsersFile string, drOpts DROpts, copts ClusterOpts) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	fmt.Fprintln(f, "# KOF Cluster Configuration")
	fmt.Fprintf(f, "# Generated by tibkafkatokof from: %s\n", cfg.SourceFile)
	fmt.Fprintln(f, "#")
	fmt.Fprintln(f, "# Start the cluster (one tibftlserver per server entry):")
	for i := 0; i < numPservers; i++ {
		fmt.Fprintf(f, "#   tibftlserver -c %s -n %s\n", filepath.Base(path), primaryServerName(cores, i))
	}
	fmt.Fprintln(f)

	fmt.Fprintln(f, "globals:")
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
		fmt.Fprintf(f, "  %s:\n", primaryServerName(cores, i))
		label := ""
		if drOpts.Enabled() {
			label = "PRIMARY_SERVER"
		}
		writeRealmBlock(f, dataDir, realmPath, label, copts)
		// Each server's login (ftl-internal role) for connecting to the other FTL servers, when
		// the FTL servers require authentication.
		if authUsersFile != "" {
			fmt.Fprintln(f, "  - ftlserver.properties:")
			fmt.Fprintf(f, "      user: %s\n", FTLInternalUser)
			fmt.Fprintf(f, "      password: %s\n", FTLInternalPassword)
		}
		fmt.Fprintln(f, "  - persistence:")
		fmt.Fprintf(f, "      name: pserver%d\n", i+1)
		fmt.Fprintf(f, "      data: %s/pserver%d\n", dataDir, i+1)
		fmt.Fprintf(f, "      kof.broker.properties: %s\n", propsPaths[i%len(propsPaths)])
		fmt.Fprintf(f, "      loglevel: %s\n", copts.LogLevel)
		if copts.Tibschemad {
			writeTibschemadBlock(f, i+1, numPservers)
		}
		fmt.Fprintln(f)
	}
	return nil
}

func writeAuxYAML(path string, cfg *BrokerConfig, dataDir string, propsPaths []string, start, end int, ports PortMap, cores []CoreServer, drOpts DROpts, copts ClusterOpts) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	host := resolveHost(cfg.KOFHost)
	auxNum := start/3 + 1 // label for comments

	fmt.Fprintf(f, "# KOF Auxiliary Cluster Configuration (group %d)\n", auxNum)
	fmt.Fprintf(f, "# Generated by tibkafkatokof from: %s\n", cfg.SourceFile)
	fmt.Fprintln(f, "#")
	fmt.Fprintln(f, "# Pservers in this file connect to the primary realm cluster.")
	fmt.Fprintf(f, "# Start: tibftlserver -c %s -n PSRV%d\n", filepath.Base(path), start+1)
	fmt.Fprintln(f)

	fmt.Fprintln(f, "globals:")
	fmt.Fprintln(f, "  core.servers:")
	for _, c := range cores {
		fmt.Fprintf(f, "    %s: %s\n", c.Name, c.Address)
	}
	if drOpts.Enabled() {
		fmt.Fprintf(f, "  dr: %s\n", buildDRString(drOpts.DRServers))
		fmt.Fprintln(f, "  auto.init.primary.on.first.startup: true")
	}
	fmt.Fprintln(f)

	fmt.Fprintln(f, "servers:")
	for i := start; i < end; i++ {
		fmt.Fprintf(f, "  PSRV%d:\n", i+1)
		fmt.Fprintln(f, "  - ftl:")
		fmt.Fprintf(f, "      server: %s:%d\n", host, ports.PserverPorts[i])
		fmt.Fprintln(f, "  - persistence:")
		fmt.Fprintf(f, "      name: pserver%d\n", i+1)
		fmt.Fprintf(f, "      data: %s/pserver%d\n", dataDir, i+1)
		fmt.Fprintf(f, "      kof.broker.properties: %s\n", propsPaths[i%len(propsPaths)])
		fmt.Fprintf(f, "      loglevel: %s\n", copts.LogLevel)
		fmt.Fprintln(f)
	}
	// No realm entries in auxiliary files -- these pservers join the primary realm cluster.
	return nil
}

// writeDRYAML writes the primary DR YAML (<stem>-dr.yaml) or an aux DR YAML for the
// DR replica cluster.
// start=0 produces the primary DR YAML with realm entries; start>0 produces an aux DR file.
// drServers is the list of DR server names/addresses; primaryCores is the primary core.servers list
// (used as the back-reference in globals.dr of the DR YAML).
func writeDRYAML(path string, cfg *BrokerConfig, drDataDir string, propsPaths []string, realmPath string, start, end int, drServers, primaryCores []CoreServer, copts ClusterOpts) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	isPrimary := start == 0

	if isPrimary {
		fmt.Fprintln(f, "# KOF Cluster — DR Configuration")
	} else {
		fmt.Fprintf(f, "# KOF Auxiliary Cluster — DR Configuration (group %d)\n", start/3+1)
	}
	fmt.Fprintf(f, "# Generated by tibkafkatokof from: %s\n", cfg.SourceFile)
	fmt.Fprintln(f)

	fmt.Fprintln(f, "globals:")
	fmt.Fprintln(f, "  core.servers:")
	for _, s := range drServers {
		fmt.Fprintf(f, "    %s: %s\n", s.Name, s.Address)
	}
	fmt.Fprintf(f, "  dr: %s\n", buildDRString(primaryCores))
	fmt.Fprintln(f)

	fmt.Fprintln(f, "servers:")
	for i := start; i < end; i++ {
		drSrv := drServers[i%len(drServers)]
		drPserverNum := i + 1
		fmt.Fprintf(f, "  %s:\n", drSrv.Name)
		if isPrimary {
			writeRealmBlock(f, drDataDir, realmPath, "DR_SERVER", copts)
		}
		fmt.Fprintln(f, "  - persistence:")
		fmt.Fprintf(f, "      name: drpserver%d\n", drPserverNum)
		fmt.Fprintf(f, "      data: %s/drpserver%d\n", drDataDir, drPserverNum)
		fmt.Fprintf(f, "      kof.broker.properties: %s\n", propsPaths[i%len(propsPaths)])
		fmt.Fprintf(f, "      loglevel: %s\n", copts.LogLevel)
		fmt.Fprintln(f)
	}
	return nil
}

// primaryServerName returns the servers: key for the i-th server of the primary cluster.
//
// These servers carry no `ftl:` block, so tibftlserver derives their listen address by
// matching the name against globals.core.servers. The keys must therefore be the
// core.servers names -- which -core-servers lets the user choose. (Auxiliary pservers are
// the other case: they are absent from core.servers and instead carry their own `ftl:`
// block, so writeAuxYAML is free to name them PSRVn.)
//
// The fallback only triggers if fewer core servers were supplied than there are servers to
// write, in which case the extra ones have no address to inherit anyway.
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
