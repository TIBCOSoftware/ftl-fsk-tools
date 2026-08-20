package translator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const minimalBroker = `
process.roles=broker,controller
node.id=1
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=/var/kafka/data/broker-1
`

// writeCluster generates a primary cluster YAML for numPservers pservers using the
// supplied core server names, and returns its contents.
func writeCluster(t *testing.T, numPservers int, cores []CoreServer) string {
	t.Helper()
	cfg := makeCfg(t, minimalBroker)
	out := t.TempDir()

	ports := PortMap{}
	props := make([]string, numPservers)
	for i := 0; i < numPservers; i++ {
		ports.RealmPorts = append(ports.RealmPorts, 5600+i)
		ports.PserverPorts = append(ports.PserverPorts, 5700+i)
		props[i] = "kof.broker.properties"
	}

	err := WriteKOFClusterYAML(cfg, out, "/var/tmp/kof/data", props, "realm.json",
		numPservers, ports, cores, "", "", DROpts{}, ClusterOpts{LogLevel: "kof:info"})
	if err != nil {
		t.Fatalf("WriteKOFClusterYAML: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(out, clusterYAMLStem(min3(numPservers))+".yaml"))
	if err != nil {
		t.Fatalf("read generated cluster yaml: %v", err)
	}
	return string(b)
}

// The servers: keys must be the -core-servers names. Servers in the primary cluster carry
// no `ftl:` block, so tibftlserver resolves each one's listen address by matching -n
// against globals.core.servers; if the two lists disagree the server gets no address and
// falls back to a hardcoded default. Regression test for the case where -core-servers
// supplies names other than SRV1..N.
func TestPrimaryYAML_ServerKeysFollowCoreServerNames(t *testing.T) {
	cores := []CoreServer{
		{Name: "primary1", Address: "primary-host-1:8585"},
		{Name: "primary2", Address: "primary-host-2:8686"},
		{Name: "primary3", Address: "primary-host-3:8787"},
	}
	got := writeCluster(t, 3, cores)

	for _, c := range cores {
		if !strings.Contains(got, "\n  "+c.Name+":\n") {
			t.Errorf("servers: block is missing key %q\n%s", c.Name, got)
		}
		if !strings.Contains(got, "-n "+c.Name+"\n") {
			t.Errorf("start-up comment does not mention -n %s\n%s", c.Name, got)
		}
	}
	if strings.Contains(got, "\n  SRV1:\n") {
		t.Errorf("servers: block still uses the hardcoded SRV1 key\n%s", got)
	}

	// Every servers: key must also appear as a core.servers name, or it has no address.
	_, serversSection, found := strings.Cut(got, "\nservers:\n")
	if !found {
		t.Fatalf("no servers: section\n%s", got)
	}
	for _, line := range strings.Split(serversSection, "\n") {
		name := strings.TrimSuffix(strings.TrimPrefix(line, "  "), ":")
		// Server keys only: skip blanks, the "- service:" sequence entries under each
		// server, and anything more deeply indented.
		if line != "  "+name+":" || name == "" || strings.HasPrefix(name, "- ") {
			continue
		}
		if !strings.Contains(got, "    "+name+": ") {
			t.Errorf("server %q has no globals.core.servers entry\n%s", name, got)
		}
	}
}

// With no -core-servers the names fall back to SRV1..N, matching buildCoreServers.
func TestPrimaryYAML_DefaultServerNames(t *testing.T) {
	got := writeCluster(t, 3, nil)
	for _, name := range []string{"SRV1", "SRV2", "SRV3"} {
		if !strings.Contains(got, "\n  "+name+":\n") {
			t.Errorf("servers: block is missing default key %q\n%s", name, got)
		}
	}
}

// Fewer core servers than pservers must not panic; the extras fall back to SRV<n>.
func TestPrimaryYAML_FewerCoreServersThanPservers(t *testing.T) {
	got := writeCluster(t, 3, []CoreServer{{Name: "only1", Address: "host-1:8585"}})
	if !strings.Contains(got, "\n  only1:\n") {
		t.Errorf("servers: block is missing key %q\n%s", "only1", got)
	}
	for _, name := range []string{"SRV2", "SRV3"} {
		if !strings.Contains(got, "\n  "+name+":\n") {
			t.Errorf("servers: block is missing fallback key %q\n%s", name, got)
		}
	}
}
