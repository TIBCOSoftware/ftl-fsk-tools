// tibkafkatokof translates a Kafka broker server.properties into the FTL
// artifacts needed to run a KOF-enabled pserver cluster:
//
//	kof-cluster.yaml          — FTL pserver cluster configuration (primary)
//	kof-cluster-auxN.yaml     — Auxiliary pserver groups (when --num-pservers > 3)
//	kof-cluster-secure.yaml   — Secure variant with TLS/auth (when --tls-cert or --oauth-token-url provided)
//	realm.json                — FTL realm configuration with kof.cluster definition
//	kof.broker.properties     — Flat key=value broker properties (same format as server.properties)
//
// Usage:
//
//	tibkafkatokof [flags] <server.properties>
package main

import (
	"flag"
	"fmt"
	rand "math/rand/v2"
	"os"
	"path/filepath"
	"strings"

	"tibco.com/ftl-support/tibkafkatokof/translator"
)

func main() {
	// Core flags
	outputDir   := flag.String("output-dir",   "./kof-output",    "output directory for generated files")
	realmName   := flag.String("realm-name",   "_default_realm",  "realm name written into realm.json")
	dataDir     := flag.String("data-dir",     "/var/kof/data",   "KOF data directory path on pserver hosts")
	numPservers := flag.Int("num-pservers",    3,                  "number of pservers to generate (must be a positive odd number)")
	coreServersFlag := flag.String("core-servers", "",
		"comma-separated NAME=host:port list for globals.core.servers\n"+
			"    e.g. SRV1=host1:5600,SRV2=host2:5601,SRV3=host3:5602\n"+
			"    if omitted, ports are randomly generated in range 5600-5699")

	// TLS flags
	tlsCert        := flag.String("tls-cert",         "", "server TLS certificate PEM file path")
	tlsKey         := flag.String("tls-key",          "", "server TLS private key PEM file path")
	tlsKeyPassword := flag.String("tls-key-password", "", "TLS private key passphrase")
	tlsCA          := flag.String("tls-ca",           "", "CA/trust PEM file path for connecting to other FTL servers")

	// mTLS flags (needed when a Kafka mTLS listener is present)
	tlsServerTrust    := flag.String("tls-server-trust",        "", "CA PEM to verify inbound client certificates (tls.server.trust.file)")
	tlsClientCert     := flag.String("tls-client-cert",         "", "client cert PEM for server-to-server connections (tls.client.cert)")
	tlsClientKey      := flag.String("tls-client-key",          "", "client private key PEM for server-to-server connections (tls.client.private.key)")
	tlsClientKeyPass  := flag.String("tls-client-key-password", "", "passphrase for tls-client-key")

	// OAuth2 flags
	oauthTokenURL      := flag.String("oauth-token-url",      "", "OAuth2 token endpoint URL (server-to-server)")
	oauthJWKSURL       := flag.String("oauth-jwks-url",       "", "OAuth2 JWKS or validation key (file: path or URL)")
	oauthClientID      := flag.String("oauth-client-id",      "", "OAuth2 client ID")
	oauthClientSecret  := flag.String("oauth-client-secret",  "", "OAuth2 client secret")
	oauthProviderTrust := flag.String("oauth-provider-trust", "", "OAuth2 provider trust PEM file")

	// Basic auth flag
	authUsersFile := flag.String("auth-users-file", "", "path to FTL users.txt for file-based authentication")

	// DR flags
	drServersFlag := flag.String("dr-servers", "",
		"comma-separated DRSRV1=host:port,DRSRV2=host:port list for DR servers\n"+
			"    if provided, DR mode is enabled for all generated files")
	drDataDirFlag := flag.String("dr-data-dir", "",
		"data directory for DR pservers (default: <data-dir>/dr)")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: tibkafkatokof [flags] <server.properties>")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Translates a Kafka broker server.properties into FTL KOF artifacts.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Output files:")
		fmt.Fprintln(os.Stderr, "  kof-cluster.yaml          FTL pserver cluster configuration (primary, first 3 pservers)")
		fmt.Fprintln(os.Stderr, "  kof-cluster-auxN.yaml     Additional pserver groups (when --num-pservers > 3)")
		fmt.Fprintln(os.Stderr, "  kof-cluster-secure.yaml   Secure variant with TLS/auth settings for FTL server")
		fmt.Fprintln(os.Stderr, "  realm.json                FTL realm configuration with kof.cluster")
		fmt.Fprintln(os.Stderr, "  kof.broker.properties     Flat key=value broker properties")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		flag.Usage()
		os.Exit(1)
	}
	if *numPservers < 1 || *numPservers%2 == 0 {
		fmt.Fprintln(os.Stderr, "error: --num-pservers must be a positive odd number (e.g. 3, 5, 7)")
		os.Exit(1)
	}

	cfg, err := translator.ParseBrokerConfig(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "error creating output directory:", err)
		os.Exit(1)
	}

	ports := generatePorts(*numPservers)
	propsPath := filepath.Join(*outputDir, "kof.broker.properties")

	// Parse --core-servers flag into CoreServer slice.
	coreServers := parseCoreServers(*coreServersFlag)

	// Build DROpts from flags.
	drDataDirResolved := *drDataDirFlag
	if drDataDirResolved == "" {
		drDataDirResolved = *dataDir + "/dr"
	}
	drOpts := translator.DROpts{
		DRServers: parseCoreServers(*drServersFlag),
		DRDataDir: drDataDirResolved,
	}

	// Build SecureOpts from flags.
	secureOpts := translator.SecureOpts{
		TLSCert:             *tlsCert,
		TLSKey:              *tlsKey,
		TLSKeyPassword:      *tlsKeyPassword,
		TLSCA:               *tlsCA,
		TLSServerTrust:      *tlsServerTrust,
		TLSClientCert:       *tlsClientCert,
		TLSClientKey:        *tlsClientKey,
		TLSClientKeyPassword: *tlsClientKeyPass,
		OAuthTokenURL:       *oauthTokenURL,
		OAuthJWKSURL:        *oauthJWKSURL,
		OAuthClientID:       *oauthClientID,
		OAuthClientSecret:   *oauthClientSecret,
		OAuthProviderTrust:  *oauthProviderTrust,
		AuthUsersFile:       *authUsersFile,
	}

	if err := translator.WriteKOFClusterYAML(cfg, *outputDir, *dataDir, propsPath, *numPservers, ports, coreServers, drOpts); err != nil {
		fmt.Fprintln(os.Stderr, "error writing kof-cluster.yaml:", err)
		os.Exit(1)
	}

	if translator.ShouldWriteSecure(cfg, secureOpts) {
		if err := translator.WriteKOFSecureYAML(cfg, *outputDir, *dataDir, propsPath, *numPservers, ports, coreServers, secureOpts, drOpts); err != nil {
			fmt.Fprintln(os.Stderr, "error writing kof-cluster-secure.yaml:", err)
			os.Exit(1)
		}
	}

	if err := translator.WriteRealmJSON(cfg, *outputDir, *realmName, *numPservers, drOpts); err != nil {
		fmt.Fprintln(os.Stderr, "error writing realm.json:", err)
		os.Exit(1)
	}
	fmt.Println("wrote", filepath.Join(*outputDir, "realm.json"))

	if err := translator.WriteKOFBrokerProperties(cfg, *outputDir, *dataDir, 1); err != nil {
		fmt.Fprintln(os.Stderr, "error writing kof.broker.properties:", err)
		os.Exit(1)
	}
}

// parseCoreServers parses "SRV1=host:5600,SRV2=host:5601" into a CoreServer slice.
// Returns nil (auto-generate) if the flag is empty.
func parseCoreServers(s string) []translator.CoreServer {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	tokens := strings.Split(s, ",")
	out := make([]translator.CoreServer, 0, len(tokens))
	for _, t := range tokens {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		idx := strings.Index(t, "=")
		if idx < 1 {
			// Treat bare "host:port" as SRVn
			out = append(out, translator.CoreServer{
				Name:    fmt.Sprintf("SRV%d", len(out)+1),
				Address: t,
			})
		} else {
			out = append(out, translator.CoreServer{
				Name:    t[:idx],
				Address: t[idx+1:],
			})
		}
	}
	return out
}

// generatePorts picks n random FTL ports in two non-overlapping ranges:
//   - realm server ports: 5600–5699
//   - pserver connection ports: 5700–5799
func generatePorts(n int) translator.PortMap {
	pm := translator.PortMap{
		RealmPorts:   make([]int, n),
		PserverPorts: make([]int, n),
	}
	used := map[int]bool{}
	pick := func(lo, hi int) int {
		for {
			p := rand.IntN(hi-lo) + lo
			if !used[p] {
				used[p] = true
				return p
			}
		}
	}
	for i := range pm.RealmPorts {
		pm.RealmPorts[i] = pick(5600, 5700)
	}
	for i := range pm.PserverPorts {
		pm.PserverPorts[i] = pick(5700, 5800)
	}
	return pm
}
