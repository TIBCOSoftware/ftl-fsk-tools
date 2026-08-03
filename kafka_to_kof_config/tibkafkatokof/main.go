// tibkafkatokof translates one or more Kafka broker server.properties files into
// the FTL artifacts needed to run a KOF-enabled pserver cluster:
//
//	kof-cluster.yaml          — FTL pserver cluster configuration (primary)
//	kof-cluster-auxN.yaml     — Auxiliary pserver groups (one per additional group of 3 pservers)
//	kof-cluster-secure.yaml   — Secure variant with TLS/auth (when --tls-cert or --oauth-token-url provided)
//	realm.json                — FTL realm configuration with kof.cluster definition
//	kof.broker.N.properties   — Per-pserver broker properties (one file per input, N is 1-based)
//
// Usage:
//
//	tibkafkatokof [flags] <server.properties...>
package main

import (
	"flag"
	"fmt"
	"io"
	rand "math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"tibco.com/ftl-support/tibkafkatokof/translator"
)

func main() {
	// Core flags
	outputDir := flag.String("output-dir", "./kof-output", "output directory for generated files")
	realmName := flag.String("realm-name", "_default_realm", "realm name written into realm.json")
	dataDir := flag.String("data-dir", "/var/tmp/kof/data", "KOF data directory path on pserver hosts")
	ftlLogLevel := flag.String("ftl-loglevel", translator.DefaultFTLLogLevel,
		"loglevel for the generated FTL servers, written into each pserver in the "+
			"cluster YAML (the output servers' logging, NOT this tool's own logging; "+
			"e.g. connections:debug;kof:info;durables:info;store:info)")
	transportType := flag.String("transport-type", "auto",
		"transport type for all pserver connections in realm.json: auto|dtcp\n"+
			"    auto uses OS-selected transport; dtcp is optimized for low latency")
	coreServersFlag := flag.String("core-servers", "",
		"comma-separated NAME=host:port list for globals.core.servers\n"+
			"    e.g. SRV1=host1:5600,SRV2=host2:5601,SRV3=host3:5602\n"+
			"    if omitted, ports are randomly generated in range 5600-5699")

	// TLS flags
	tlsCert := flag.String("tls-cert", "", "server TLS certificate PEM file path")
	tlsKey := flag.String("tls-key", "", "server TLS private key PEM file path")
	tlsKeyPassword := flag.String("tls-key-password", "", "TLS private key passphrase")
	tlsCA := flag.String("tls-ca", "", "CA/trust PEM file path for connecting to other FTL servers")

	// mTLS flags (needed when a Kafka mTLS listener is present)
	tlsServerTrust := flag.String("tls-server-trust", "", "CA PEM to verify inbound client certificates (tls.server.trust.file)")
	tlsClientCert := flag.String("tls-client-cert", "", "client cert PEM for server-to-server connections (tls.client.cert)")
	tlsClientKey := flag.String("tls-client-key", "", "client private key PEM for server-to-server connections (tls.client.private.key)")
	tlsClientKeyPass := flag.String("tls-client-key-password", "", "passphrase for tls-client-key")

	// OAuth2 flags — server-to-server
	oauthTokenURL := flag.String("oauth-token-url", "", "OAuth2 token endpoint URL (server-to-server, oauth2.svr.endpoint.token)")
	oauthJWKSURL := flag.String("oauth-jwks-url", "", "OAuth2 JWKS or validation key (file: path or URL, oauth2.validation.key)")
	oauthClientID := flag.String("oauth-client-id", "", "OAuth2 client ID for server-to-server (oauth2.svr.client.id)")
	oauthClientSecret := flag.String("oauth-client-secret", "", "OAuth2 client secret for server-to-server (oauth2.svr.client.secret)")
	oauthProviderTrust := flag.String("oauth-provider-trust", "", "OAuth2 provider trust PEM file (oauth2.provider.trust.file)")

	// OAuth2 flags — claim/audience (globals)
	oauthClaimRoles := flag.String("oauth-claim-roles", "group", "OAuth2 claim mapped to FTL roles (oauth2.claim.roles)")
	oauthClaimUsername := flag.String("oauth-claim-username", "preferred_username", "OAuth2 claim mapped to FTL user (oauth2.claim.username)")
	oauthAudience := flag.String("oauth-audience", "ftl", "OAuth2 audience value (oauth2.audience)")

	// OAuth2 flags — UI endpoints (globals)
	oauthUIAuthURL := flag.String("oauth-ui-auth-url", "", "OAuth2 auth endpoint for UI (oauth2.ui.endpoint.auth)")
	oauthUITokenURL := flag.String("oauth-ui-token-url", "", "OAuth2 token endpoint for UI (oauth2.ui.endpoint.token)")
	oauthUILogoutURL := flag.String("oauth-ui-logout-url", "", "OAuth2 logout endpoint for UI (oauth2.ui.endpoint.logout)")

	// OAuth2 flags — UI client credentials (per-server ftlserver.properties)
	oauthUIClientID := flag.String("oauth-ui-client-id", "", "OAuth2 client ID for UI authorization code flow (oauth2.ui.client.id)")
	oauthUIClientSecret := flag.String("oauth-ui-client-secret", "", "OAuth2 client secret for UI (oauth2.ui.client.secret)")

	// Shared auth flags
	authRolemap := flag.String("auth-rolemap", "", "path to FTL role map file (auth.rolemap in ftlserver.properties for oauth2)")
	realmServiceUser := flag.String("realm-service-user", "primary", "services.realm.user credential for oauth2 mode")
	realmServicePassword := flag.String("realm-service-password", "primary-pw", "services.realm.password credential for oauth2 mode")
	serverUser := flag.String("server-user", "internal", "user in ftlserver.properties for server-to-server connections (non-oauth2 modes)")
	serverPassword := flag.String("server-password", "internal-pw", "password in ftlserver.properties for server-to-server connections (non-oauth2 modes)")

	// Basic auth flag
	authUsersFile := flag.String("auth-users-file", "", "path to FTL users.txt for file-based authentication")

	// Documentation flag: print the line-by-line listener/security property account and exit.
	listProps := flag.Bool("list-properties", false, "print how each Kafka listener/security property is treated, then exit")
	colorMode := flag.String("color", "auto", "colorize --list-properties output: auto|always|never")
	autoMode := flag.Bool("auto", false, "run the mechanical conversions automatically (JKS/PKCS12 keystores -> PEM via keytool/openssl); items needing a human stay RESOLVE-REQUIRED")

	// DR flags
	drServersFlag := flag.String("dr-servers", "",
		"comma-separated DRSRV1=host:port,DRSRV2=host:port list for DR servers\n"+
			"    if provided, DR mode is enabled for all generated files")
	drDataDirFlag := flag.String("dr-data-dir", "",
		"data directory for DR pservers (default: <data-dir>/dr)")

	writeMigrationConfig := flag.Bool("migration-config", false,
		"write kafka-to-kof.properties to the output directory (migration tool configuration)")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: tibkafkatokof [flags] <server.properties...>")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Translates one or more Kafka broker server.properties into FTL KOF artifacts.")
		fmt.Fprintln(os.Stderr, "Pass one file per broker (1-9 files); pserver count is derived from the file count.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Output files:")
		fmt.Fprintln(os.Stderr, "  kof-cluster.yaml          FTL pserver cluster configuration (primary, first 3 pservers)")
		fmt.Fprintln(os.Stderr, "  kof-cluster-auxN.yaml     Additional pserver groups (one per group of 3 pservers beyond the first)")
		fmt.Fprintln(os.Stderr, "  kof-cluster-secure.yaml   Secure variant with TLS/auth settings for FTL server")
		fmt.Fprintln(os.Stderr, "  realm.json                FTL realm configuration with kof.cluster")
		fmt.Fprintln(os.Stderr, "  kof.broker.N.properties   Per-pserver broker properties (N is 1-based)")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *transportType != "auto" && *transportType != "dtcp" {
		fmt.Fprintf(os.Stderr, "error: --transport-type must be auto or dtcp (got %q)\n", *transportType)
		os.Exit(1)
	}

	if *listProps {
		translator.WriteSupportList(os.Stdout, useColor(*colorMode))
		return
	}

	args := flag.Args()
	if len(args) < 1 || len(args) > 9 {
		flag.Usage()
		os.Exit(1)
	}

	cfgs := make([]*translator.BrokerConfig, 0, len(args))
	for _, arg := range args {
		cfg, err := translator.ParseBrokerConfig(arg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		cfgs = append(cfgs, cfg)
	}
	numPservers := len(cfgs)

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "error creating output directory:", err)
		os.Exit(1)
	}

	ports := generatePorts(numPservers)
	realmPath := filepath.Join(*outputDir, "realm.json")

	// Build per-pserver properties file paths (1-based index).
	propsPaths := make([]string, numPservers)
	for i := range propsPaths {
		propsPaths[i] = filepath.Join(*outputDir, fmt.Sprintf("kof.broker.%d.properties", i+1))
	}

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
		TLSCert:              *tlsCert,
		TLSKey:               *tlsKey,
		TLSKeyPassword:       *tlsKeyPassword,
		TLSCA:                *tlsCA,
		TLSServerTrust:       *tlsServerTrust,
		TLSClientCert:        *tlsClientCert,
		TLSClientKey:         *tlsClientKey,
		TLSClientKeyPassword: *tlsClientKeyPass,
		OAuthTokenURL:        *oauthTokenURL,
		OAuthJWKSURL:         *oauthJWKSURL,
		OAuthClientID:        *oauthClientID,
		OAuthClientSecret:    *oauthClientSecret,
		OAuthProviderTrust:   *oauthProviderTrust,
		OAuthClaimRoles:      *oauthClaimRoles,
		OAuthClaimUsername:   *oauthClaimUsername,
		OAuthAudience:        *oauthAudience,
		OAuthUIAuthURL:       *oauthUIAuthURL,
		OAuthUITokenURL:      *oauthUITokenURL,
		OAuthUILogoutURL:     *oauthUILogoutURL,
		OAuthUIClientID:      *oauthUIClientID,
		OAuthUIClientSecret:  *oauthUIClientSecret,
		AuthRolemap:          *authRolemap,
		RealmServiceUser:     *realmServiceUser,
		RealmServicePassword: *realmServicePassword,
		ServerUser:           *serverUser,
		ServerPassword:       *serverPassword,
		AuthUsersFile:        *authUsersFile,
	}

	// --auto: run the mechanical conversions (keystores) for each broker config.
	if *autoMode {
		for _, cfg := range cfgs {
			r := translator.AutoResolve(cfg, os.Stderr)
			if r.KeystoresConverted > 0 || r.KeystoresFailed > 0 {
				fmt.Fprintf(os.Stderr, "auto: %d keystore converted, %d failed\n\n",
					r.KeystoresConverted, r.KeystoresFailed)
			}
		}
	}

	// Write per-pserver broker properties files and collect status.
	// Unsupported key=value pairs are deduplicated across all brokers (by key) and
	// written once to unsupported.properties.
	statuses := make([]translator.ConfigStatus, numPservers)
	seenUnsupported := make(map[string]bool)
	var allUnsupported []string
	for i, cfg := range cfgs {
		status, unsupported, err := translator.WriteKOFBrokerProperties(cfg, *outputDir, i+1)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error writing kof.broker.%d.properties: %v\n", i+1, err)
			os.Exit(1)
		}
		statuses[i] = status
		for _, kv := range unsupported {
			key := kv
			if idx := strings.Index(kv, "="); idx >= 0 {
				key = kv[:idx]
			}
			if !seenUnsupported[key] {
				seenUnsupported[key] = true
				allUnsupported = append(allUnsupported, kv)
			}
		}
	}
	if len(allUnsupported) > 0 {
		if err := translator.WriteUnsupportedProperties(*outputDir, allUnsupported); err != nil {
			fmt.Fprintln(os.Stderr, "error writing unsupported.properties:", err)
			os.Exit(1)
		}
	}

	// The cluster and realm artifacts derive from the listener layout, independent
	// of the security resolution in broker.properties, so they are always written --
	// even when broker.properties is still INVALID.
	// Policy: if any Kafka listener is secured, the FTL servers (the realm and the pservers) must
	// require authentication too. Auto-provision FTL basic auth (a users file, self-contained, no
	// operator certs) so the pserver-to-pserver connections are valid; this is separate from the
	// Kafka listener security.
	ftlUsersFile := *authUsersFile // operator-provided FTL users file, if any
	if cfgs[0].IsSecure && ftlUsersFile == "" {
		uf, uerr := translator.WriteFTLServerUsers(*outputDir)
		if uerr != nil {
			fmt.Fprintln(os.Stderr, "error writing ftl-users.txt:", uerr)
			os.Exit(1)
		}
		ftlUsersFile = uf
		fmt.Fprintf(os.Stdout, "Writing file: %s (FTL server basic auth; a Kafka listener is secured)\n", uf)
	}
	// Kafka client users: the inline JAAS user_X entries of the SASL/PLAIN listeners,
	// materialized as a second file: provider. Only relevant when the FTL servers
	// authenticate (ftlUsersFile set), which any secured listener implies.
	kafkaUsersFile := ""
	if ftlUsersFile != "" {
		kuf, kerr := translator.WriteKafkaClientUsers(*outputDir, cfgs)
		if kerr != nil {
			fmt.Fprintln(os.Stderr, "error writing kafka-users.txt:", kerr)
			os.Exit(1)
		}
		if kuf != "" {
			kafkaUsersFile = kuf
			fmt.Fprintf(os.Stdout, "Writing file: %s (Kafka client users from the inline jaas entries)\n", kuf)
		}
	}
	if err := translator.WriteKOFClusterYAML(cfgs[0], *outputDir, *dataDir, propsPaths, realmPath, numPservers, ports, coreServers, ftlUsersFile, kafkaUsersFile, drOpts, *ftlLogLevel); err != nil {
		fmt.Fprintln(os.Stderr, "error writing kof-cluster.yaml:", err)
		os.Exit(1)
	}
	if translator.ShouldWriteSecure(cfgs[0], secureOpts) {
		if err := translator.WriteKOFSecureYAML(cfgs[0], *outputDir, *dataDir, propsPaths, realmPath, numPservers, ports, coreServers, secureOpts, drOpts, *ftlLogLevel); err != nil {
			fmt.Fprintln(os.Stderr, "error writing kof-cluster-secure.yaml:", err)
			os.Exit(1)
		}
	}
	if err := translator.WriteRealmJSON(cfgs[0], *outputDir, *realmName, numPservers, drOpts, *transportType); err != nil {
		fmt.Fprintln(os.Stderr, "error writing realm.json:", err)
		os.Exit(1)
	}
	if *writeMigrationConfig {
		if err := translator.WriteMigrationConfig(cfgs, *outputDir); err != nil {
			fmt.Fprintln(os.Stderr, "error writing kafka-to-kof.properties:", err)
			os.Exit(1)
		}
	}

	anyInvalid := false
	for i, status := range statuses {
		if status == translator.StatusInvalid {
			anyInvalid = true
			printResolveSummary(os.Stderr, translator.Summarize(cfgs[i]), *outputDir, *autoMode, i+1)
		}
	}
	if anyInvalid {
		os.Exit(2)
	}
	fmt.Fprintln(os.Stdout, "\nAll kof.broker.*.properties files are processed successfully.")
}

// printResolveSummary prints, after an INVALID run, a numbered list of the
// unresolved settings -- each as "line N: key = value" with what's wrong and the
// fix -- then points --auto at the lines it can fix and lists the lines that need a
// human. The operator runs --auto and/or edits the >>>>>>> blocks, then re-runs.
func printResolveSummary(w io.Writer, s translator.ResolveSummary, outputDir string, autoRan bool, n int) {
	brokerPath := filepath.Join(outputDir, fmt.Sprintf("kof.broker.%d.properties", n))
	fmt.Fprintf(w, "\nINVALID -- %d setting(s) to fix in %s:\n", s.Total(), brokerPath)

	var autoLines, youLines []int
	for i, it := range s.Items {
		val := it.Value
		if it.Note != "" {
			val += "   (file: " + it.Note + ")"
		}
		what, fix, autoFixable := resolveExplain(it.Kind, autoRan)
		fmt.Fprintf(w, "\n  %d. line %d:  %s = %s\n", i+1, it.Line, it.Key, val)
		fmt.Fprintf(w, "        %s\n", what)
		fmt.Fprintf(w, "        %s\n", fix)
		if autoFixable && !autoRan {
			autoLines = append(autoLines, it.Line)
		} else {
			youLines = append(youLines, it.Line)
		}
	}

	fmt.Fprintln(w)
	if len(autoLines) > 0 {
		fmt.Fprintf(w, "Run with --auto to convert line(s) %s for you (keystore -> PEM).\n", joinInts(autoLines))
	}
	if len(youLines) > 0 {
		fmt.Fprintf(w, "Line(s) needing you: %s -- edit the >>>>>>> block in the file.\n", joinInts(youLines))
	}
	fmt.Fprintf(w, "Then re-run the same command:\n  tibkafkatokof -output-dir %s %s\n", outputDir, brokerPath)
}

// resolveExplain returns, for a resolve kind, a one-line "what's wrong", a one-line
// "fix", and whether --auto can do it. autoRan tweaks the keystore wording (--auto
// already tried but the .jks wasn't on this host).
func resolveExplain(kind translator.ResolveKind, autoRan bool) (what, fix string, autoFixable bool) {
	switch kind {
	case translator.KindKeystore:
		what = "a Java keystore (JKS/PKCS12); KoF reads PEM only."
		if autoRan {
			return what, "Fix: --auto couldn't here (file not on this host). Run --auto where the .jks is, or use the commands in the block.", true
		}
		return what, "Fix: run with --auto to convert it, or run the keytool/openssl commands in the block.", true
	case translator.KindHandler:
		return "a custom Java callback class KoF can't run.",
			"Fix: in the block, set a backend (oauth/file/inline) and fill its params.", false
	case translator.KindBackendParams:
		return "a backend is selected but its params are missing.",
			"Fix: in the block, fill the params (oauth: jwks url + issuer + audience; inline: jaas users).", false
	case translator.KindMechanism:
		return "a SASL mechanism KoF can't serve (it serves PLAIN and OAUTHBEARER only).",
			"Fix: in the block, switch this listener to PLAIN or OAUTHBEARER.", false
	case translator.KindAuthorizer:
		return "a custom authorizer; KoF supports the standard one.",
			"Fix: in the block, set the value to: standard.", false
	}
	return "", "", false
}

// joinInts formats line numbers as "45, 49".
func joinInts(xs []int) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += ", "
		}
		out += strconv.Itoa(x)
	}
	return out
}

// useColor decides whether to colorize the --list-properties output. "always" and
// "never" force it; "auto" (the default) colors only when stdout is a terminal and
// NO_COLOR is not set, so piped or redirected output stays plain.
func useColor(mode string) bool {
	switch mode {
	case "always":
		return true
	case "never":
		return false
	default:
		if os.Getenv("NO_COLOR") != "" {
			return false
		}
		fi, err := os.Stdout.Stat()
		if err != nil {
			return false
		}
		return fi.Mode()&os.ModeCharDevice != 0
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
