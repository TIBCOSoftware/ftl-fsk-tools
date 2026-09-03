/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

// tibftlimportconfig translates one or more Kafka broker server.properties files into
// the FTL artifacts needed to run a FSK-enabled pserver cluster:
//
//	tibftlserver-cluster.yaml         — FTL pserver cluster configuration (every server, however many shards)
//	tibftlserver-cluster-secure.yaml  — Secure variant with TLS/auth (when --tls-cert or --oauth-token-url provided)
//	ftlserver.json                    — FTL realm configuration with kof.cluster definition
//	kof.broker.N.properties           — Per-pserver broker properties (one file per input, N is 1-based)
//
// A single-broker conversion produces one pserver, which is a standalone server
// rather than a cluster, so those YAMLs are named tibftlserver-standalone*.yaml.
//
// Usage:
//
//	tibftlimportconfig [flags] <server.properties...>
package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	rand "math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"tibco.com/ftl-support/tibftlimportconfig/translator"
)

func main() {
	// Core flags
	outputDir := flag.String("output-dir", "./kof-output", "output directory for generated files")
	dataDir := flag.String("data-dir", "/var/tmp/kof/data", "FSK data directory path on pserver hosts")
	ftlLogLevel := flag.String("ftl-loglevel", translator.DefaultFTLLogLevel,
		"loglevel for the generated FTL servers, written into each pserver in the "+
			"cluster YAML (the output servers' logging, NOT this tool's own logging; "+
			"e.g. connections:debug;kof:info;durables:info;store:info)")
	transportType := flag.String("transport-type", "auto",
		"transport type for all pserver connections in ftlserver.json: auto|dtcp\n"+
			"    auto lets the realm server pick the concrete transport for each connection\n"+
			"    at deployment time: dynamic TCP for client and intra-cluster transports,\n"+
			"    static TCP for inter-cluster and DR transports\n"+
			"    dtcp pins every transport to dynamic TCP")
	coreServersFlag := flag.String("core-servers", "",
		"comma-separated NAME=host:port list for globals.core.servers\n"+
			"    e.g. SRV1=host1:5600,SRV2=host2:5601,SRV3=host3:5602\n"+
			"    if omitted, ports are derived from the cluster in range 5600-5699\n"+
			"    (the same brokers always yield the same ports, so re-runs do not churn)")

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
	realmServiceUser := flag.String("realm-service-user", "primary", "realm user credential for oauth2 mode (written into each realm block)")
	realmServicePassword := flag.String("realm-service-password", "primary-pw", "realm password credential for oauth2 mode (written into each realm block)")
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

	diskPersistence := flag.String("disk-persistence", "async",
		"disk persistence for the generated kof.cluster: async, sync, or in-memory\n"+
			"    async      writes are buffered for an eventual flush to disk\n"+
			"    sync       every write is flushed to disk before it is acknowledged\n"+
			"    in-memory  nothing is written to disk; also turns off the cluster's\n"+
			"               disk index and compaction, which require disk persistence\n"+
			"    the data store is always async and the sync and meta stores always sync,\n"+
			"    whichever the cluster is -- except under in-memory, where the stores are\n"+
			"    in-memory too, since an override would otherwise put them back on disk")

	// 0 is the "auto" sentinel: 3, or 1 for a lone broker. Resolved once the input
	// count is known, below. Naming the real default in the text keeps flag's own
	// "(default 0)" out of the help.
	replicationFactor := flag.Int("replication-factor", 0,
		"pservers per FSK shard (kof.cluster.N): 1, 3 or 5 (default 3)\n"+
			"    the number of input server.properties files must be an exact multiple\n"+
			"    of this -- 9 files at 3 gives three shards, 6 gives two, 5 files at 5\n"+
			"    gives one; the FTL realm keeps its own 3 servers either way\n"+
			"    the values are odd because a shard needs a majority quorum, so a\n"+
			"    2-file input is refused; use 1 for one unreplicated shard per broker\n"+
			"    unset, it is 3, or 1 when a single file is given")

	writeMigrationConfig := flag.Bool("migration-config", false,
		"write kafka-to-kof.properties to the output directory (migration tool configuration)")

	writeTibschemad := flag.Bool("tibschemad", false,
		"add a tibschemad (FTL schema daemon) section to the generated cluster YAML\n"+
			"    each server gains a schemaN persistence and a tibschemad entry; no extra servers or ports")

	// Undocumented. Reads the configuration from live Kafka brokers over the Admin
	// API instead of from server.properties files: a comma-separated host:port list,
	// mutually exclusive with the positional arguments. What the Admin API reports is
	// the broker's effective configuration, not the file the operator wrote, so the
	// translation it produces is harder to reason about than the file-based one.
	// Kept working, but deliberately absent from every help topic.
	fromBrokers := flag.String("from-brokers", "", "")
	fromBrokersTimeout := flag.Int("from-brokers-timeout-ms", 10000, "")

	// Undocumented. Turns off the default cluster's disk index: writes
	// "default.cluster.disk.index: 'false'" into every realm block of the generated
	// cluster YAMLs and sets cluster_disk_index_default_value to false in ftlserver.json.
	// tibftlserver enables the index by default whenever disk persistence is sync or
	// async, so both places have to say no. Deliberately absent from every help topic.
	disableDiskIndex := flag.Bool("disable-disk-index", false, "")

	flag.Usage = func() { writeHelp(os.Stderr, "") }
	if topic, ok := helpRequested(os.Args[1:]); ok {
		if !writeHelp(os.Stdout, topic) {
			fmt.Fprintf(os.Stderr, "error: unknown help topic %q; try one of: %s\n", topic, topicList())
			os.Exit(1)
		}
		return
	}
	flag.Parse()

	if *transportType != "auto" && *transportType != "dtcp" {
		fmt.Fprintf(os.Stderr, "error: --transport-type must be auto or dtcp (got %q)\n", *transportType)
		os.Exit(1)
	}

	if _, ok := translator.RealmDiskPersistence(*diskPersistence); !ok {
		fmt.Fprintf(os.Stderr, "error: --disk-persistence must be one of %s (got %q)\n",
			strings.Join(translator.DiskPersistenceModeNames, ", "), *diskPersistence)
		os.Exit(1)
	}

	// The accepted factors are odd because an FSK shard needs a majority quorum.
	// 0 is the unset sentinel and is resolved against the input count below.
	if *replicationFactor != 0 && *replicationFactor != 1 && *replicationFactor != 3 && *replicationFactor != 5 {
		fmt.Fprintf(os.Stderr, "error: --replication-factor must be one of 1, 3, 5 (got %d)\n", *replicationFactor)
		os.Exit(1)
	}

	if *listProps {
		translator.WriteSupportList(os.Stdout, useColor(*colorMode))
		return
	}

	args := flag.Args()
	usingFromBrokers := *fromBrokers != ""

	if usingFromBrokers && len(args) > 0 {
		fmt.Fprintln(os.Stderr, "error: -from-brokers and positional server.properties arguments are mutually exclusive")
		os.Exit(1)
	}

	var cfgs []*translator.BrokerConfig
	if usingFromBrokers {
		addrs := splitCSVTrimmed(*fromBrokers)
		if len(addrs) < 1 || len(addrs) > 9 {
			fmt.Fprintf(os.Stderr, "error: -from-brokers requires 1-9 broker addresses (got %d)\n", len(addrs))
			os.Exit(1)
		}
		cfgs = make([]*translator.BrokerConfig, 0, len(addrs))
		for _, addr := range addrs {
			fmt.Fprintf(os.Stdout, "fetching config from broker %s ...\n", addr)
			cfg, err := translator.FetchBrokerConfig(addr, *fromBrokersTimeout)
			if err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
				os.Exit(1)
			}
			cfgs = append(cfgs, cfg)
		}
	} else {
		if len(args) < 1 || len(args) > 9 {
			flag.Usage()
			os.Exit(1)
		}
		cfgs = make([]*translator.BrokerConfig, 0, len(args))
		for _, arg := range args {
			cfg, err := translator.ParseBrokerConfig(arg)
			if err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
				os.Exit(1)
			}
			cfgs = append(cfgs, cfg)
		}
	}
	numPservers := len(cfgs)

	// Resolve --replication-factor now that the broker count is known. Unset means 3,
	// the FTL quorum, except for a lone broker: that is a standalone server and is
	// explicitly unreplicated. Two brokers get no such exemption -- FTL runs 1, 3 or 5,
	// and two servers have no majority, so losing either one stalls the shard.
	replFactor := *replicationFactor
	if replFactor == 0 {
		replFactor = 3
		if numPservers == 1 {
			replFactor = 1
		}
	}
	if numPservers%replFactor != 0 {
		fmt.Fprintf(os.Stderr,
			"error: --replication-factor %d requires the number of broker files to be a multiple of %d (got %d)\n",
			replFactor, replFactor, numPservers)
		fmt.Fprintln(os.Stderr,
			"       use --replication-factor 1 for one unreplicated shard per broker")
		os.Exit(1)
	}

	// Every pserver needs a distinct, non-negative node.id or it will not start.
	// Whichever way the configs arrived, fill in the ones the source left out.
	translator.EnsureNodeIDs(cfgs)

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "error creating output directory:", err)
		os.Exit(1)
	}

	ports := generatePorts(cfgs)
	realmPath := filepath.Join(*outputDir, translator.RealmFileName)

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

	// Settings shared by every generated cluster YAML and by ftlserver.json.
	clusterOpts := translator.ClusterOpts{
		LogLevel:          *ftlLogLevel,
		DisableDiskIndex:  *disableDiskIndex,
		DiskPersistence:   *diskPersistence,
		Tibschemad:        *writeTibschemad,
		ReplicationFactor: replFactor,
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
			if r.KeystoresConverted > 0 || r.KeystoresFailed > 0 || r.KeystoresSkipped > 0 {
				fmt.Fprintf(os.Stderr, "auto: %d keystore(s) converted, %d failed, %d could not be done here\n\n",
					r.KeystoresConverted, r.KeystoresFailed, r.KeystoresSkipped)
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
	if err := translator.WriteKOFClusterYAML(cfgs[0], *outputDir, *dataDir, propsPaths, realmPath, numPservers, ports, coreServers, ftlUsersFile, kafkaUsersFile, drOpts, clusterOpts); err != nil {
		fmt.Fprintln(os.Stderr, "error writing cluster YAML:", err)
		os.Exit(1)
	}
	if translator.ShouldWriteSecure(cfgs[0], secureOpts) {
		// Wire the auto-generated FTL + Kafka users into the secure YAML's
		// auth.providers, same as the base cluster YAML. Without this the secure
		// YAML would be TLS-without-auth, which the FTL server rejects at startup.
		if secureOpts.AuthUsersFile == "" {
			secureOpts.AuthUsersFile = ftlUsersFile
		}
		if secureOpts.KafkaUsersFile == "" {
			secureOpts.KafkaUsersFile = kafkaUsersFile
		}
		if err := translator.WriteKOFSecureYAML(cfgs[0], *outputDir, *dataDir, propsPaths, realmPath, numPservers, ports, coreServers, secureOpts, drOpts, clusterOpts); err != nil {
			fmt.Fprintln(os.Stderr, "error writing secure cluster YAML:", err)
			os.Exit(1)
		}
	}
	if err := translator.WriteRealmJSON(cfgs[0], *outputDir, numPservers, drOpts, *transportType, clusterOpts); err != nil {
		fmt.Fprintln(os.Stderr, "error writing "+translator.RealmFileName+":", err)
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
	// A keystore rewritten to PEM is settled as far as the config goes, so it does
	// not make the file INVALID -- but the .pem does not exist until someone makes
	// it, and a config pointing at a missing file fails at startup. Say so, loudly.
	anyPendingKeystore := false
	for i, cfg := range cfgs {
		if len(translator.PendingKeystores(cfg)) > 0 {
			anyPendingKeystore = true
		}
		printKeystoreWarning(os.Stderr, cfg, *outputDir, *autoMode, i+1)
	}
	if anyInvalid {
		os.Exit(2)
	}
	// Don't follow a SEVERE WARNING with "processed successfully" -- the translation
	// did succeed, but the output is not runnable until the .pem files exist, and
	// only one of those two facts should be the last thing on screen.
	if anyPendingKeystore {
		fmt.Fprintln(os.Stdout, "\nAll kof.broker.*.properties files were translated, but the keystore"+
			"\nconversion(s) above must be completed before FSK will start.")
		return
	}
	fmt.Fprintln(os.Stdout, "\nAll kof.broker.*.properties files are processed successfully.")
}

// printKeystoreWarning reports the Java keystores rewritten to PEM whose .pem file
// was not produced. The config itself is correct and stamped ACCEPTED -- but it names
// files that do not exist, so tibftlserver will not start on it. That is severe enough
// to say in those words, and it is the one thing standing between this output and a
// working FSK server.
func printKeystoreWarning(w io.Writer, cfg *translator.BrokerConfig, outputDir string, autoRan bool, n int) {
	pending := translator.PendingKeystores(cfg)
	if len(pending) == 0 {
		return
	}
	brokerPath := filepath.Join(outputDir, fmt.Sprintf("kof.broker.%d.properties", n))
	fmt.Fprintf(w, "\n*** SEVERE WARNING -- %s WILL NOT RUN WITH FSK AS IT STANDS ***\n", brokerPath)
	fmt.Fprintf(w, "%d Java keystore(s) were rewritten to PEM, but the .pem file(s) do not exist.\n", len(pending))
	if autoRan {
		fmt.Fprintln(w, "--auto could not create them here -- see the warnings above.")
	}
	fmt.Fprintln(w, "tibftlserver will fail at startup on the missing file. Create them first:")
	for i, kc := range pending {
		fmt.Fprintf(w, "\n  %d. %s\n", i+1, kc.TypeKey)
		for _, cmd := range kc.Commands() {
			fmt.Fprintf(w, "       %s\n", cmd)
		}
	}
	if !autoRan {
		fmt.Fprintf(w, "\nOr re-run with --auto, on a host that has %s, to run these for you.\n",
			pending[0].FromLoc)
	}
}

// printResolveSummary prints, after an INVALID run, a numbered list of the
// unresolved settings -- each as "line N: key = value" with what's wrong and the
// fix -- then points --auto at the lines it can fix and lists the lines that need a
// human. The operator runs --auto and/or edits the >>>>>>> blocks, then re-runs.
func printResolveSummary(w io.Writer, s translator.ResolveSummary, outputDir string, autoRan bool, n int) {
	brokerPath := filepath.Join(outputDir, fmt.Sprintf("kof.broker.%d.properties", n))
	fmt.Fprintf(w, "\nINVALID -- %d setting(s) TIBCO FTL(R) Service for Kafka (FSK) cannot use, in %s:\n",
		s.Total(), brokerPath)

	var autoLines, youLines []int
	autoAny, youAny := false, false
	for i, it := range s.Items {
		val := it.Value
		if it.Note != "" {
			val += "   (file: " + it.Note + ")"
		}
		what, fix, autoFixable := resolveExplain(it.Kind, autoRan)
		// A config fetched over the Admin API has no source file, so there is no
		// line to cite; naming one would just be wrong.
		if it.Line > 0 {
			fmt.Fprintf(w, "\n  %d. line %d:  %s = %s\n", i+1, it.Line, it.Key, val)
		} else {
			fmt.Fprintf(w, "\n  %d. %s = %s\n", i+1, it.Key, val)
		}
		fmt.Fprintf(w, "        %s\n", what)
		fmt.Fprintf(w, "        %s\n", fix)
		if autoFixable && !autoRan {
			autoAny = true
			if it.Line > 0 {
				autoLines = append(autoLines, it.Line)
			}
		} else {
			youAny = true
			if it.Line > 0 {
				youLines = append(youLines, it.Line)
			}
		}
	}

	fmt.Fprintln(w)
	if autoAny {
		if len(autoLines) > 0 {
			fmt.Fprintf(w, "Run with --auto to convert line(s) %s for you (keystore -> PEM).\n", joinInts(autoLines))
		} else {
			fmt.Fprintln(w, "Run with --auto to convert the keystore(s) to PEM for you.")
		}
	}
	if youAny {
		if len(youLines) > 0 {
			fmt.Fprintf(w, "Line(s) needing you: %s -- edit the >>>>>>> block in the file.\n", joinInts(youLines))
		} else {
			fmt.Fprintln(w, "Edit the >>>>>>> block(s) in the file for the setting(s) above.")
		}
	}
	fmt.Fprintf(w, "Then re-run the same command:\n  tibftlimportconfig -output-dir %s %s\n", outputDir, brokerPath)
}

// resolveExplain returns, for a resolve kind, a one-line "what's wrong", a one-line
// "fix", and whether --auto can do it. autoRan tweaks the keystore wording (--auto
// already tried but the .jks wasn't on this host).
func resolveExplain(kind translator.ResolveKind, autoRan bool) (what, fix string, autoFixable bool) {
	switch kind {
	case translator.KindKeystore:
		what = "a Java keystore (JKS/PKCS12); FSK reads PEM only."
		if autoRan {
			return what, "Fix: --auto couldn't here (file not on this host). Run --auto where the .jks is, or use the commands in the block.", true
		}
		return what, "Fix: run with --auto to convert it, or run the keytool/openssl commands in the block.", true
	case translator.KindHandler:
		return "a custom Java callback class FSK can't run.",
			"Fix: in the block, set a backend (oauth/file/inline) and fill its params.", false
	case translator.KindBackendParams:
		return "a backend is selected but its params are missing.",
			"Fix: in the block, fill the params (oauth: jwks url + issuer + audience; inline: jaas users).", false
	case translator.KindMechanism:
		return "a SASL mechanism FSK can't serve (it serves PLAIN and OAUTHBEARER only).",
			"Fix: in the block, switch this listener to PLAIN or OAUTHBEARER.", false
	case translator.KindAuthorizer:
		return "a custom authorizer; FSK supports the standard one.",
			"Fix: in the block, set the value to: " + translator.AuthorizerCanonical + ".", false
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

// splitCSVTrimmed splits a comma-separated string and trims whitespace from each token.
func splitCSVTrimmed(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
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

// generatePorts picks one FTL port per pserver in two non-overlapping ranges:
//   - realm server ports: 5600–5699
//   - pserver connection ports: 5700–5799
//
// The choice is arbitrary but NOT random: the generator is seeded from the cluster
// itself (node ids and Kafka listener addresses), so converting the same brokers twice
// produces the same YAML. Re-running the tool should not churn the output, and the
// checked-in examples would otherwise differ on every regeneration. Two clusters on one
// host still get different ports, because they must already differ in their Kafka
// listeners to coexist.
func generatePorts(cfgs []*translator.BrokerConfig) translator.PortMap {
	n := len(cfgs)
	pm := translator.PortMap{
		RealmPorts:   make([]int, n),
		PserverPorts: make([]int, n),
	}
	r := rand.New(rand.NewPCG(clusterSeed(cfgs), 0))
	used := map[int]bool{}
	pick := func(lo, hi int) int {
		for {
			p := r.IntN(hi-lo) + lo
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

// clusterSeed hashes the identity of the cluster being converted. It deliberately uses
// only what the source configuration says -- not the file path, the output directory or
// the clock -- so the same brokers seed the same ports no matter where the tool runs.
func clusterSeed(cfgs []*translator.BrokerConfig) uint64 {
	h := fnv.New64a()
	for _, cfg := range cfgs {
		fmt.Fprintf(h, "node=%d;", cfg.NodeID)
		for _, ld := range cfg.Listeners {
			fmt.Fprintf(h, "%s://%s:%d;", ld.Name, ld.AdvAddr, ld.AdvPort)
		}
		fmt.Fprint(h, "|")
	}
	return h.Sum64()
}
