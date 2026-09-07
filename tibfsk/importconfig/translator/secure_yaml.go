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

// SecureOpts holds the TLS and authentication values passed via CLI flags.
type SecureOpts struct {
	// TLS fields
	TLSCert        string
	TLSKey         string
	TLSKeyPassword string
	TLSCA          string

	// OAuth2 fields — server-to-server
	OAuthTokenURL      string
	OAuthJWKSURL       string
	OAuthClientID      string
	OAuthClientSecret  string
	OAuthProviderTrust string

	// OAuth2 — configurable claim/audience values (defaults: "group", "preferred_username", "ftl")
	OAuthClaimRoles    string
	OAuthClaimUsername string
	OAuthAudience      string

	// OAuth2 — UI endpoint globals
	OAuthUIAuthURL   string
	OAuthUITokenURL  string
	OAuthUILogoutURL string

	// OAuth2 — UI client credentials (per-server ftlserver.properties)
	OAuthUIClientID     string
	OAuthUIClientSecret string

	// Shared auth
	AuthRolemap          string // auth.rolemap: ftlserver.properties for oauth2
	RealmServiceUser     string // services.realm.user (oauth2 mode; default "primary")
	RealmServicePassword string // services.realm.password (oauth2 mode; default "primary-pw")

	// Per-server internal credentials (file-auth+tls and tls-only modes)
	ServerUser     string // user in ftlserver.properties (default "internal")
	ServerPassword string // password in ftlserver.properties (default "internal-pw")

	// Basic (file-based) auth
	AuthUsersFile string
	// KafkaUsersFile is the tool's OWN auto-generated kafka-users.txt (inline JAAS
	// PLAIN users), wired as a second file: provider -- same as the base
	// cluster YAML. Not an operator input; populated from the generated file.
	KafkaUsersFile string

	// mTLS — needed when an Apache Kafka mTLS listener is present
	TLSServerTrust       string // tls.server.trust.file — CA used to verify inbound client certs
	TLSClientCert        string // tls.client.cert — cert presented when connecting to other FTL servers
	TLSClientKey         string // tls.client.private.key
	TLSClientKeyPassword string // tls.client.private.key.password (optional)

}

// authFlags records which auth providers are active for this configuration.
type authFlags struct {
	FileAuth bool // SASL_SSL PLAIN listener + AuthUsersFile
	MTLS     bool // TLSServerTrust != "" (client-cert verification)
	OAuth2   bool // OAUTHBEARER listener + OAuthTokenURL
}

// detectAuth inspects listeners and opts to determine which auth providers are active.
func detectAuth(cfg *BrokerConfig, opts SecureOpts) authFlags {
	var ac authFlags
	for _, l := range cfg.Listeners {
		switch l.AuthMethod {
		case "sasl_tls":
			if opts.AuthUsersFile != "" {
				ac.FileAuth = true
			}
		case "oauth_tls":
			if opts.OAuthTokenURL != "" {
				ac.OAuth2 = true
			}
		}
	}
	if opts.TLSServerTrust != "" {
		ac.MTLS = true
	}
	// A secured cluster's FTL servers need auth (TLS requires auth). The base
	// cluster YAML wires file:ftl-users whenever a users file exists; mirror that
	// here so a non-SASL (pure SSL/mTLS) listener's secure YAML isn't TLS-without-auth.
	if opts.AuthUsersFile != "" {
		ac.FileAuth = true
	}
	return ac
}

// buildAuthProviders returns the comma-separated auth.providers value for all active providers.
// Order: file → mtls → oauth2
func buildAuthProviders(ac authFlags, opts SecureOpts) string {
	var parts []string
	if ac.FileAuth {
		parts = append(parts, "file:"+opts.AuthUsersFile)
		if opts.KafkaUsersFile != "" {
			parts = append(parts, "file:"+opts.KafkaUsersFile)
		}
	}
	if ac.MTLS {
		parts = append(parts, "mtls")
	}
	if ac.OAuth2 {
		parts = append(parts, "oauth2")
	}
	return strings.Join(parts, ",")
}

// ShouldWriteSecure reports whether a secure YAML should be generated.
// Requires security detected in the broker config AND at least one opt populated.
func ShouldWriteSecure(cfg *BrokerConfig, opts SecureOpts) bool {
	if !cfg.IsSecure {
		return false
	}
	return opts.TLSCert != "" || opts.OAuthTokenURL != "" || opts.AuthUsersFile != ""
}

// WriteKOFSecureYAML generates the secure variant of the cluster YAML
// (<stem>-secure.yaml). See clusterYAMLStem for the stem.
//
// It covers every server, laid out exactly as WriteKOFClusterYAML lays out the plain
// variant: globals.core.servers names the first shard only, every server carries its own
// "- realm:" block with its own data directory, and the servers past core.servers state
// their address in an "- ftl:" block.
func WriteKOFSecureYAML(cfg *BrokerConfig, outputDir, dataDir string, propsPaths []string, realmPath string, numPservers int, ports PortMap, coreServers []CoreServer, opts SecureOpts, drOpts DROpts, copts ClusterOpts) error {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	path := filepath.Join(outputDir, clusterYAMLStem(numPservers)+"-secure.yaml")
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	host := resolveHost(cfg.KOFHost)
	cores := buildCoreServers(coreServers, host, ports)
	ac := detectAuth(cfg, opts)
	providers := buildAuthProviders(ac, opts)

	fmt.Fprintln(f, "# FSK Cluster — Secure Configuration")
	fmt.Fprintf(f, "# Generated by tibftlimportconfig from: %s\n", cfg.SourceFile)
	fmt.Fprintln(f, "#")
	if providers != "" {
		fmt.Fprintf(f, "# Auth providers: %s\n", providers)
	} else {
		fmt.Fprintln(f, "# Auth providers: none — TLS encryption only")
	}
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

	if providers != "" {
		fmt.Fprintf(f, "  auth.providers: %s\n", providers)
	}
	if ac.OAuth2 {
		writeOAuthGlobals(f, opts)
	}
	if drOpts.Enabled() {
		fmt.Fprintf(f, "  dr: %s\n", buildDRString(drOpts.DRServers))
		fmt.Fprintln(f, "  auto.init.primary.on.first.startup: true")
	}
	fmt.Fprintln(f)

	// In oauth2 mode the realm authenticates the services with a dedicated credential;
	// it lives on every realm entry now that there is no shared services section.
	var realmCreds []string
	if ac.OAuth2 {
		realmUser := opts.RealmServiceUser
		if realmUser == "" {
			realmUser = "primary"
		}
		realmPw := opts.RealmServicePassword
		if realmPw == "" {
			realmPw = "primary-pw"
		}
		realmCreds = []string{"user: " + realmUser, "password: " + realmPw}
	}

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
		writeRealmBlock(f, realmDataDir(dataDir, i), realmPath, label, copts, realmCreds...)

		// The block is always written for the logging settings; the credentials and the
		// TLS/OAuth properties inside it stay conditional on what this configuration uses.
		fmt.Fprintln(f, "  - ftlserver.properties:")
		if hasTLSOpts(opts) || ac.OAuth2 {
			if !ac.OAuth2 {
				serverUser := opts.ServerUser
				if serverUser == "" {
					serverUser = "internal"
				}
				serverPw := opts.ServerPassword
				if serverPw == "" {
					serverPw = "internal-pw"
				}
				fmt.Fprintf(f, "      user: %s\n", serverUser)
				fmt.Fprintf(f, "      password: %s\n", serverPw)
			}
			writeTLSProperties(f, opts)
			if ac.OAuth2 {
				writeOAuthServerProperties(f, opts)
			}
		}
		writeServerLogging(f, name, dataDir)

		fmt.Fprintln(f, "  - persistence:")
		fmt.Fprintf(f, "      name: pserver%d\n", i+1)
		fmt.Fprintf(f, "      data: %s/pserver%d\n", dataDir, i+1)
		fmt.Fprintf(f, "      kof.broker.properties: %s\n", propsPaths[i%len(propsPaths)])
		fmt.Fprintf(f, "      loglevel: %s\n", copts.LogLevel)
		// auth.type stays none here in phase 1; wiring the schema daemon to oauth2
		// alongside the rest of this file's security settings is phase 2.
		//
		// The schema daemon is its own small cluster and does not scale with the
		// pservers: it stays on the first 3 servers however many shards there are.
		if copts.Tibschemad && i < min3(numPservers) {
			writeTibschemadBlock(f, i+1, min3(numPservers))
		}
		fmt.Fprintln(f)
	}

	fmt.Fprintf(os.Stdout, "Writing file: %s\n", path)
	return nil
}

func writeOAuthGlobals(f *os.File, opts SecureOpts) {
	claimRoles := opts.OAuthClaimRoles
	if claimRoles == "" {
		claimRoles = "group"
	}
	claimUsername := opts.OAuthClaimUsername
	if claimUsername == "" {
		claimUsername = "preferred_username"
	}
	audience := opts.OAuthAudience
	if audience == "" {
		audience = "ftl"
	}
	fmt.Fprintf(f, "  oauth2.claim.roles: %s\n", claimRoles)
	fmt.Fprintf(f, "  oauth2.claim.username: %s\n", claimUsername)
	fmt.Fprintf(f, "  oauth2.audience: %s\n", audience)
	if opts.OAuthUIAuthURL != "" {
		fmt.Fprintf(f, "  oauth2.ui.endpoint.auth: %s\n", opts.OAuthUIAuthURL)
	}
	if opts.OAuthUITokenURL != "" {
		fmt.Fprintf(f, "  oauth2.ui.endpoint.token: %s\n", opts.OAuthUITokenURL)
	}
	if opts.OAuthUILogoutURL != "" {
		fmt.Fprintf(f, "  oauth2.ui.endpoint.logout: %s\n", opts.OAuthUILogoutURL)
	}
	if opts.OAuthTokenURL != "" {
		fmt.Fprintf(f, "  oauth2.svr.endpoint.token: %s\n", opts.OAuthTokenURL)
	}
}

func writeTLSProperties(f *os.File, opts SecureOpts) {
	if opts.TLSCert != "" {
		fmt.Fprintf(f, "      tls.server.cert: %s\n", opts.TLSCert)
	}
	if opts.TLSKey != "" {
		fmt.Fprintf(f, "      tls.server.private.key: %s\n", opts.TLSKey)
	}
	if opts.TLSKeyPassword != "" {
		fmt.Fprintf(f, "      tls.server.private.key.password: %s\n", opts.TLSKeyPassword)
	}
	if opts.TLSServerTrust != "" {
		fmt.Fprintf(f, "      tls.server.trust.file: %s\n", opts.TLSServerTrust)
	}
	if opts.TLSCA != "" {
		fmt.Fprintf(f, "      tls.client.trust.file: %s\n", opts.TLSCA)
	}
	if opts.TLSClientCert != "" {
		fmt.Fprintf(f, "      tls.client.cert: %s\n", opts.TLSClientCert)
	}
	if opts.TLSClientKey != "" {
		fmt.Fprintf(f, "      tls.client.private.key: %s\n", opts.TLSClientKey)
	}
	if opts.TLSClientKeyPassword != "" {
		fmt.Fprintf(f, "      tls.client.private.key.password: %s\n", opts.TLSClientKeyPassword)
	}
}

func writeOAuthServerProperties(f *os.File, opts SecureOpts) {
	if opts.OAuthJWKSURL != "" {
		fmt.Fprintf(f, "      oauth2.validation.key: %s\n", opts.OAuthJWKSURL)
	}
	if opts.OAuthUIClientID != "" {
		fmt.Fprintf(f, "      oauth2.ui.client.id: %s\n", opts.OAuthUIClientID)
	}
	if opts.OAuthUIClientSecret != "" {
		fmt.Fprintf(f, "      oauth2.ui.client.secret: %s\n", opts.OAuthUIClientSecret)
	}
	if opts.OAuthProviderTrust != "" {
		fmt.Fprintf(f, "      oauth2.provider.trust.file: %s\n", opts.OAuthProviderTrust)
	}
	if opts.OAuthClientID != "" {
		fmt.Fprintf(f, "      oauth2.svr.client.id: %s\n", opts.OAuthClientID)
	}
	if opts.OAuthClientSecret != "" {
		fmt.Fprintf(f, "      oauth2.svr.client.secret: %s\n", opts.OAuthClientSecret)
	}
	if opts.AuthRolemap != "" {
		fmt.Fprintf(f, "      auth.rolemap: %s\n", opts.AuthRolemap)
	}
}

func hasTLSOpts(opts SecureOpts) bool {
	return opts.TLSCert != "" || opts.TLSKey != "" || opts.TLSServerTrust != "" ||
		opts.TLSClientCert != "" || opts.TLSCA != ""
}
