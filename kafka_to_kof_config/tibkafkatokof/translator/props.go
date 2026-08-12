package translator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// listenerKeyOrder defines the listener-related keys emitted first.
var listenerKeyOrder = []string{
	"listeners",
	"advertised.listeners",
	"listener.security.protocol.map",
	"inter.broker.listener.name",
	"controller.listener.names",
}

// ConfigStatus is the validation state the tool computes from the file content
// and stamps into a generated kof.broker.properties. It is never hand-set: the
// operator edits the RESOLVE-REQUIRED blocks and re-runs the tool, which recomputes
// the status until it is ACCEPTED.
type ConfigStatus string

const (
	// StatusInvalid: at least one setting is not usable as-is -- a custom Java class
	// the operator must replace with a backend, a JKS/PKCS12 keystore that must
	// become PEM, or a backend whose params (oauth JWKS, inline jaas users) are
	// missing. Each is wrapped in a RESOLVE-REQUIRED block. The file must not be run.
	StatusInvalid ConfigStatus = "INVALID"
	// StatusAccepted: nothing is left to resolve. Every security setting is a value
	// KoF understands and its params are present. Not a guarantee the config works,
	// only that the tool has nothing more to flag.
	StatusAccepted ConfigStatus = "ACCEPTED"
)

// resolveBandOpen/Close wrap each block the operator must edit, like a merge
// conflict marker, so the blocks are easy to find by eye and by search. They are
// comment lines, so the file still parses as Java .properties.
const resolveBandOpen = "# >>>>>>>>>>>>>>>> KOF RESOLVE-REQUIRED (edit below) >>>>>>>>>>>>>>>>"
const resolveBandClose = "# <<<<<<<<<<<<<<<< end KOF RESOLVE-REQUIRED <<<<<<<<<<<<<<<<<<<<<<<"

// WriteKOFBrokerProperties writes kof.broker.N.properties (N is 1-based) as a flat
// Java-style key=value file mirroring the structure of the input server.properties. It
// returns the validation status, the list of unsupported key=value pairs (routed to
// unsupported.properties by the caller), and any I/O error.
func WriteKOFBrokerProperties(cfg *BrokerConfig, outputDir string, n int) (ConfigStatus, []string, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return StatusInvalid, nil, fmt.Errorf("create output dir: %w", err)
	}
	path := filepath.Join(outputDir, fmt.Sprintf("kof.broker.%d.properties", n))
	f, err := os.Create(path)
	if err != nil {
		return StatusInvalid, nil, fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}
	defer f.Close()
	status, unsupported := writeKOFProps(f, cfg)
	fmt.Fprintf(os.Stdout, "Writing file: %s [\n", path)
	return status, unsupported, nil
}

// WriteUnsupportedProperties writes unsupported.properties containing key=value pairs
// that are not in the KoF broker properties whitelist. Only written when non-empty.
func WriteUnsupportedProperties(outputDir string, unsupportedKV []string) error {
	path := filepath.Join(outputDir, "unsupported.properties")
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()
	fmt.Fprintln(f, "# ====================================================================")
	fmt.Fprintln(f, "# KOF-CONFIG-STATUS: ACCEPTED")
	fmt.Fprintln(f, "#")
	fmt.Fprintln(f, "# Settings in this file were unable to be automatically converted by the tool tibkafkatokof.  No")
	fmt.Fprintln(f, "# compatible KoF settings were found to be equivalent. Not all unsupported settings will have a")
	fmt.Fprintln(f, "# material impact, and some may be unnecessary.  Unsupported settings are provided here for")
	fmt.Fprintln(f, "# reference, and may be manually resolved by the user if necessary.")
	fmt.Fprintln(f, "# ====================================================================")
	fmt.Fprintln(f)
	for _, kv := range unsupportedKV {
		fmt.Fprintln(f, kv)
	}
	fmt.Fprintf(os.Stdout, "Writing file: %s\n", path)
	return nil
}

// configStatus reports the file's validation state: StatusInvalid if any authorizer
// is a custom class, any keystore is JKS/PKCS12, or any SASL mechanism is unservable;
// otherwise StatusAccepted. Only properties that appear in kof.broker.properties
// (i.e. pass isSupportedBrokerProperty) are considered.
func configStatus(cfg *BrokerConfig) ConfigStatus {
	for _, k := range cfg.SettingKeys {
		if !isSupportedBrokerProperty(k) {
			continue
		}
		switch {
		case isAuthorizerKey(k):
			if recognized, _ := resolveAuthorizer(cfg.Settings[k]); !recognized {
				return StatusInvalid
			}
		case isKeystoreTypeKey(k):
			if isJavaKeystore(cfg.Settings[k]) {
				return StatusInvalid
			}
		case isMechanismsKey(k):
			if mechanismsUnservable(cfg.Settings[k]) {
				return StatusInvalid
			}
		}
	}
	return StatusAccepted
}

// writeStatusBanner stamps the KOF-CONFIG-STATUS banner at the top of
// kof.broker.N.properties. The tool recomputes and rewrites it on every run.
func writeStatusBanner(f *os.File, status ConfigStatus) {
	fmt.Fprintln(f, "# ====================================================================")
	fmt.Fprintf(f, "# KOF-CONFIG-STATUS: %s\n", status)
	if status == StatusInvalid {
		fmt.Fprintln(f, "# This file has UNRESOLVED settings and MUST NOT be run. Each block tagged")
		fmt.Fprintln(f, "# RESOLVE-REQUIRED below needs an edit (a custom Java class to replace with a")
		fmt.Fprintln(f, "# backend, a Java keystore to convert to PEM, or a backend's missing params).")
		fmt.Fprintln(f, "# Jump to each by searching for >>>>>>> , make the edit, and re-run the tool;")
		fmt.Fprintln(f, "# it re-checks and stamps ACCEPTED once nothing is left. Do not edit this")
		fmt.Fprintln(f, "# status line by hand -- the tool sets it.")
	} else {
		fmt.Fprintln(f, "# Settings in this file were automatically converted by the tool tibkafkatokof.  All")
		fmt.Fprintln(f, "# settings after conversions are compatible with KoF.  Settings that could not")
		fmt.Fprintln(f, "# automatically convert were written to a separate file: unsupported.properties.")
	}
	fmt.Fprintln(f, "# ====================================================================")
	fmt.Fprintln(f)
}

func writeKOFProps(f *os.File, cfg *BrokerConfig) (ConfigStatus, []string) {
	status := configStatus(cfg)

	writeStatusBanner(f, status)

	fmt.Fprintf(f, "# KOF Broker Properties\n")
	fmt.Fprintf(f, "# Generated by tibkafkatokof from: %s\n", cfg.SourceFile)
	fmt.Fprintf(f, "# Node ID: %d\n", cfg.NodeID)
	fmt.Fprintln(f)

	var unsupported []string

	// Emit standard listener keys first
	fmt.Fprintln(f, "# --- Listeners ---")
	emitted := make(map[string]bool)
	for _, k := range listenerKeyOrder {
		emitted[k] = true
		v, ok := cfg.Settings[k]
		if !ok {
			continue
		}
		if !isSupportedBrokerProperty(k) {
			unsupported = append(unsupported, k+"="+v)
			continue
		}
		emitProp(f, cfg, k)
	}
	// Emit per-listener.name.* keys (in insertion order)
	for _, k := range cfg.SettingKeys {
		if emitted[k] || !isListenerNameKey(k) {
			continue
		}
		emitted[k] = true
		if !isSupportedBrokerProperty(k) {
			unsupported = append(unsupported, k+"="+cfg.Settings[k])
			continue
		}
		emitProp(f, cfg, k)
	}
	fmt.Fprintln(f)

	// Emit all remaining properties in insertion order
	fmt.Fprintln(f, "# --- All remaining properties ---")
	for _, k := range cfg.SettingKeys {
		if emitted[k] {
			continue
		}
		if !isSupportedBrokerProperty(k) {
			unsupported = append(unsupported, k+"="+cfg.Settings[k])
			continue
		}
		emitProp(f, cfg, k)
	}
	return status, unsupported
}

// emitProp writes one whitelisted property, dispatching security-relevant ones to
// their handlers: authorizer.class.name is translated or flagged, Java keystores are
// flagged for conversion, and SASL mechanisms are flagged when unservable. Handler
// classes and inter-broker/controller keys never reach emitProp — they are routed to
// unsupported.properties by writeKOFProps before this function is called.
func emitProp(f *os.File, cfg *BrokerConfig, k string) {
	v := cfg.Settings[k]
	switch {
	case isAuthorizerKey(k):
		emitAuthorizer(f, cfg, k, v)
	case isKeystoreTypeKey(k) && isJavaKeystore(v):
		emitJavaKeystore(f, cfg, k, v)
	case isMechanismsKey(k) && mechanismsUnservable(v):
		emitMechanisms(f, cfg, k, v)
	case runtimeRejects(k):
		emitRejected(f, k, v)
	default:
		fmt.Fprintf(f, "%s=%s\n", k, v)
	}
}

// emitRejected comments out a key the ftlserver runtime would reject at startup
// (a KRaft/control key or an unsupported security key). The key is kept for
// reference, commented, so the generated file boots. Inter-broker/controller keys
// get the fuller note that KoF secures that traffic through the FTL servers.
func emitRejected(f *os.File, k, v string) {
	if isInterBrokerKey(k) {
		fmt.Fprintf(f, "# IGNORED by the ftlserver: %s configures inter-broker/controller traffic,\n", k)
		fmt.Fprintf(f, "# which in KoF uses the FTL servers' own connections, not a Kafka listener.\n")
		fmt.Fprintf(f, "# Secure the FTL servers separately (realm tls.server.*/auth.providers).\n")
	} else {
		fmt.Fprintf(f, "# NOT a supported KoF property; the ftlserver rejects it. Kept for reference only.\n")
	}
	fmt.Fprintf(f, "#%s=%s\n", k, v)
}

// emitMechanisms flags a listener whose SASL mechanisms KoF cannot serve at all
// (e.g. SCRAM-only). The value is commented out so the listener has no mechanism,
// making the file INVALID until the operator switches to PLAIN or OAUTHBEARER.
func emitMechanisms(f *os.File, cfg *BrokerConfig, k, v string) {
	src := cfg.SettingLines[k]
	_, unsup := mechanismsSupport(v)
	fmt.Fprintln(f, resolveBandOpen)
	fmt.Fprintf(f, "# [source:%d] original: %s=%s\n", src, k, v)
	fmt.Fprintf(f, "# RESOLVE-REQUIRED: KoF cannot serve %s. It supports PLAIN and OAUTHBEARER.\n", strings.Join(unsup, ", "))
	fmt.Fprintf(f, "# Switch this listener to one of those (and configure its backend), then uncomment:\n")
	fmt.Fprintf(f, "#%s=<PLAIN|OAUTHBEARER>\n", k)
	fmt.Fprintln(f, resolveBandClose)
}

// emitJavaKeystore flags a JKS/PKCS12 keystore type as RESOLVE-REQUIRED: KoF reads
// PEM, so a Java keystore silently would not work. The type is commented out (no
// active value) and the block carries the exact conversion commands, named for the
// real .location file, so the file is INVALID until the operator converts and
// uncomments it.
func emitJavaKeystore(f *os.File, cfg *BrokerConfig, k, v string) {
	src := cfg.SettingLines[k]
	srcType := strings.ToUpper(strings.TrimSpace(v))

	// Keystore (private key + chain) vs truststore (CA certs only): the truststore
	// openssl step adds -nokeys.
	kind := "keystore"
	if strings.Contains(strings.ToLower(k), "truststore") {
		kind = "truststore"
	}
	nokeys := ""
	if kind == "truststore" {
		nokeys = "-nokeys "
	}

	// Name the files from the matching .location, so the commands are copy-paste.
	locKey := k[:len(k)-len(".type")] + ".location"
	loc := cfg.Settings[locKey]
	if loc == "" {
		loc = "<" + kind + ">"
	}
	pem := strings.TrimSuffix(loc, filepath.Ext(loc)) + ".pem"
	p12 := strings.TrimSuffix(loc, filepath.Ext(loc)) + ".p12"

	fmt.Fprintln(f, resolveBandOpen)
	fmt.Fprintf(f, "# [source:%d] original: %s=%s\n", src, k, v)
	fmt.Fprintf(f, "# RESOLVE-REQUIRED: KoF reads PEM, not a %s %s. This is a real file conversion,\n", srcType, kind)
	fmt.Fprintf(f, "# NOT just a rename -- run the conversion below to actually create the .pem file:\n")
	if srcType == "JKS" {
		fmt.Fprintf(f, "#   keytool -importkeystore -srckeystore %s -srcstoretype JKS \\\n", loc)
		fmt.Fprintf(f, "#           -destkeystore %s -deststoretype PKCS12\n", p12)
		fmt.Fprintf(f, "#   openssl pkcs12 -in %s -nodes %s-out %s\n", p12, nokeys, pem)
	} else { // PKCS12 -> one step
		fmt.Fprintf(f, "#   openssl pkcs12 -in %s -nodes %s-out %s\n", loc, nokeys, pem)
	}
	fmt.Fprintf(f, "# Only AFTER the .pem file exists, set %s=%s and uncomment:\n", locKey, pem)
	fmt.Fprintf(f, "#%s=PEM\n", k)
	fmt.Fprintf(f, "# (The tool only checks the type is not a Java keystore -- it cannot verify the\n")
	fmt.Fprintf(f, "# .pem exists, so flipping this without running the conversion will fail at runtime.)\n")
	fmt.Fprintln(f, resolveBandClose)
}

// emitAuthorizer rewrites authorizer.class.name. A recognized Kafka authorizer
// class is replaced with authorizerCanonical; KoF enforces the same ACL model.
// Any other class is a custom Java authorizer KoF cannot run, so it is left with
// no active value and the file is INVALID until the operator resolves it.
func emitAuthorizer(f *os.File, cfg *BrokerConfig, k, v string) {
	src := cfg.SettingLines[k]
	recognized, canonical := resolveAuthorizer(v)

	if recognized {
		// Already canonical (re-run / operator edit): write it through. A Java class
		// is rewritten to the canonical value, keeping the original as a comment.
		if !strings.EqualFold(strings.TrimSpace(v), canonical) {
			fmt.Fprintf(f, "# [source:%d] original: %s=%s\n", src, k, v)
			fmt.Fprintf(f, "# %s rewritten to %q. KoF enforces the same ACL model:\n", strings.TrimSpace(v), canonical)
			fmt.Fprintf(f, "# default-deny, super.users bypass, per-principal allow rules.\n")
		}
		fmt.Fprintf(f, "%s=%s\n", k, canonical)
		return
	}
	fmt.Fprintln(f, resolveBandOpen)
	fmt.Fprintf(f, "# [source:%d] original: %s=%s\n", src, k, v)
	fmt.Fprintf(f, "# RESOLVE-REQUIRED: unrecognized authorizer. Supported: %s.\n", strings.Join(recognizedAuthorizerNames(), ", "))
	fmt.Fprintf(f, "# A custom Java authorizer cannot be run. Uncomment only if it is\n")
	fmt.Fprintf(f, "# ACL-equivalent to a standard authorizer:\n")
	fmt.Fprintf(f, "#%s=%s\n", k, authorizerCanonical)
	fmt.Fprintln(f, resolveBandClose)
}

// emitHandlerClass writes a SASL callback handler class key. A value KoF
// understands (a canonical token, or a recognized Java class it translates) is
// written active when its backend's params are present; otherwise a
// RESOLVE-REQUIRED block tells the operator exactly what to add or choose.
func emitHandlerClass(f *os.File, cfg *BrokerConfig, k, v string) {
	src := cfg.SettingLines[k]
	o := resolveHandler(cfg, k)

	// Unrecognized custom class: the operator must choose a backend.
	if o.NeedsPick {
		fmt.Fprintln(f, resolveBandOpen)
		fmt.Fprintf(f, "# [source:%d] original: %s=%s\n", src, k, v)
		fmt.Fprintln(f, "# RESOLVE-REQUIRED: KoF cannot run custom Java handler class.")
		if o.Guess == BackendNone {
			fmt.Fprintln(f, "# Set the value to one of (oauth, file, inline) and uncomment:")
			fmt.Fprintf(f, "#%s=<oauth|file|inline>\n", k)
		} else {
			fmt.Fprintf(f, "# Name suggests %q (one of oauth, file, inline) -- verify, then uncomment:\n", o.Guess)
			fmt.Fprintf(f, "#%s=%s\n", k, o.Guess)
			// The guessed backend also needs its params -- show them so it can be
			// resolved in one pass instead of waiting for the next run to flag them.
			emitBackendParamHints(f, k, o.Guess)
		}
		fmt.Fprintln(f, resolveBandClose)
		return
	}

	// Backend chosen but its params are missing: write the value active, then a
	// RESOLVE-REQUIRED block listing the keys to add.
	if !o.Resolved {
		if o.FromClass {
			fmt.Fprintf(f, "# [source:%d] original: %s=%s\n", src, k, v)
		}
		fmt.Fprintf(f, "%s=%s\n", k, o.Active)
		fmt.Fprintln(f, resolveBandOpen)
		fmt.Fprintf(f, "# RESOLVE-REQUIRED: %s selected, but its params are missing. Set these, then re-run:\n", o.Active)
		emitBackendParamHints(f, k, o.Backend)
		fmt.Fprintln(f, resolveBandClose)
		return
	}

	// Resolved: write the active value. If it came from a Java class, keep the
	// original as a comment for provenance.
	if o.FromClass {
		fmt.Fprintf(f, "# [source:%d] original: %s=%s\n", src, k, v)
	}
	fmt.Fprintf(f, "%s=%s\n", k, o.Active)
}

// emitBackendParamHints writes the commented param lines for a backend: a key with
// a placeholder value and a trailing note, or a bare note (file realm config).
func emitBackendParamHints(f *os.File, handlerKey string, be AuthBackend) {
	for _, p := range backendParamHints(handlerKey, be) {
		if p.Key == "" {
			fmt.Fprintf(f, "# %s\n", p.Note)
			continue
		}
		fmt.Fprintf(f, "#%s=<value>   # %s\n", p.Key, p.Note)
	}
}

func isListenerNameKey(k string) bool {
	const pfx = "listener.name."
	return len(k) > len(pfx) && k[:len(pfx)] == pfx
}
