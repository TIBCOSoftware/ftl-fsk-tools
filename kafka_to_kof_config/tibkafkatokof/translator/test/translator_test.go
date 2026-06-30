// Package test is a separated, black-box test package for the translator. It
// exercises only the exported API (ParseBrokerConfig, WriteKOFBrokerProperties,
// WriteSupportList) against fixture files in testdata/, and checks three things:
//   - behavioral assertions: the status and rewritten values that must hold
//     regardless of exact wording;
//   - a fail-closed invariant: an INVALID output never leaves an active Java
//     class value for a handler/authorizer key;
//   - golden snapshots: the full generated file, regenerated with -update.
package test

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tibco.com/ftl-support/tibkafkatokof/translator"
)

var update = flag.Bool("update", false, "regenerate golden files")

// fixtures lists each testdata input and the validation status it must produce.
var fixtures = []struct {
	name   string
	status translator.ConfigStatus
}{
	{"recognized-standard", translator.StatusAccepted},      // PLAIN handler (inline + jaas users) + StandardAuthorizer
	{"authorizer-aclauthorizer", translator.StatusAccepted}, // AclAuthorizer -> standard
	{"plaintext", translator.StatusAccepted},                // no security settings -- pass-through
	{"inter-broker-only", translator.StatusAccepted},        // only IGNORED inter-broker keys
	{"oauth-resolved", translator.StatusAccepted},           // oauth + JWKS endpoint present
	{"handler-ldap-resolved", translator.StatusAccepted},    // operator picked ldap (realm-side; no broker params)
	{"intervene-custom-ldap-handler", translator.StatusInvalid},
	{"intervene-opaque-handler", translator.StatusInvalid},
	{"intervene-custom-authorizer", translator.StatusInvalid},
	{"oauth-missing-jwks", translator.StatusInvalid}, // oauth selected, no JWKS endpoint
	{"inter-broker-and-jks", translator.StatusInvalid},
	{"scram-only", translator.StatusInvalid}, // SCRAM-only listener: KoF can't serve it
}

// generate runs the exported path on a fixture and returns the status, the
// generated file's text, and its path (valid for the duration of the subtest).
func generate(t *testing.T, name string) (translator.ConfigStatus, string, string) {
	t.Helper()
	dir := t.TempDir()
	cfg, err := translator.ParseBrokerConfig(filepath.Join("testdata", name+".properties"))
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	status, err := translator.WriteKOFBrokerProperties(cfg, dir, "", 1)
	if err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	path := filepath.Join(dir, "kof.broker.properties")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return status, string(b), path
}

func TestFixtures(t *testing.T) {
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			status, out, path := generate(t, fx.name)

			// Behavioral: the returned status and the in-file marker must match,
			// and must be the status the fixture is meant to produce.
			if status != fx.status {
				t.Errorf("status = %q, want %q", status, fx.status)
			}
			if marker := "# KOF-CONFIG-STATUS: " + string(fx.status); !strings.Contains(out, marker) {
				t.Errorf("missing in-file marker %q", marker)
			}

			// Fail-closed: an INVALID file must flag the work and leave no active
			// Java class value for a handler/authorizer key.
			if fx.status == translator.StatusInvalid {
				if !strings.Contains(out, "RESOLVE-REQUIRED") {
					t.Errorf("INVALID output has no RESOLVE-REQUIRED line")
				}
				assertNoActiveJavaClass(t, path)
			}

			// Golden snapshot of the full file.
			goldenCompare(t, filepath.Join("testdata", "golden", fx.name+".properties"), out)
		})
	}
}

// TestSupportListGolden snapshots the --list-properties account so any wording or
// disposition change is caught and reviewed.
func TestSupportListGolden(t *testing.T) {
	var buf bytes.Buffer
	translator.WriteSupportList(&buf, false)
	goldenCompare(t, filepath.Join("testdata", "golden", "list-properties.txt"), buf.String())
}

// assertNoActiveJavaClass re-parses a generated file and fails if any active
// (uncommented) handler-class or authorizer.class.name value is still a Java class
// (a dotted package name). Canonical values (oauth, inline, standard) have no dot.
func assertNoActiveJavaClass(t *testing.T, path string) {
	t.Helper()
	cfg, err := translator.ParseBrokerConfig(path)
	if err != nil {
		t.Fatalf("re-parse %s: %v", path, err)
	}
	for k, v := range cfg.Settings {
		kl := strings.ToLower(k)
		classKey := kl == "authorizer.class.name" ||
			kl == "sasl.server.callback.handler.class" ||
			strings.HasSuffix(kl, ".sasl.server.callback.handler.class")
		if classKey && strings.Contains(v, ".") {
			t.Errorf("fail-closed violated: active Java class survived in INVALID output: %s=%s", k, v)
		}
		typeKey := kl == "ssl.keystore.type" || kl == "ssl.truststore.type" ||
			strings.HasSuffix(kl, ".ssl.keystore.type") || strings.HasSuffix(kl, ".ssl.truststore.type")
		if typeKey {
			switch strings.ToUpper(v) {
			case "JKS", "PKCS12":
				t.Errorf("fail-closed violated: active Java keystore survived in INVALID output: %s=%s", k, v)
			}
		}
	}
}

func goldenCompare(t *testing.T, goldenPath, got string) {
	t.Helper()
	if *update {
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden %s: %v", goldenPath, err)
		}
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden %s (regenerate with: go test ./translator/test/ -update): %v", goldenPath, err)
	}
	if got != string(want) {
		t.Errorf("output does not match golden %s.\nRegenerate with: go test ./translator/test/ -update\n--- got ---\n%s", goldenPath, got)
	}
}
