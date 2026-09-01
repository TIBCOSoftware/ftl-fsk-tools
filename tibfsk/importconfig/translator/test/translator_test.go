/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tibco.com/ftl-support/tibftlimportconfig/translator"
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
	{"inter-broker-only", translator.StatusAccepted},        // inter-broker keys → unsupported.properties, file has nothing to resolve
	{"oauth-resolved", translator.StatusAccepted},           // oauth + JWKS endpoint → all go to unsupported.properties
	{"intervene-custom-ldap-handler", translator.StatusAccepted}, // handler → unsupported.properties
	{"intervene-opaque-handler", translator.StatusAccepted},      // handler → unsupported.properties
	{"intervene-custom-authorizer", translator.StatusInvalid},    // custom authorizer (whitelisted) → RESOLVE-REQUIRED
	{"oauth-missing-jwks", translator.StatusAccepted},            // handler + jwks → unsupported.properties; nothing left to resolve
	{"inter-broker-and-jks", translator.StatusAccepted},          // JKS keystores → rewritten to the PEM form FSK reads
	{"scram-only", translator.StatusInvalid},                     // SCRAM-only listener (sasl.enabled.mechanisms whitelisted, value invalid)
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
	status, _, err := translator.WriteKOFBrokerProperties(cfg, dir, 1)
	if err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	path := filepath.Join(dir, "kof.broker.1.properties")
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

// TestWriteMigrationConfig verifies that kafka-to-kof.properties is generated
// with the correct source and target broker addresses.
func TestWriteMigrationConfig(t *testing.T) {
	t.Run("single-broker", func(t *testing.T) {
		cfg, err := translator.ParseBrokerConfig(filepath.Join("testdata", "plaintext.properties"))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		dir := t.TempDir()
		if err := translator.WriteMigrationConfig([]*translator.BrokerConfig{cfg}, dir); err != nil {
			t.Fatalf("write: %v", err)
		}
		b, err := os.ReadFile(filepath.Join(dir, "kafka-to-kof.properties"))
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		out := string(b)
		if !strings.Contains(out, "source.bootstrap.servers=broker1:9092") {
			t.Errorf("expected source.bootstrap.servers=broker1:9092 in output:\n%s", out)
		}
		if !strings.Contains(out, "target.bootstrap.servers=<KOF-HOST-1>:9092") {
			t.Errorf("expected target.bootstrap.servers=<KOF-HOST-1>:9092 in output:\n%s", out)
		}
	})

	t.Run("multi-broker", func(t *testing.T) {
		// Build three minimal configs inline.
		hosts := []string{"kafka-a", "kafka-b", "kafka-c"}
		cfgs := make([]*translator.BrokerConfig, len(hosts))
		for i, h := range hosts {
			tmp := filepath.Join(t.TempDir(), "server.properties")
			content := fmt.Sprintf("node.id=%d\nlisteners=PLAINTEXT://0.0.0.0:9092\nadvertised.listeners=PLAINTEXT://%s:9092\n", i+1, h)
			if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
				t.Fatalf("write tmp: %v", err)
			}
			cfg, err := translator.ParseBrokerConfig(tmp)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			cfgs[i] = cfg
		}
		dir := t.TempDir()
		if err := translator.WriteMigrationConfig(cfgs, dir); err != nil {
			t.Fatalf("write: %v", err)
		}
		b, err := os.ReadFile(filepath.Join(dir, "kafka-to-kof.properties"))
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		out := string(b)
		want := "source.bootstrap.servers=kafka-a:9092,kafka-b:9092,kafka-c:9092"
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output:\n%s", want, out)
		}
		want = "target.bootstrap.servers=<KOF-HOST-1>:9092,<KOF-HOST-2>:9092,<KOF-HOST-3>:9092"
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output:\n%s", want, out)
		}
	})
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
