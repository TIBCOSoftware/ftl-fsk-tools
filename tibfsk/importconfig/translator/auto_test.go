/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

// makeCfg writes src to a temp server.properties and parses it.
func makeCfg(t *testing.T, src string) *BrokerConfig {
	t.Helper()
	in := filepath.Join(t.TempDir(), "server.properties")
	if err := os.WriteFile(in, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := ParseBrokerConfig(in)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// On success, the keystore type becomes PEM and the location points at the .pem.
func TestAutoConvertKeystores_Success(t *testing.T) {
	dir := t.TempDir()
	jks := filepath.Join(dir, "broker.keystore.jks")
	if err := os.WriteFile(jks, []byte("dummy"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := makeCfg(t, "node.id=1\nssl.keystore.type=JKS\nssl.keystore.location="+jks+"\nssl.keystore.password=secret\n")

	var ran [][]string
	fake := func(name string, args ...string) (string, error) {
		ran = append(ran, append([]string{name}, args...))
		return "", nil // pretend keytool/openssl succeeded
	}
	rep := autoResolve(cfg, io.Discard, fake)

	if rep.KeystoresConverted != 1 || rep.KeystoresFailed != 0 {
		t.Fatalf("report = %+v, want 1 converted", rep)
	}
	if cfg.Settings["ssl.keystore.type"] != "PEM" {
		t.Errorf("type = %q, want PEM", cfg.Settings["ssl.keystore.type"])
	}
	if got := cfg.Settings["ssl.keystore.location"]; filepath.Ext(got) != ".pem" {
		t.Errorf("location = %q, want a .pem", got)
	}
	if len(ran) != 2 || ran[0][0] != "keytool" || ran[1][0] != "openssl" {
		t.Errorf("expected keytool then openssl, got %v", ran)
	}
}

// A missing source file is skipped gracefully (not a failure): no command runs. The
// config still names the PEM form -- that translation does not depend on this host
// having the .jks -- but the conversion stays pending, so the operator is still told
// the .pem does not exist.
func TestAutoConvertKeystores_MissingFile(t *testing.T) {
	cfg := makeCfg(t, "node.id=1\nssl.keystore.type=JKS\nssl.keystore.location=/nope/broker.jks\n")
	called := false
	fake := func(name string, args ...string) (string, error) { called = true; return "", nil }

	rep := autoResolve(cfg, io.Discard, fake)
	if rep.KeystoresSkipped != 1 || rep.KeystoresConverted != 0 || rep.KeystoresFailed != 0 {
		t.Fatalf("report = %+v, want 1 skipped", rep)
	}
	if called {
		t.Error("should not run any command when the source file is missing")
	}
	if cfg.Settings["ssl.keystore.type"] != "PEM" {
		t.Errorf("type = %q, want PEM (the translation does not need the file)", cfg.Settings["ssl.keystore.type"])
	}
	pending := PendingKeystores(cfg)
	if len(pending) != 1 || pending[0].FromLoc != "/nope/broker.jks" {
		t.Errorf("pending = %+v, want the unconverted /nope/broker.jks", pending)
	}
}

// --auto does NOT touch a custom handler class -- backend choice is the operator's.
func TestAutoResolve_LeavesHandlerForOperator(t *testing.T) {
	const key = "listener.name.sasl.plain.sasl.server.callback.handler.class"
	cfg := makeCfg(t, "node.id=1\nlisteners=SASL://0.0.0.0:9092\n"+key+"=com.acme.LdapPlainServerCallbackHandler\n")
	autoResolve(cfg, io.Discard, func(string, ...string) (string, error) { return "", nil })
	if got := cfg.Settings[key]; got != "com.acme.LdapPlainServerCallbackHandler" {
		t.Errorf("handler = %q, want it left unchanged (operator picks the backend)", got)
	}
}
