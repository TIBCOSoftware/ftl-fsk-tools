/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// renderProps renders a config (given as server.properties text) through the
// writer and returns the generated kof.broker.properties text and unsupported list.
func renderProps(t *testing.T, src string) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	in := filepath.Join(dir, "server.properties")
	if err := os.WriteFile(in, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := ParseBrokerConfig(in)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "kof.broker.properties")
	of, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	_, unsupported := writeKOFProps(of, cfg)
	of.Close()
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(b), unsupported
}

// parseSrc writes src to a temp server.properties and parses it.
func parseSrc(t *testing.T, src string) *BrokerConfig {
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

func TestUnsupportedScan(t *testing.T) {
	src := "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
		"sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,GSSAPI\n" +
		"delegation.token.secret.key=abc\n" +
		"delegation.token.max.lifetime.ms=1000\n" +
		"principal.builder.class=com.acme.Builder\n"
	items := UnsupportedScan(parseSrc(t, src))

	got := map[string]bool{}
	for _, it := range items {
		got[it.What] = true
	}
	for _, want := range []string{
		"sasl.enabled.mechanisms=SCRAM-SHA-256",
		"sasl.enabled.mechanisms=GSSAPI",
		"delegation.token.*",
		"principal.builder.class",
	} {
		if !got[want] {
			t.Errorf("missing unsupported item %q; got %v", want, got)
		}
	}
	// PLAIN is supported -- it must not be flagged.
	if got["sasl.enabled.mechanisms=PLAIN"] {
		t.Error("PLAIN should not be flagged unsupported")
	}
	// The delegation.token family is reported once, not per key.
	n := 0
	for _, it := range items {
		if it.What == "delegation.token.*" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("delegation.token.* reported %d times, want 1", n)
	}
}

func TestSummarize(t *testing.T) {
	// handler class goes to unsupported.properties and is not included in Summarize.
	// Only authorizer (whitelisted, unrecognized) and keystores (whitelisted, JKS) appear.
	src := "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
		"listener.name.sasl.plain.sasl.server.callback.handler.class=com.acme.MagicAuth\n" +
		"authorizer.class.name=com.acme.MyAuthorizer\n" +
		"ssl.keystore.type=JKS\n" +
		"ssl.keystore.location=/c/broker.keystore.jks\n" +
		"ssl.truststore.type=JKS\n" +
		"ssl.truststore.location=/c/kafka.truststore.jks\n"
	s := Summarize(parseSrc(t, src))
	if s.Total() != 3 {
		t.Errorf("Total() = %d, want 3 (1 authorizer + 2 keystores; handler goes to unsupported)", s.Total())
	}
	counts := map[ResolveKind]int{}
	for _, it := range s.Items {
		counts[it.Kind]++
		if it.Line == 0 {
			t.Errorf("item %s has no source line", it.Key)
		}
		if it.Kind == KindKeystore && it.Note == "" {
			t.Errorf("keystore item %s should carry its .location file", it.Key)
		}
	}
	if counts[KindHandler] != 0 || counts[KindAuthorizer] != 1 || counts[KindKeystore] != 2 {
		t.Errorf("kind counts wrong: %v (want handler=0, authorizer=1, keystore=2)", counts)
	}

	// A fully-resolved config has nothing to resolve.
	clean := "node.id=1\nlisteners=PLAINTEXT://0.0.0.0:9092\nauthorizer.class.name=" + authorizerCanonical + "\n"
	if got := Summarize(parseSrc(t, clean)).Total(); got != 0 {
		t.Errorf("clean config Total() = %d, want 0", got)
	}
}

const oauthHandler = "listener.name.sasl.oauthbearer.sasl.server.callback.handler.class"
const oauthJwks = "listener.name.sasl.oauthbearer.sasl.oauthbearer.jwks.endpoint.url"
const oauthValidatorClass = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler"

// Handler class keys are not in the FSK section 1 whitelist and go to
// unsupported.properties, not kof.broker.properties. The file is ACCEPTED.
func TestHandlerClass_GoesToUnsupported(t *testing.T) {
	src := "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
		oauthHandler + "=" + oauthValidatorClass + "\n" +
		oauthJwks + "=https://idp/realms/r/protocol/openid-connect/certs\n"
	out, unsupported := renderProps(t, src)

	// Handler and JWKS must not appear in kof.broker.properties.
	if strings.Contains(out, oauthHandler) {
		t.Errorf("handler class appeared in kof.broker.properties (should be in unsupported):\n%s", out)
	}
	if strings.Contains(out, oauthJwks) {
		t.Errorf("jwks endpoint appeared in kof.broker.properties (should be in unsupported):\n%s", out)
	}
	// Both must appear in the unsupported list.
	found := map[string]bool{}
	for _, kv := range unsupported {
		if strings.HasPrefix(kv, oauthHandler+"=") {
			found["handler"] = true
		}
		if strings.HasPrefix(kv, oauthJwks+"=") {
			found["jwks"] = true
		}
	}
	if !found["handler"] {
		t.Errorf("handler class not in unsupported list: %v", unsupported)
	}
	if !found["jwks"] {
		t.Errorf("jwks endpoint not in unsupported list: %v", unsupported)
	}
	// File is ACCEPTED: no RESOLVE-REQUIRED blocks.
	if strings.Contains(out, "RESOLVE-REQUIRED") {
		t.Errorf("unexpected RESOLVE-REQUIRED in output:\n%s", out)
	}
}

// Unrecognized custom handler class also goes to unsupported, not a RESOLVE-REQUIRED block.
func TestUnrecognizedHandler_GoesToUnsupported(t *testing.T) {
	handlerKey := "listener.name.sasl.plain.sasl.server.callback.handler.class"
	src := "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
		handlerKey + "=com.acme.LdapPlainServerCallbackHandler\n"
	out, unsupported := renderProps(t, src)

	if strings.Contains(out, handlerKey) {
		t.Errorf("handler key appeared in kof.broker.properties:\n%s", out)
	}
	found := false
	for _, kv := range unsupported {
		if strings.HasPrefix(kv, handlerKey+"=") {
			found = true
		}
	}
	if !found {
		t.Errorf("handler class not in unsupported list: %v", unsupported)
	}
}

// Idempotency: a file carrying canonical authorizer is stable across re-runs.
// Handler classes and oauth params go to unsupported and do not participate in idempotency.
func TestIdempotent_CanonicalAuthorizer(t *testing.T) {
	src := "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
		"authorizer.class.name=" + authorizerCanonical + "\n"
	out1, _ := renderProps(t, src)
	if !strings.Contains(out1, "authorizer.class.name="+authorizerCanonical+"\n") {
		t.Errorf("canonical authorizer value not preserved:\n%s", out1)
	}
	// Re-run on the generated output: value still preserved.
	out2, _ := renderProps(t, out1)
	if !strings.Contains(out2, "authorizer.class.name="+authorizerCanonical+"\n") {
		t.Errorf("re-run did not preserve canonical authorizer (not idempotent):\n%s", out2)
	}
}
