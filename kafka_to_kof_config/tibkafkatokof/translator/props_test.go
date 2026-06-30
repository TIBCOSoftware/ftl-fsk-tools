package translator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// renderProps renders a config (given as server.properties text) through the
// writer and returns the generated kof.broker.properties text.
func renderProps(t *testing.T, src string) string {
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
	writeKOFProps(of, cfg)
	of.Close()
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func hasMarker(out string, s ConfigStatus) bool {
	return strings.Contains(out, statusMarkerPrefix+" "+string(s))
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
	src := "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
		"listener.name.sasl.plain.sasl.server.callback.handler.class=com.acme.MagicAuth\n" +
		"authorizer.class.name=com.acme.MyAuthorizer\n" +
		"ssl.keystore.type=JKS\n" +
		"ssl.keystore.location=/c/broker.keystore.jks\n" +
		"ssl.truststore.type=JKS\n" +
		"ssl.truststore.location=/c/kafka.truststore.jks\n"
	s := Summarize(parseSrc(t, src))
	if s.Total() != 4 {
		t.Errorf("Total() = %d, want 4", s.Total())
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
	if counts[KindHandler] != 1 || counts[KindAuthorizer] != 1 || counts[KindKeystore] != 2 {
		t.Errorf("kind counts wrong: %v", counts)
	}

	// A fully-resolved config has nothing to resolve.
	clean := "node.id=1\nlisteners=PLAINTEXT://0.0.0.0:9092\nauthorizer.class.name=standard\n"
	if got := Summarize(parseSrc(t, clean)).Total(); got != 0 {
		t.Errorf("clean config Total() = %d, want 0", got)
	}
}

const oauthHandler = "listener.name.sasl.oauthbearer.sasl.server.callback.handler.class"
const oauthJwks = "listener.name.sasl.oauthbearer.sasl.oauthbearer.jwks.endpoint.url"
const oauthValidatorClass = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler"

// A recognized oauth validator class, with the JWKS endpoint present, is
// translated to the canonical token and the file is ACCEPTED.
func TestRecognizedHandler_WithParams_Accepted(t *testing.T) {
	src := "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
		oauthHandler + "=" + oauthValidatorClass + "\n" +
		oauthJwks + "=https://idp/realms/r/protocol/openid-connect/certs\n"
	out := renderProps(t, src)

	if !strings.Contains(out, "# original: "+oauthHandler+"="+oauthValidatorClass) &&
		!strings.Contains(out, "original: "+oauthHandler+"="+oauthValidatorClass) {
		t.Errorf("original class not kept as a comment:\n%s", out)
	}
	if !strings.Contains(out, oauthHandler+"=oauth\n") {
		t.Errorf("handler not rewritten to oauth:\n%s", out)
	}
	if !hasMarker(out, StatusAccepted) {
		t.Errorf("expected ACCEPTED:\n%s", out)
	}
}

// The same recognized oauth class, but with NO JWKS endpoint, is INVALID: the
// value is written active, with a RESOLVE-REQUIRED block naming the missing keys.
func TestRecognizedHandler_MissingParams_Invalid(t *testing.T) {
	src := "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
		oauthHandler + "=" + oauthValidatorClass + "\n"
	out := renderProps(t, src)

	if !strings.Contains(out, oauthHandler+"=oauth\n") {
		t.Errorf("handler not written active as oauth:\n%s", out)
	}
	if !strings.Contains(out, "params are missing") || !strings.Contains(out, oauthJwks) {
		t.Errorf("missing-params block not emitted with the jwks key:\n%s", out)
	}
	if !hasMarker(out, StatusInvalid) {
		t.Errorf("expected INVALID:\n%s", out)
	}
}

// An unrecognized custom handler class is INVALID, with no active value and a
// name-derived guess offered (commented).
func TestUnrecognizedHandler_Invalid(t *testing.T) {
	src := "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
		"listener.name.sasl.plain.sasl.server.callback.handler.class=com.acme.LdapPlainServerCallbackHandler\n"
	out := renderProps(t, src)

	if !strings.Contains(out, "RESOLVE-REQUIRED") {
		t.Errorf("custom class not flagged:\n%s", out)
	}
	if !strings.Contains(out, "#listener.name.sasl.plain.sasl.server.callback.handler.class=ldap") {
		t.Errorf("ldap guess not offered:\n%s", out)
	}
	if !hasMarker(out, StatusInvalid) {
		t.Errorf("expected INVALID:\n%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "#") &&
			strings.HasPrefix(line, "listener.name.sasl.plain.sasl.server.callback.handler.class=") {
			t.Errorf("custom class produced an active value: %q", line)
		}
	}
}

// Idempotency: a file already carrying canonical values (oauth + jwks, standard)
// is ACCEPTED, and re-running the tool on the output keeps it ACCEPTED and stable.
func TestIdempotent_CanonicalValues(t *testing.T) {
	src := "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
		oauthHandler + "=oauth\n" +
		oauthJwks + "=https://idp/certs\n" +
		"authorizer.class.name=standard\n"
	out1 := renderProps(t, src)
	if !hasMarker(out1, StatusAccepted) {
		t.Fatalf("canonical values not ACCEPTED:\n%s", out1)
	}
	if !strings.Contains(out1, oauthHandler+"=oauth\n") || !strings.Contains(out1, "authorizer.class.name=standard\n") {
		t.Errorf("canonical values not preserved:\n%s", out1)
	}
	// Re-run on the generated output: still ACCEPTED.
	out2 := renderProps(t, out1)
	if !hasMarker(out2, StatusAccepted) {
		t.Errorf("re-run not ACCEPTED (not idempotent):\n%s", out2)
	}
}
