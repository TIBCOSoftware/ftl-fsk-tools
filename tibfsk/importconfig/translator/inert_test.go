/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import (
	"strings"
	"testing"
)

// The three settings a plain, unsecured broker reports at their Apache Kafka defaults. None
// of them is doing anything, so none of them may make the file INVALID -- this is
// exactly the case that made `tibftlimportconfig -from-brokers localhost:9092` fail
// against a broker with no TLS and no auth.
func TestInertSecuritySettingsAreAcceptedAndCommentedOut(t *testing.T) {
	cases := []struct {
		name    string
		src     string
		key     string
		value   string
		wantWhy string
	}{
		{
			name:    "empty authorizer",
			src:     "node.id=1\nlisteners=PLAINTEXT://0.0.0.0:9092\nauthorizer.class.name=\n",
			key:     "authorizer.class.name",
			wantWhy: "the value is empty",
		},
		{
			name:    "keystore type with no location",
			src:     "node.id=1\nlisteners=PLAINTEXT://0.0.0.0:9092\nssl.keystore.type=JKS\n",
			key:     "ssl.keystore.type",
			value:   "JKS",
			wantWhy: "no ssl.keystore.location is set",
		},
		{
			name:    "SASL mechanisms with no SASL listener",
			src:     "node.id=1\nlisteners=PLAINTEXT://0.0.0.0:9092\nsasl.enabled.mechanisms=GSSAPI\n",
			key:     "sasl.enabled.mechanisms",
			value:   "GSSAPI",
			wantWhy: "no listener uses SASL",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := parseSrc(t, c.src)
			if why := inertReason(cfg, c.key); !strings.Contains(why, c.wantWhy) {
				t.Errorf("inertReason = %q, want it to mention %q", why, c.wantWhy)
			}
			if got := configStatus(cfg); got != StatusAccepted {
				t.Errorf("status = %q, want ACCEPTED", got)
			}
			if got := Summarize(cfg).Total(); got != 0 {
				t.Errorf("Summarize().Total() = %d, want 0", got)
			}
			out := renderCfg(t, cfg)
			if !strings.Contains(out, "#"+c.key+"="+c.value+"\n") {
				t.Errorf("%s should be commented out, got:\n%s", c.key, out)
			}
			if strings.Contains(out, "\n"+c.key+"=") {
				t.Errorf("%s must not be written active, got:\n%s", c.key, out)
			}
		})
	}
}

// The negative controls. A wrong "inert" would silently drop a setting a real
// cluster depends on, so each of these must still be flagged.
func TestInertDoesNotSwallowSettingsInUse(t *testing.T) {
	cases := []struct {
		name string
		src  string
	}{
		{
			// GSSAPI on a listener that really does speak SASL: unservable, INVALID.
			name: "mechanisms with a SASL listener",
			src: "node.id=1\nlisteners=SASL://0.0.0.0:9092\n" +
				"listener.security.protocol.map=SASL:SASL_SSL\n" +
				"sasl.enabled.mechanisms=GSSAPI\n",
		},
		{
			// No listeners line to reason about: make no claim, keep flagging.
			name: "mechanisms with no parsable listeners",
			src:  "node.id=1\nsasl.enabled.mechanisms=GSSAPI\n",
		},
		{
			// Apache Kafka defaults a listener's protocol to its own name when no
			// listener.security.protocol.map names it.
			name: "mechanisms with an unmapped SASL_SSL listener",
			src:  "node.id=1\nlisteners=SASL_SSL://0.0.0.0:9092\nsasl.enabled.mechanisms=GSSAPI\n",
		},
		{
			name: "unrecognized authorizer class",
			src: "node.id=1\nlisteners=PLAINTEXT://0.0.0.0:9092\n" +
				"authorizer.class.name=com.acme.MyAuthorizer\n",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := parseSrc(t, c.src)
			if got := configStatus(cfg); got != StatusInvalid {
				t.Errorf("status = %q, want INVALID -- the setting is in use", got)
			}
			if got := Summarize(cfg).Total(); got != 1 {
				t.Errorf("Summarize().Total() = %d, want 1", got)
			}
		})
	}
}

// A per-listener keystore type with no location of its own falls back to the
// broker-wide location, the way Apache Kafka resolves it -- so it is in use, not inert.
func TestKeystoreLocationFallsBackAcrossListenerScope(t *testing.T) {
	cfg := parseSrc(t, "node.id=1\nlisteners=SSL://0.0.0.0:9093\n"+
		"ssl.keystore.location=/c/broker.keystore.pem\n"+
		"listener.name.ssl.ssl.keystore.type=PEM\n")
	if why := inertReason(cfg, "listener.name.ssl.ssl.keystore.type"); why != "" {
		t.Errorf("inertReason = %q, want \"\" -- it uses the broker-wide location", why)
	}
}

// A JKS that names a location is a translation, not a refusal: type and location are
// rewritten to the PEM form, the file is ACCEPTED, and the conversion commands ride
// along in the output for the operator to run.
func TestJavaKeystoreIsRewrittenToPEM(t *testing.T) {
	cfg := parseSrc(t, "node.id=1\nlisteners=SSL://0.0.0.0:9093\n"+
		"ssl.keystore.type=JKS\nssl.keystore.location=/c/broker.keystore.jks\n")

	if got := configStatus(cfg); got != StatusAccepted {
		t.Errorf("status = %q, want ACCEPTED", got)
	}
	if cfg.Settings["ssl.keystore.type"] != "PEM" {
		t.Errorf("type = %q, want PEM", cfg.Settings["ssl.keystore.type"])
	}
	if got := cfg.Settings["ssl.keystore.location"]; got != "/c/broker.keystore.pem" {
		t.Errorf("location = %q, want /c/broker.keystore.pem", got)
	}
	if len(cfg.KeystoreConversions) != 1 {
		t.Fatalf("KeystoreConversions = %+v, want 1", cfg.KeystoreConversions)
	}
	if len(PendingKeystores(cfg)) != 1 {
		t.Error("the .pem does not exist yet, so the conversion must still be pending")
	}

	out := renderCfg(t, cfg)
	for _, want := range []string{
		"\nssl.keystore.type=PEM\n",
		"\nssl.keystore.location=/c/broker.keystore.pem\n",
		"keytool -importkeystore -srckeystore /c/broker.keystore.jks",
		"openssl pkcs12 -in /c/broker.keystore.p12 -nodes -out /c/broker.keystore.pem",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, resolveBandOpen) {
		t.Errorf("a convertible keystore must not open a RESOLVE-REQUIRED block:\n%s", out)
	}
}

// A truststore is CA certs only, so its openssl step adds -nokeys.
func TestTruststoreConversionUsesNokeys(t *testing.T) {
	cfg := parseSrc(t, "node.id=1\nlisteners=SSL://0.0.0.0:9093\n"+
		"ssl.truststore.type=PKCS12\nssl.truststore.location=/c/kafka.truststore.p12\n")
	out := renderCfg(t, cfg)
	if !strings.Contains(out, "openssl pkcs12 -in /c/kafka.truststore.p12 -nodes -nokeys -out /c/kafka.truststore.pem") {
		t.Errorf("truststore conversion should pass -nokeys and skip keytool:\n%s", out)
	}
	if strings.Contains(out, "keytool") {
		t.Errorf("a PKCS12 source needs no keytool step:\n%s", out)
	}
}

// Without a source file there are no line numbers, so the provenance tag has to stay
// off rather than printing "[source:0]".
func TestSourceNoteOmittedWithoutLineNumbers(t *testing.T) {
	if got := sourceNote(0); got != "" {
		t.Errorf("sourceNote(0) = %q, want \"\"", got)
	}
	if got := sourceNote(12); got != "[source:12] " {
		t.Errorf("sourceNote(12) = %q", got)
	}
}
