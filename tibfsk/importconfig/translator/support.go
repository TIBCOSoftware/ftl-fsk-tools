/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import (
	"fmt"
	"io"
	"strings"
)

// This file is the canonical, line-by-line account of how tibftlimportconfig treats
// each Kafka listener/security broker property. It is the single source of truth:
// the --list-properties flag prints it, and the README support section is
// generated from it, so the documentation cannot drift from the tool.
//
// Property names are from the public Apache Kafka broker configuration reference.

// Disposition is how the tool treats one property. The set is deliberately small
// and each value answers a different question than "accepted: true/false", because
// some properties (the callback handler classes) cannot be answered that way.
type Disposition string

const (
	// DispAccept: honored as written and passed through to the FSK listener.
	DispAccept Disposition = "accept"
	// DispTranslate: accepted, but the VALUE is rewritten to a FSK form (e.g. a
	// handler class becomes a backend name).
	DispTranslate Disposition = "translate"
	// DispDepends: cannot be accepted or rejected statically -- it depends on the
	// value. The explanation states the rule (this is the handler-class case).
	DispDepends Disposition = "depends"
	// DispNotApplicable: a Kafka-internal concern that FTL handles natively, so the
	// property is ignored (e.g. inter-broker/controller listeners, KRaft bootstrap).
	DispNotApplicable Disposition = "not-applicable"
	// DispUnsupported: recognized but not implemented by FSK; rejected or ignored
	// with a warning (e.g. Kerberos, delegation tokens, custom Java plug-in classes).
	DispUnsupported Disposition = "unsupported"
)

// PropSupport is one property's line in the account.
type PropSupport struct {
	Property    string // exact Kafka property key, or a "<...>" pattern for a family
	Disp        Disposition
	Explanation string // plain-language reason; carries the comment for DispDepends
}

// propSections groups the account for readable output. The grouping is presentation
// only; every property stands on its own line with its own disposition.
var propSections = []struct {
	Name  string
	Props []PropSupport
}{
	{
		Name: "Listener layout",
		Props: []PropSupport{
			{"listeners", DispAccept,
				"The NAME://host:port addresses the broker binds. FSK binds the client listeners."},
			{"advertised.listeners", DispAccept,
				"The addresses clients are told to connect to. Passed through unchanged."},
			{"listener.security.protocol.map", DispAccept,
				"Maps each listener name to its security protocol (PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL)."},
			{"listener.name.<listener>.<property>", DispAccept,
				"Per-listener override of any property below. Honored for the listener it names."},
			{"inter.broker.listener.name", DispNotApplicable,
				"Inter-broker traffic uses the FTL servers' own connections, not a Kafka listener. Filtered out, not bound -- unless it is the only client-facing listener, in which case it is kept and bound for Kafka clients."},
			{"controller.listener.names", DispNotApplicable,
				"The controller quorum is FTL-native (not KRaft over a Kafka listener). Filtered out."},
			{"control.plane.listener.name", DispNotApplicable,
				"Control-plane traffic is carried by the FTL servers' own connections. Ignored."},
			{"sasl.mechanism.inter.broker.protocol", DispNotApplicable,
				"The SASL mechanism for inter-broker traffic. Inter-broker traffic uses the FTL servers' own connections, not a Kafka SASL listener, so it is ignored."},
			{"sasl.mechanism.controller.protocol", DispNotApplicable,
				"The SASL mechanism for controller traffic. The controller quorum is FTL-native, so it is ignored."},
		},
	},
	{
		Name: "TLS (server certificate and client-cert verification)",
		Props: []PropSupport{
			{"ssl.keystore.location", DispAccept,
				"Path to the listener's server certificate/key used to terminate TLS on the Kafka listener."},
			{"ssl.keystore.password", DispAccept,
				"Password for the server keystore. Honored."},
			{"ssl.keystore.key", DispAccept,
				"The server private key given inline as PEM (instead of a keystore file). Honored."},
			{"ssl.keystore.certificate.chain", DispAccept,
				"The server certificate chain given inline as PEM. Honored."},
			{"ssl.key.password", DispAccept,
				"Passphrase for the server private key. Honored."},
			{"ssl.keystore.type", DispTranslate,
				"PEM is accepted directly. A Java keystore (JKS or PKCS12) is rewritten to PEM, along with " +
					"ssl.keystore.location, and the generated file carries the keytool/openssl commands that " +
					"produce the .pem -- run those before starting tibftlserver."},
			{"ssl.truststore.location", DispAccept,
				"CA bundle used to verify inbound client certificates when the listener is mutual-TLS. Honored."},
			{"ssl.truststore.password", DispAccept,
				"Password for the truststore. Honored."},
			{"ssl.truststore.certificates", DispAccept,
				"Trusted CA certificates given inline as PEM. Honored."},
			{"ssl.truststore.type", DispTranslate,
				"Same as ssl.keystore.type: PEM is accepted; a Java truststore (JKS or PKCS12) is rewritten " +
					"to PEM, with the conversion commands in the generated file."},
			{"ssl.client.auth", DispAccept,
				"none/requested/required. 'required' makes the listener mutual-TLS (the client certificate is verified)."},
			{"ssl.enabled.protocols", DispAccept,
				"The TLS versions the listener allows (e.g. TLSv1.2, TLSv1.3). Honored."},
			{"ssl.protocol", DispAccept,
				"The default TLS protocol version. Honored."},
			{"ssl.cipher.suites", DispAccept,
				"The allowed TLS cipher suites. Honored; the suite names are applied to FSK's OpenSSL TLS stack."},
			{"ssl.principal.mapping.rules", DispDepends,
				"Maps a client certificate's subject DN to a principal. Simple CN extraction is honored; " +
					"complex multi-rule sets are not fully evaluated and need review."},
			{"ssl.endpoint.identification.algorithm", DispNotApplicable,
				"Hostname verification done by a TLS client against a server's certificate. FSK's inbound listener " +
					"verifies client certificates by CA chain, not by hostname, so this does not apply."},
			{"ssl.provider", DispUnsupported,
				"Names a Java JSSE security provider. FSK uses OpenSSL, so a named Java provider has no effect."},
			{"ssl.secure.random.implementation", DispUnsupported,
				"Selects a Java SecureRandom implementation. FSK uses the OpenSSL RNG; not applicable."},
			{"ssl.keymanager.algorithm", DispUnsupported,
				"A Java KeyManager algorithm. FSK does not use the Java TLS stack; not applicable."},
			{"ssl.trustmanager.algorithm", DispUnsupported,
				"A Java TrustManager algorithm. FSK does not use the Java TLS stack; not applicable."},
			{"ssl.engine.factory.class", DispUnsupported,
				"A custom Java SSL engine. FSK terminates TLS with OpenSSL and cannot load a Java class."},
			{"ssl.allow.dn.changes", DispNotApplicable,
				"Whether a certificate's DN may change across a re-authentication. FSK does not implement this policy."},
			{"ssl.allow.san.changes", DispNotApplicable,
				"Whether a certificate's SANs may change across a re-authentication. FSK does not implement this policy."},
		},
	},
	{
		Name: "SASL authentication",
		Props: []PropSupport{
			{"sasl.enabled.mechanisms", DispDepends,
				"The SASL mechanisms the listener offers. PLAIN and OAUTHBEARER are supported; SCRAM and " +
					"GSSAPI are not, and naming one on a listener that speaks SASL is flagged RESOLVE-REQUIRED. " +
					"On a listener that does not speak SASL the setting offers nothing, so it is commented out " +
					"rather than flagged."},
			{"sasl.jaas.config", DispDepends,
				"Its meaning depends on the login module it names. PlainLoginModule: the inline " +
					"user_<name>=\"<password>\" entries are read as credentials. OAuthBearerLoginModule: " +
					"selects OAUTHBEARER (OAuth2); the token validation is configured by the sasl.oauthbearer.* " +
					"properties and the server callback handler class, not by inline users. ScramLoginModule " +
					"and Kerberos (Krb5LoginModule) are unsupported, and any custom or delegated module is not run."},
			{"sasl.server.callback.handler.class", DispDepends,
				"A Java class. A recognized standard class is translated to a FSK backend (e.g. oauth, inline); " +
					"an unrecognized custom class cannot be run and is flagged RESOLVE-REQUIRED for the operator."},
			{"sasl.client.callback.handler.class", DispNotApplicable,
				"A callback handler used by a SASL client, not by the broker accepting connections. Ignored."},
			{"sasl.login.callback.handler.class", DispNotApplicable,
				"A callback handler used during a SASL client's login, not by the broker. Ignored."},
			{"sasl.login.class", DispNotApplicable,
				"A custom client-side Login class. Not used by the broker's inbound listener; ignored."},
			{"sasl.server.max.receive.size", DispNotApplicable,
				"Maximum size of a SASL handshake message buffer in the Java broker. FSK manages its own buffers."},
			{"sasl.kerberos.*", DispUnsupported,
				"GSSAPI/Kerberos is not implemented in FSK. The whole family (sasl.kerberos.service.name, kinit.cmd, " +
					"ticket.renew.window.factor, ticket.renew.jitter, min.time.before.relogin, principal.to.local.rules) is unsupported."},
			{"sasl.login.refresh.*", DispNotApplicable,
				"Token-refresh timing for a SASL client maintaining its own login (window.factor, window.jitter, " +
					"min.period.seconds, buffer.seconds). FSK's inbound listener validates tokens; it does not run a client login."},
			{"sasl.login.connect.timeout.ms / read.timeout.ms / retry.backoff[.max].ms", DispNotApplicable,
				"Network tuning for a SASL client contacting a token endpoint to obtain a login. Not used by the broker's listener."},
			{"connections.max.reauth.ms", DispAccept,
				"SASL re-authentication deadline (KIP-368). Honored; the connection must re-authenticate before it elapses."},
		},
	},
	{
		Name: "SASL OAUTHBEARER token validation",
		Props: []PropSupport{
			{"sasl.oauthbearer.jwks.endpoint.url", DispAccept,
				"The JWKS endpoint whose keys validate inbound OAUTHBEARER token signatures. Honored by the token validator."},
			{"sasl.oauthbearer.expected.issuer", DispAccept,
				"The required 'iss' claim. A token whose issuer differs is rejected. Honored."},
			{"sasl.oauthbearer.expected.audience", DispAccept,
				"The required 'aud' claim. A token whose audience does not match is rejected. Honored."},
			{"sasl.oauthbearer.sub.claim.name", DispAccept,
				"Which token claim provides the principal (default 'sub'). Honored; that claim becomes the Kafka principal."},
			{"sasl.oauthbearer.clock.skew.seconds", DispAccept,
				"Allowed clock skew when checking token expiry/not-before. Honored."},
			{"sasl.oauthbearer.jwks.endpoint.refresh.ms", DispAccept,
				"How often the validator refreshes the JWKS keys. Honored."},
			{"sasl.oauthbearer.jwks.endpoint.retry.backoff[.max].ms", DispAccept,
				"Retry backoff for fetching the JWKS keys. Honored."},
			{"sasl.oauthbearer.scope.claim.name", DispNotApplicable,
				"Which claim carries OAuth scopes. FSK authorization is principal-based (ACLs keyed on the principal); " +
					"OAuth scopes are not mapped to authorization, so this is ignored."},
			{"sasl.oauthbearer.token.endpoint.url", DispNotApplicable,
				"The IdP token endpoint used to OBTAIN a token, by a producer/consumer or by a broker acting as " +
					"an OAuth client for inter-broker auth. The FSK listener only validates the token a client " +
					"presents (against jwks.endpoint.url); it never acquires one, and inter-broker auth uses the " +
					"FTL servers' own connections, not a Kafka SASL listener. So nothing on the FSK Kafka listeners uses it."},
			{"sasl.oauthbearer.unsecured.*", DispDepends,
				"Options for the unsecured (no-signature) validator used in testing. The OAuthBearerUnsecuredValidatorCallbackHandler " +
					"is recognized as a backend, but an unsecured token is for testing only and must not be relied on in production."},
		},
	},
	{
		Name: "Authorization (ACLs)",
		Props: []PropSupport{
			{"authorizer.class.name", DispDepends,
				"The built-in ACL authorizers are recognized: org.apache.kafka.metadata.authorizer.StandardAuthorizer " +
					"(KRaft mode) and kafka.security.authorizer.AclAuthorizer (ZooKeeper mode). When one of these is named, " +
					"the tool rewrites the value to 'KofAuthorizer' and FSK enforces the same ACL model: default-deny, super.users " +
					"bypass, and per-principal allow rules. Any other authorizer is a custom Java class FSK cannot run, so it " +
					"is flagged RESOLVE-REQUIRED."},
			{"super.users", DispAccept,
				"Principals that bypass the ACL table. Canonicalized (User: prefix stripped) and installed as the bypass list."},
			{"allow.everyone.if.no.acl.found", DispAccept,
				"When true, an operation with no matching ACL is allowed instead of denied. Honored; it changes the default policy."},
			{"principal.builder.class", DispUnsupported,
				"A custom Java class that derives the principal. FSK derives the principal natively (cert CN / SASL username)."},
			{"security.providers", DispUnsupported,
				"Custom Java security provider classes loaded by the broker. FSK cannot load Java provider classes."},
			{"early.start.listeners", DispNotApplicable,
				"Which listeners start before the KRaft metadata is caught up. The controller/quorum is FTL-native, so this does not apply."},
		},
	},
	{
		Name: "Connection limits",
		Props: []PropSupport{
			{"max.connections", DispAccept,
				"Maximum concurrent connections, applied per listener. Honored; a new connection past the limit is refused."},
			{"max.connections.per.ip", DispUnsupported,
				"A per-source-IP connection cap. Not implemented in this release; the setting is ignored."},
			{"max.connections.per.ip.overrides", DispUnsupported,
				"Per-IP overrides of the per-IP cap. Not implemented in this release; ignored."},
			{"connection.failed.authentication.delay.ms", DispUnsupported,
				"A delay before closing a connection that failed authentication. Not implemented; a failed auth is closed immediately."},
			{"connections.max.idle.ms", DispNotApplicable,
				"Idle-connection timeout in the Java broker. FSK manages connection lifetime through the FTL servers."},
		},
	},
	{
		Name: "Delegation tokens",
		Props: []PropSupport{
			{"delegation.token.*", DispUnsupported,
				"Delegation tokens are not implemented in FSK. The whole family (delegation.token.secret.key, " +
					"max.lifetime.ms, expiry.time.ms, expiry.check.interval.ms) is unsupported."},
		},
	},
}

// ANSI color codes for the --list-properties terminal output. They are emitted
// only when color is enabled. The generated kof.broker.properties is NEVER colored
// -- it is a real config file the ftlserver parses and the operator edits, so
// escape codes would corrupt it.
const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	ansiDim    = "\033[2m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
)

// dispColor maps a disposition to its color: accept green, depends yellow,
// not-applicable dim, unsupported (not accepted) red.
func dispColor(d Disposition) string {
	switch d {
	case DispAccept:
		return ansiGreen
	case DispDepends:
		return ansiYellow
	case DispNotApplicable:
		return ansiDim
	case DispUnsupported:
		return ansiRed
	default:
		return ""
	}
}

// WriteSupportList prints the line-by-line property account to w. Used by
// --list-properties and by the README generator. When color is true, the
// disposition tags, property names, and section headers are ANSI-colored;
// unsupported is red. Pass false for non-terminal output (pipes, the README, tests).
func WriteSupportList(w io.Writer, color bool) {
	paint := func(code, s string) string {
		if !color || code == "" {
			return s
		}
		return code + s + ansiReset
	}

	// Split across two lines: the property explanations below wrap at 76 columns, so a
	// single-line title would overhang the whole table.
	fmt.Fprintln(w, paint(ansiBold,
		"tibftlimportconfig -- Apache Kafka listener/security property support"))
	fmt.Fprintln(w, paint(ansiBold,
		"                      in TIBCO FTL(R) Service for Kafka (FSK)"))
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Disposition legend:")
	legend := []struct{ word, desc string }{
		{"accept", "honored as written, passed through to the FSK listener"},
		{"depends", "cannot be decided statically; the line states the rule"},
		{"not-applicable", "Kafka-internal; FTL handles it natively, so it is ignored"},
		{"unsupported", "not implemented by FSK; rejected or ignored with a warning"},
	}
	for _, l := range legend {
		pad := strings.Repeat(" ", 14-len(l.word))
		fmt.Fprintf(w, "  %s%s %s\n", paint(dispColor(Disposition(l.word)), l.word), pad, l.desc)
		if l.word == "depends" {
			fmt.Fprintln(w, "                 (e.g. a recognized class is rewritten, a custom class is flagged)")
		}
	}
	fmt.Fprintln(w)

	for _, sec := range propSections {
		fmt.Fprintln(w, paint(ansiBold+ansiCyan, "== "+sec.Name+" =="))
		for _, p := range sec.Props {
			fmt.Fprintf(w, "%s  [%s]\n", paint(ansiBold, p.Property), paint(dispColor(p.Disp), string(p.Disp)))
			for _, line := range wrapText(p.Explanation, 76) {
				fmt.Fprintf(w, "    %s\n", line)
			}
		}
		fmt.Fprintln(w)
	}
}

// wrapText wraps s to at most width columns on word boundaries.
func wrapText(s string, width int) []string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return []string{""}
	}
	var lines []string
	cur := words[0]
	for _, w := range words[1:] {
		if len(cur)+1+len(w) > width {
			lines = append(lines, cur)
			cur = w
		} else {
			cur += " " + w
		}
	}
	return append(lines, cur)
}
