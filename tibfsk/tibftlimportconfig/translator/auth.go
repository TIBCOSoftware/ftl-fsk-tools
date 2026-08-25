package translator

import (
	"sort"
	"strings"
)

// AuthBackend is the canonical credential backend a SASL listener resolves to.
// It is what the kof-ready config records instead of a Java handler class.
//
// CANONICAL VOCABULARY: these strings are the canonical kof.broker.properties tokens the
// pserver consumes. The single source of truth is the C header
// hydra/header/private/kof/kofcanonical.h (KOF_CANON_AUTH_*); keep these in sync with it.
type AuthBackend string

const (
	BackendInline AuthBackend = "inline" // KOF_CANON_AUTH_INLINE -- inline user_X=Y entries in the jaas config
	BackendFile   AuthBackend = "file"   // KOF_CANON_AUTH_FILE   -- realm file provider
	BackendOauth  AuthBackend = "oauth"  // KOF_CANON_AUTH_OAUTH  -- realm oauth2 provider (OAUTHBEARER)
	BackendNone   AuthBackend = ""       // no configured credential source
)

// recognizedHandlers maps a known Kafka SASL server callback handler class to the
// backend it resolves to. Recognition is by exact class name -- we never run the
// class. Only classes whose behavior FSK can reproduce natively are listed;
// anything else is unrecognized and must be resolved by the operator.
var recognizedHandlers = map[string]AuthBackend{
	"org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler":                                   BackendInline,
	"org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler":                              BackendOauth,
	"org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler": BackendOauth,
}

// canonicalBackends maps the backend tokens the tool writes (and the operator
// edits in) back to a backend. This is what makes the tool idempotent: on a re-run
// it sees its own "...handler.class=oauth" and knows it is already resolved.
var canonicalBackends = map[string]AuthBackend{
	string(BackendInline): BackendInline,
	string(BackendFile):   BackendFile,
	string(BackendOauth):  BackendOauth,
}

// backendFromValue returns the backend if value is already a canonical token.
func backendFromValue(value string) (AuthBackend, bool) {
	be, ok := canonicalBackends[strings.ToLower(strings.TrimSpace(value))]
	return be, ok
}

// guessBackendFromName makes a best-effort guess of the backend from an
// unrecognized class name. It is a hint only -- never applied without operator
// confirmation, since the name can be anything.
func guessBackendFromName(class string) AuthBackend {
	l := strings.ToLower(class)
	switch {
	case strings.Contains(l, "oauth"), strings.Contains(l, "oidc"):
		return BackendOauth
	default:
		return BackendNone
	}
}

// ListenerAuth is the resolved credential source for one SASL listener.
type ListenerAuth struct {
	Backend         AuthBackend // resolved backend (BackendNone when no source / unrecognized)
	InlineUsers     []string    // user names, when Backend == BackendInline
	InlinePasswords []string    // index-aligned with InlineUsers
	HandlerClass    string      // the configured callback handler class, if any
	Recognized      bool        // class recognized (or inline/no-source), i.e. resolvable without operator input
	Guess           AuthBackend // name-derived guess for an unrecognized class
}

// resolveListenerAuth resolves the credential source for a SASL listener from its
// jaas config and optional callback handler class:
//   - a recognized class maps to a backend;
//   - an unrecognized class is flagged (Recognized=false) with a name guess --
//     FSK cannot run custom Java, so the operator must choose the backend;
//   - no class falls back to inline user_X=Y entries;
//   - nothing configured is BackendNone (the listener authenticates no one).
func resolveListenerAuth(jaas, handlerClass string) ListenerAuth {
	a := ListenerAuth{HandlerClass: handlerClass}

	if handlerClass != "" {
		// The tool's own resolved output (and operator edits) carry a canonical
		// backend token instead of a Java class; recognize it so a re-run over
		// generated output resolves the same way.
		if be, ok := backendFromValue(handlerClass); ok {
			a.Backend = be
			a.Recognized = true
			if be == BackendInline {
				a.InlineUsers, a.InlinePasswords = parsePlainJaasUsers(jaas)
			}
			return a
		}
		if be, ok := recognizedHandlers[handlerClass]; ok {
			a.Backend = be
			a.Recognized = true
			if be == BackendInline {
				a.InlineUsers, a.InlinePasswords = parsePlainJaasUsers(jaas)
			}
			return a
		}
		// Unrecognized custom class: cannot run it. Flag and offer a name guess.
		a.Recognized = false
		a.Guess = guessBackendFromName(handlerClass)
		return a
	}

	// No callback class: the default PLAIN behavior is the inline jaas users.
	if users, passwords := parsePlainJaasUsers(jaas); len(users) > 0 {
		a.Backend = BackendInline
		a.Recognized = true
		a.InlineUsers = users
		a.InlinePasswords = passwords
		return a
	}

	// Nothing configured: recognized as "no source", not an unknown class.
	a.Backend = BackendNone
	a.Recognized = true
	return a
}

// supportedMechanisms are the SASL mechanisms FSK can serve on a listener.
var supportedMechanisms = map[string]bool{"PLAIN": true, "OAUTHBEARER": true}

// isMechanismsKey reports a sasl.enabled.mechanisms key (global or per-listener).
func isMechanismsKey(key string) bool {
	kl := strings.ToLower(key)
	return kl == "sasl.enabled.mechanisms" || strings.HasSuffix(kl, ".sasl.enabled.mechanisms")
}

// mechanismsSupport splits a sasl.enabled.mechanisms value into the mechanisms FSK
// supports and the ones it does not.
func mechanismsSupport(value string) (supported, unsupported []string) {
	for _, m := range splitCSV(value) {
		mu := strings.ToUpper(strings.TrimSpace(m))
		if mu == "" {
			continue
		}
		if supportedMechanisms[mu] {
			supported = append(supported, mu)
		} else {
			unsupported = append(unsupported, mu)
		}
	}
	return supported, unsupported
}

// mechanismsUnservable reports a listener FSK cannot serve at all: it lists SASL
// mechanisms but none FSK supports (e.g. SCRAM-only or GSSAPI-only).
func mechanismsUnservable(value string) bool {
	sup, unsup := mechanismsSupport(value)
	return len(sup) == 0 && len(unsup) > 0
}

// recognizedAuthorizers are Kafka's built-in ACL authorizer classes. FSK
// reproduces their ACL model natively. StandardAuthorizer is the KRaft-mode
// authorizer; AclAuthorizer is the ZooKeeper-mode one; SimpleAclAuthorizer is the
// older deprecated name. All enforce the same rules, so all map to authorizerCanonical.
var recognizedAuthorizers = map[string]bool{
	"org.apache.kafka.metadata.authorizer.StandardAuthorizer": true,
	"kafka.security.authorizer.AclAuthorizer":                 true,
	"kafka.security.auth.SimpleAclAuthorizer":                 true,
}

// authorizerCanonical is the value the tool writes for authorizer.class.name when
// a recognized authorizer is named: FSK turns on its native ACL enforcement.
//
// CANONICAL VOCABULARY: this token is the canonical kof.broker.properties value the pserver
// consumes. The single source of truth is the C header hydra/header/private/kof/kofcanonical.h
// (KOF_CANON_AUTHORIZER_STANDARD); keep in sync with it.
const authorizerCanonical = "KofAuthorizer"

// recognizedAuthorizerNames returns the recognized authorizer classes in sorted
// order, for listing in messages.
func recognizedAuthorizerNames() []string {
	names := make([]string, 0, len(recognizedAuthorizers))
	for n := range recognizedAuthorizers {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// isAuthorizerKey reports whether a key selects the broker's authorizer
// implementation -- the other property whose value is an opaque Java class.
func isAuthorizerKey(key string) bool {
	return strings.EqualFold(key, "authorizer.class.name")
}

// resolveAuthorizer recognizes Kafka's built-in ACL authorizers (the ones FSK can
// reproduce) and returns the canonical value to write for them. Any other class is
// a custom Java authorizer FSK cannot run.
func resolveAuthorizer(value string) (recognized bool, canonical string) {
	v := strings.TrimSpace(value)
	// Already the canonical token (operator edit or a prior run) -- idempotent.
	if strings.EqualFold(v, authorizerCanonical) {
		return true, authorizerCanonical
	}
	if recognizedAuthorizers[v] {
		return true, authorizerCanonical
	}
	return false, ""
}

// handlerClassSuffix is the Kafka key suffix that carries a SASL server callback
// handler class. It appears as the global "sasl.server.callback.handler.class" or
// per-listener/per-mechanism "listener.name.<l>[.<mech>].sasl.server.callback.handler.class".
const handlerClassSuffix = ".sasl.server.callback.handler.class"

// isHandlerClassKey reports whether a property key carries a SASL callback handler
// class -- the one Kafka key whose value is an opaque Java class name.
func isHandlerClassKey(key string) bool {
	kl := strings.ToLower(key)
	return kl == "sasl.server.callback.handler.class" || strings.HasSuffix(kl, handlerClassSuffix)
}

// jaasForHandlerKey returns the jaas config that pairs with a handler-class key.
// The jaas key has the same prefix with the handler suffix swapped for
// ".sasl.jaas.config"; it falls back to the global "sasl.jaas.config".
func jaasForHandlerKey(cfg *BrokerConfig, handlerKey string) string {
	kl := strings.ToLower(handlerKey)
	if kl != "sasl.server.callback.handler.class" {
		jaasKey := handlerKey[:len(handlerKey)-len(handlerClassSuffix)] + ".sasl.jaas.config"
		if v, ok := cfg.Settings[jaasKey]; ok {
			return v
		}
	}
	return cfg.Settings["sasl.jaas.config"]
}

// HandlerOutcome is the full resolution of a SASL handler-class key: which backend
// it resolves to, whether anything is left for the operator, and what.
type HandlerOutcome struct {
	Backend   AuthBackend // backend the value resolves to (BackendNone if none)
	Active    string      // the active value to write (a canonical token), or ""
	Resolved  bool        // true: nothing left for the operator to do
	FromClass bool        // value was a Java class the tool translated (keep original as a comment)
	NeedsPick bool        // unrecognized custom class -> operator must choose a backend
	Guess     AuthBackend // name-derived guess for an unrecognized class
}

// paramHint is one backend param to show the operator: the key and a short note.
type paramHint struct {
	Key  string
	Note string
}

// resolveHandler resolves a handler-class key end to end: it recognizes a canonical
// token (idempotent re-run), translates a recognized Java class, flags an
// unrecognized custom class, and -- once a backend is known -- checks that the
// backend's params are present (oauth needs a JWKS endpoint, inline needs jaas
// users). file is configured in the realm, which this file cannot see, so
// it is treated as resolved.
func resolveHandler(cfg *BrokerConfig, handlerKey string) HandlerOutcome {
	value := cfg.Settings[handlerKey]
	jaas := jaasForHandlerKey(cfg, handlerKey)

	// Already a canonical token (operator edit or a prior run).
	if be, ok := backendFromValue(value); ok {
		return checkBackendParams(cfg, handlerKey, be, string(be), false, jaas)
	}
	// A recognized Java class -> translate to its backend.
	if be, ok := recognizedHandlers[value]; ok {
		return checkBackendParams(cfg, handlerKey, be, string(be), true, jaas)
	}
	// An unrecognized custom Java class -> the operator must choose a backend.
	return HandlerOutcome{
		FromClass: true,
		NeedsPick: true,
		Guess:     guessBackendFromName(value),
	}
}

// checkBackendParams completes a HandlerOutcome once the backend is known, marking
// it unresolved when the backend's required params are missing. The gate is the
// minimum a backend needs to work: oauth needs a JWKS endpoint, inline needs jaas
// users. file is configured in the realm, so it is treated as resolved.
func checkBackendParams(cfg *BrokerConfig, handlerKey string, be AuthBackend, active string, fromClass bool, jaas string) HandlerOutcome {
	o := HandlerOutcome{Backend: be, Active: active, FromClass: fromClass}
	switch be {
	case BackendOauth:
		if !oauthJwksPresent(cfg, handlerKey) {
			return o
		}
	case BackendInline:
		if users, _ := parsePlainJaasUsers(jaas); len(users) == 0 {
			return o
		}
	}
	o.Resolved = true
	return o
}

// handlerKeyPrefix is the handler-class key with the handler suffix stripped, i.e.
// "listener.name.<l>.<mech>" (or "" for the global key).
func handlerKeyPrefix(handlerKey string) string {
	if strings.EqualFold(handlerKey, "sasl.server.callback.handler.class") {
		return ""
	}
	return handlerKey[:len(handlerKey)-len(handlerClassSuffix)]
}

// jaasConfigKey is the jaas.config key paired with a handler-class key.
func jaasConfigKey(handlerKey string) string {
	if p := handlerKeyPrefix(handlerKey); p != "" {
		return p + ".sasl.jaas.config"
	}
	return "sasl.jaas.config"
}

// oauthKey builds an oauth param key (jwks.endpoint.url / expected.issuer /
// expected.audience) for a handler key's listener, or the global key.
func oauthKey(handlerKey, suffix string) string {
	if p := handlerKeyPrefix(handlerKey); p != "" {
		return p + ".sasl.oauthbearer." + suffix
	}
	return "sasl.oauthbearer." + suffix
}

// oauthJwksPresent reports whether a JWKS endpoint is configured for the listener
// (per-listener or broker-wide). FSK validates OAUTHBEARER tokens against it.
func oauthJwksPresent(cfg *BrokerConfig, handlerKey string) bool {
	return cfg.Settings[oauthKey(handlerKey, "jwks.endpoint.url")] != "" ||
		cfg.Settings["sasl.oauthbearer.jwks.endpoint.url"] != ""
}

// oauthParams is the OAUTHBEARER validation family FSK uses, in the order to show
// them. jwks.endpoint.url is required (the gate); the rest are recommended or have
// defaults. Listing the family lets the operator set them in one pass.
func oauthParams(handlerKey string) []paramHint {
	return []paramHint{
		{oauthKey(handlerKey, "jwks.endpoint.url"), "required -- JWKS endpoint; FSK validates token signatures against it"},
		{oauthKey(handlerKey, "expected.issuer"), "recommended -- reject a token whose iss claim differs"},
		{oauthKey(handlerKey, "expected.audience"), "recommended -- reject a token whose aud claim does not match"},
		{oauthKey(handlerKey, "sub.claim.name"), "optional -- which claim is the principal (default: sub)"},
		{oauthKey(handlerKey, "clock.skew.seconds"), "optional -- allowed skew checking exp/nbf"},
	}
}

// backendParamHints returns the param lines to show for a chosen backend: the
// oauth family, the inline jaas users, or a realm-provider note for file.
func backendParamHints(handlerKey string, be AuthBackend) []paramHint {
	switch be {
	case BackendOauth:
		return oauthParams(handlerKey)
	case BackendInline:
		return []paramHint{{jaasConfigKey(handlerKey),
			"a PlainLoginModule with user_<name>=\"<password>\" entries"}}
	case BackendFile:
		return []paramHint{{"", "configure a file provider in the realm (auth.providers=file:<path>)"}}
	}
	return nil
}

// parsePlainJaasUsers extracts inline user_<name>="<password>" entries from a
// SASL/PLAIN jaas config value (Kafka form: `<ModuleClass> <flag> user_a="x" ...;`).
// Quote-aware so a password may contain spaces. The module's own
// username=/password= and every other option are ignored. Returns index-aligned
// users/passwords. This is the same grammar the runtime parser uses.
func parsePlainJaasUsers(jaas string) (users, passwords []string) {
	isWs := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
	i, n := 0, len(jaas)

	for i < n {
		for i < n && isWs(jaas[i]) {
			i++
		}
		if i >= n || jaas[i] == ';' {
			break
		}

		ks := i
		for i < n && jaas[i] != '=' && !isWs(jaas[i]) && jaas[i] != ';' {
			i++
		}
		key := jaas[ks:i]

		if i >= n || jaas[i] != '=' {
			continue // a bare token (module class or flag)
		}
		i++

		var val string
		if i < n && jaas[i] == '"' {
			i++
			vs := i
			for i < n && jaas[i] != '"' {
				i++
			}
			val = jaas[vs:i]
			if i < n {
				i++
			}
		} else {
			vs := i
			for i < n && !isWs(jaas[i]) && jaas[i] != ';' {
				i++
			}
			val = jaas[vs:i]
		}

		if strings.HasPrefix(key, "user_") && len(key) > len("user_") {
			users = append(users, key[len("user_"):])
			passwords = append(passwords, val)
		}
	}
	return users, passwords
}
