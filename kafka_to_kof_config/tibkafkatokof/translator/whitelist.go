package translator

import "strings"

// section1Keys is the set of base keys (listener.name.* prefix stripped) that are
// explicitly allowed for security-domain properties. Any security-domain key NOT
// listed here is routed to unsupported.properties instead of kof.broker.properties.
var section1Keys = map[string]bool{
	"listener.security.protocol.map":  true,
	"ssl.keystore.location":           true,
	"ssl.keystore.type":               true,
	"ssl.key.password":                true,
	"ssl.truststore.location":         true,
	"ssl.truststore.type":             true,
	"ssl.client.auth":                 true,
	"ssl.enabled.protocols":           true,
	"ssl.cipher.suites":               true,
	"ssl.principal.mapping.rules":     true,
	"sasl.enabled.mechanisms":         true,
	"plain.connections.max.reauth.ms": true,
	// per-mechanism (OAUTHBEARER) listener-scoped form of oauth2.connections.max.reauth.ms
	"oauthbearer.connections.max.reauth.ms": true,
	"oauth2.connections.max.reauth.ms":      true,
	"authorizer.class.name":                 true,
	"super.users":                           true,
	"allow.everyone.if.no.acl.found":        true,
}

// section3Keys are KRaft/cluster-control keys always rejected outright — KoF owns
// the quorum via FTL, so these are meaningless and must not appear in kof.broker.properties.
var section3Keys = map[string]bool{
	"process.roles":               true,
	"controller.quorum.voters":    true,
	"controller.listener.names":   true,
	"inter.broker.listener.name":  true,
	"control.plane.listener.name": true,
	"early.start.listeners":       true,
}

// stripListenerPrefix removes the "listener.name.<name>." prefix from k.
// Returns the unprefixed key. If k has no such prefix, returns k unchanged.
func stripListenerPrefix(k string) string {
	const pfx = "listener.name."
	if !strings.HasPrefix(strings.ToLower(k), pfx) {
		return k
	}
	rest := k[len(pfx):]
	idx := strings.IndexByte(rest, '.')
	if idx < 0 {
		return k
	}
	return rest[idx+1:]
}

// isSecurityDomainKey returns true for keys (with listener prefix already stripped)
// in the strict security whitelist domain: ssl.*, sasl.*, plain.*, oauthbearer.*,
// authorizer.class.name, super.users, allow.everyone.if.no.acl.found, and
// listener.security.protocol.map. Any key in this domain that is NOT in
// section1Keys is rejected to unsupported.properties.
func isSecurityDomainKey(base string) bool {
	b := strings.ToLower(base)
	switch {
	case strings.HasPrefix(b, "ssl."):
		return true
	case strings.HasPrefix(b, "sasl."):
		return true
	case strings.HasPrefix(b, "plain."):
		return true
	case strings.HasPrefix(b, "oauthbearer."):
		return true
	case b == "authorizer.class.name":
		return true
	case b == "super.users":
		return true
	case b == "allow.everyone.if.no.acl.found":
		return true
	case b == "listener.security.protocol.map":
		return true
	}
	return false
}

// isSection1Whitelisted returns true if base (with listener.name.* prefix stripped)
// is explicitly allowed by the KoF broker properties whitelist section 1.
func isSection1Whitelisted(base string) bool {
	return section1Keys[strings.ToLower(base)]
}

// isSection3Rejected returns true for KRaft/cluster-control keys that are always
// routed to unsupported.properties — they never belong in kof.broker.properties.
func isSection3Rejected(k string) bool {
	return section3Keys[strings.ToLower(k)]
}

// isSupportedBrokerProperty reports whether k belongs in kof.broker.properties.
//
//   - Section 3 keys (KRaft cluster-control): always rejected → unsupported.properties
//   - Security-domain keys (ssl.*, sasl.*, plain.*, oauthbearer.*, authz):
//     must appear in the section 1 whitelist → otherwise unsupported.properties
//   - Everything else (section 2): accepted as-is
func isSupportedBrokerProperty(k string) bool {
	if isSection3Rejected(k) {
		return false
	}
	base := stripListenerPrefix(k)
	if isSecurityDomainKey(base) {
		return isSection1Whitelisted(base)
	}
	return true
}
