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
	// per-mechanism (OAUTHBEARER) listener-scoped reauth deadline; the mechanism name
	// is the lowercase Kafka spelling. The non-Kafka "oauth2." form is deliberately not
	// accepted (the runtime rejects it), matching kofbroker/whitelist.go.
	"oauthbearer.connections.max.reauth.ms": true,
	"authorizer.class.name":                 true,
	"super.users":                           true,
	"allow.everyone.if.no.acl.found":        true,
}

// section2Keys is every non-security broker property FSK accepts (general, producer,
// consumer, default-topic). A non-security key not listed here is unsupported and is
// routed to unsupported.properties. Mirrors generalWhitelist in the ftlserver runtime
// (tibftlserver/kofbroker/whitelist.go) -- keep in sync.
var section2Keys = map[string]bool{
	"node.id":                          true,
	"listeners":                        true,
	"advertised.listeners":             true,
	"producer.id.expiration.ms":        true,
	"group.initial.rebalance.delay.ms": true,
	"num.partitions":                   true,
	"log.cleanup.policy":               true,
	"compression.type":                 true,
	"log.cleaner.delete.retention.ms":  true,
	"log.index.interval.bytes":         true,
	"message.max.bytes":                true,
	"log.message.timestamp.type":       true,
	"log.retention.bytes":              true,
	"log.retention.ms":                 true,
	"log.retention.minutes":            true,
	"log.retention.hours":              true,
	"socket.request.max.bytes":         true,
	"auto.create.topics.enable":        true,
}

// perListenerReauth: per-mechanism reauth keys honored only with a listener.name.<l>.
// prefix; the bare broker-wide form is ignored by the pserver.
var perListenerReauth = map[string]bool{
	"plain.connections.max.reauth.ms":       true,
	"oauthbearer.connections.max.reauth.ms": true,
}

// section3Keys are KRaft/cluster-control keys always rejected outright — FSK owns
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
// is explicitly allowed by the FSK broker properties whitelist section 1.
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
//   - Any other key must appear in the section 2 allowlist → otherwise unsupported.properties
func isSupportedBrokerProperty(k string) bool {
	if isSection3Rejected(k) {
		return false
	}
	base := stripListenerPrefix(k)
	if isSecurityDomainKey(base) {
		// bare per-mechanism reauth is per-listener only; pserver ignores the broker-wide form
		if perListenerReauth[strings.ToLower(base)] && !strings.HasPrefix(strings.ToLower(k), "listener.name.") {
			return false
		}
		return isSection1Whitelisted(base)
	}
	return section2Keys[strings.ToLower(base)]
}

// runtimeSecurityWhitelist mirrors the ftlserver runtime's fail-closed security-key
// whitelist (tibftlserver/kofbroker/whitelist.go). The runtime rejects any KRaft/control
// key or unsupported security key at startup; the tool must therefore never EMIT
// such a key as an active line, or its "ACCEPTED" output would fail to boot.
// Keep it in sync with the runtime list.
var runtimeSecurityWhitelist = map[string]bool{
	"listener.security.protocol.map":        true,
	"ssl.keystore.location":                 true,
	"ssl.keystore.type":                     true,
	"ssl.key.password":                      true,
	"ssl.truststore.location":               true,
	"ssl.truststore.type":                   true,
	"ssl.client.auth":                       true,
	"ssl.enabled.protocols":                 true,
	"ssl.cipher.suites":                     true,
	"ssl.principal.mapping.rules":           true,
	"sasl.enabled.mechanisms":               true,
	"connections.max.reauth.ms":             true,
	"plain.connections.max.reauth.ms":       true,
	"oauthbearer.connections.max.reauth.ms": true,
	"authorizer.class.name":                 true,
	"super.users":                           true,
	"allow.everyone.if.no.acl.found":        true,
}

var runtimeKraftKeys = map[string]bool{
	"process.roles":               true,
	"controller.quorum.voters":    true,
	"controller.listener.names":   true,
	"inter.broker.listener.name":  true,
	"control.plane.listener.name": true,
	"early.start.listeners":       true,
}

// runtimeBareKey strips a "listener.name.<name>." prefix.
func runtimeBareKey(key string) string {
	const p = "listener.name."
	kl := strings.ToLower(key)
	if strings.HasPrefix(kl, p) {
		if i := strings.IndexByte(kl[len(p):], '.'); i > 0 && len(kl) > len(p)+i+1 {
			return kl[len(p)+i+1:]
		}
	}
	return kl
}

func runtimeIsSecurityDomainKey(bare string) bool {
	switch {
	case strings.HasPrefix(bare, "ssl."),
		strings.HasPrefix(bare, "sasl."),
		strings.HasPrefix(bare, "plain."),
		strings.HasPrefix(bare, "oauthbearer."),
		strings.HasPrefix(bare, "oauth2."):
		return true
	}
	switch bare {
	case "connections.max.reauth.ms",
		"listener.security.protocol.map",
		"authorizer.class.name",
		"super.users",
		"allow.everyone.if.no.acl.found":
		return true
	}
	return false
}

// runtimeRejects reports whether the ftlserver runtime would reject this key at
// startup: a KRaft/control key, or a security-domain key not on the whitelist.
// The tool must not emit such a key active.
func runtimeRejects(key string) bool {
	bare := runtimeBareKey(key)
	if runtimeKraftKeys[bare] {
		return true
	}
	if perListenerReauth[bare] && !strings.HasPrefix(strings.ToLower(key), "listener.name.") {
		return true
	}
	if runtimeIsSecurityDomainKey(bare) && !runtimeSecurityWhitelist[bare] {
		return true
	}
	return false
}
