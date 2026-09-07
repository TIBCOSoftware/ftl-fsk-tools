/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import "strings"

// A security setting can be present without being in use: an empty
// authorizer.class.name is no authorizer, a keystore type with no location loads
// nothing, and a SASL mechanism list means nothing on a broker whose listeners are
// all PLAINTEXT. Apache Kafka's own DescribeConfigs reports all three at their factory
// defaults, so a plain unsecured broker fetched over the Admin API carries every one
// of them.
//
// Refusing to convert over a setting that does nothing would block a config that
// runs fine as it stands. These are kept in the generated file, commented, with the
// reason -- the operator can still see what the source said.

// inertReason reports why a whitelisted security setting is not actually in use by
// this broker, or "" when it is (or when the tool cannot tell). Every caller must
// treat "" as "in use": a wrong "inert" would silently drop a setting a real
// cluster depends on.
func inertReason(cfg *BrokerConfig, k string) string {
	switch {
	case isAuthorizerKey(k):
		if strings.TrimSpace(cfg.Settings[k]) == "" {
			return "the value is empty, so the source configured no authorizer"
		}
	case isKeystoreTypeKey(k):
		if locKey, loc := keystoreLocation(cfg, k); loc == "" {
			return "no " + locKey + " is set, so no " + keystoreKind(k) + " is loaded"
		}
	case isMechanismsKey(k):
		if !saslInUse(cfg, k) {
			return "no listener uses SASL, so this mechanism list is never offered"
		}
	}
	return ""
}

// keystoreLocation resolves the keystore file a .type key governs, following Apache Kafka's
// fallback between the per-listener and broker-wide forms: a listener that names no
// location of its own uses the broker-wide one, and a broker-wide type can be the
// type of a location only some listener names. It returns the key it would have come
// from (for the message) and the value, which is "" when no listener names a file.
func keystoreLocation(cfg *BrokerConfig, typeKey string) (string, string) {
	locKey := keystoreLocationKey(typeKey)
	if v := strings.TrimSpace(cfg.Settings[locKey]); v != "" {
		return locKey, v
	}
	// Per-listener type: fall back to the broker-wide location.
	if listenerNameOf(typeKey) != "" {
		global := stripListenerPrefix(locKey)
		if v := strings.TrimSpace(cfg.Settings[global]); v != "" {
			return locKey, v
		}
		return locKey, ""
	}
	// Broker-wide type: any per-listener location of the same kind uses it.
	suffix := "." + strings.ToLower(locKey)
	for _, k := range cfg.SettingKeys {
		if listenerNameOf(k) == "" || !strings.HasSuffix(strings.ToLower(k), suffix) {
			continue
		}
		if v := strings.TrimSpace(cfg.Settings[k]); v != "" {
			return locKey, v
		}
	}
	return locKey, ""
}

// saslInUse reports whether the listeners a key governs actually speak SASL. The
// broker-wide form (no listener.name. prefix) asks about any client listener; the
// listener.name.<x>. form asks about that one.
//
// It answers true whenever the tool cannot tell -- no listeners parsed, or a
// protocol it does not recognize -- so an unreadable listeners line can never make a
// genuinely SASL cluster look inert.
func saslInUse(cfg *BrokerConfig, key string) bool {
	if len(cfg.Listeners) == 0 {
		return true
	}
	name := listenerNameOf(key)
	matched := false
	for _, ld := range cfg.Listeners {
		if ld.IsController {
			continue // controller traffic is FTL's in FSK, not an Apache Kafka listener
		}
		if name != "" && !strings.EqualFold(ld.Name, name) {
			continue
		}
		matched = true
		switch listenerProtocol(ld) {
		case "SASL_PLAINTEXT", "SASL_SSL":
			return true
		case "PLAINTEXT", "SSL":
			// definitely not SASL
		default:
			return true // unrecognized protocol: make no claim
		}
	}
	// A per-listener key naming a listener that does not exist governs nothing we
	// can reason about; leave it to the normal rules.
	return !matched
}

// listenerProtocol reports the security protocol of ld. Apache Kafka defaults a listener's
// protocol to its own name when listener.security.protocol.map does not name it, so
// "listeners=SASL_SSL://..." with no map still resolves.
func listenerProtocol(ld ListenerDef) string {
	if ld.Protocol != "" {
		return strings.ToUpper(strings.TrimSpace(ld.Protocol))
	}
	return strings.ToUpper(strings.TrimSpace(ld.Name))
}

// listenerNameOf returns the listener a "listener.name.<x>.…" key applies to, or ""
// for the broker-wide form.
func listenerNameOf(k string) string {
	const pfx = "listener.name."
	if !strings.HasPrefix(strings.ToLower(k), pfx) {
		return ""
	}
	rest := k[len(pfx):]
	idx := strings.IndexByte(rest, '.')
	if idx < 0 {
		return ""
	}
	return rest[:idx]
}
