package translator

import "strings"

// UnsupportedItem is a key or key=value in the broker config that KoF does not
// honor. It is informational: the line is kept in the output for reference, but it
// will not take effect, and the tool says why so the operator does not have to read
// the FTL docs to find out.
type UnsupportedItem struct {
	What   string // the key, a "key.*" family, or a "key=value"
	Reason string
}

// UnsupportedScan reports the broker keys/values KoF does not support, in file
// order. Families (sasl.kerberos.*, delegation.token.*) are reported once.
func UnsupportedScan(cfg *BrokerConfig) []UnsupportedItem {
	var items []UnsupportedItem
	seen := map[string]bool{}
	add := func(what, reason string) {
		if seen[what] {
			return
		}
		seen[what] = true
		items = append(items, UnsupportedItem{what, reason})
	}

	for _, k := range cfg.SettingKeys {
		kl := strings.ToLower(k)
		switch {
		case strings.HasPrefix(kl, "delegation.token."):
			add("delegation.token.*", "delegation tokens are not implemented in KoF")
		case strings.HasPrefix(kl, "sasl.kerberos.") || strings.Contains(kl, ".sasl.kerberos."):
			add("sasl.kerberos.*", "Kerberos/GSSAPI is not supported")
		case kl == "principal.builder.class":
			add(k, "a custom principal builder is not supported; KoF derives the principal natively")
		case kl == "security.providers":
			add(k, "custom Java security providers cannot be loaded")
		case kl == "ssl.engine.factory.class" || strings.HasSuffix(kl, ".ssl.engine.factory.class"):
			add(k, "a custom Java SSL engine is not supported; KoF terminates TLS with OpenSSL")
		case kl == "ssl.provider" || strings.HasSuffix(kl, ".ssl.provider"):
			add(k, "a named Java security provider has no effect; KoF uses OpenSSL")
		case kl == "max.connections.per.ip" || kl == "max.connections.per.ip.overrides":
			add(k, "per-IP connection limits are not implemented")
		}

		// Value-level: unsupported SASL mechanisms on a listener that ALSO offers a
		// supported one (mixed) -- informational, the supported one still works. A
		// listener with no supported mechanism is handled as a RESOLVE-REQUIRED block
		// (INVALID), not here.
		if isMechanismsKey(k) {
			if sup, unsup := mechanismsSupport(cfg.Settings[k]); len(sup) > 0 {
				for _, mu := range unsup {
					add(k+"="+mu, "KoF supports PLAIN and OAUTHBEARER only; this mechanism is ignored")
				}
			}
		}
	}
	return items
}
