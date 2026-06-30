package translator

// ResolveKind classifies one unresolved setting so the report can explain it.
type ResolveKind string

const (
	KindHandler       ResolveKind = "handler"        // custom handler class -> pick a backend
	KindBackendParams ResolveKind = "backend-params" // backend chosen, params missing
	KindKeystore      ResolveKind = "keystore"       // JKS/PKCS12 -> convert to PEM
	KindAuthorizer    ResolveKind = "authorizer"     // custom authorizer
	KindMechanism     ResolveKind = "mechanism"      // SASL mechanism KoF cannot serve
)

// ResolveItem is one unresolved setting, with enough to point the operator at it:
// the source line it came from, the key, its value, and a note (the keystore file).
type ResolveItem struct {
	Kind  ResolveKind
	Line  int
	Key   string
	Value string
	Note  string
}

// ResolveSummary is the ordered list of unresolved settings (file order).
type ResolveSummary struct {
	Items []ResolveItem
}

// Total is the number of unresolved items (0 means ACCEPTED).
func (s ResolveSummary) Total() int { return len(s.Items) }

// Summarize collects the unresolved settings from a parsed config, in file order,
// using the same resolution the status computation uses.
func Summarize(cfg *BrokerConfig) ResolveSummary {
	var s ResolveSummary
	add := func(kind ResolveKind, k, value, note string) {
		s.Items = append(s.Items, ResolveItem{Kind: kind, Line: cfg.SettingLines[k], Key: k, Value: value, Note: note})
	}
	for _, k := range cfg.SettingKeys {
		switch {
		case isHandlerClassKey(k):
			o := resolveHandler(cfg, k)
			switch {
			case o.NeedsPick:
				add(KindHandler, k, cfg.Settings[k], "")
			case !o.Resolved:
				add(KindBackendParams, k, string(o.Backend), "")
			}
		case isAuthorizerKey(k):
			if recognized, _ := resolveAuthorizer(cfg.Settings[k]); !recognized {
				add(KindAuthorizer, k, cfg.Settings[k], "")
			}
		case isKeystoreTypeKey(k):
			if isJavaKeystore(cfg.Settings[k]) {
				locKey := k[:len(k)-len(".type")] + ".location"
				add(KindKeystore, k, cfg.Settings[k], cfg.Settings[locKey])
			}
		case isMechanismsKey(k):
			if mechanismsUnservable(cfg.Settings[k]) {
				add(KindMechanism, k, cfg.Settings[k], "")
			}
		}
	}
	return s
}
