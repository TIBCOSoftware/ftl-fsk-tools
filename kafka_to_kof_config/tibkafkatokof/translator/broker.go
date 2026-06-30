package translator

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ListenerDef represents one named Kafka listener (bind side + advertised side).
type ListenerDef struct {
	Name         string // canonical upper-case name, e.g. PLAINTEXT, BROKER, CONTROLLER
	BindAddr     string // bind address (0.0.0.0 or specific IP)
	BindPort     int
	AdvAddr      string // advertised address
	AdvPort      int
	Protocol     string // PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
	SASLMech     string // PLAIN, OAUTHBEARER, or ""
	ClientAuth   bool   // true when ssl.client.auth=required for this listener
	AuthMethod   string // none | tls | mtls | sasl | sasl_tls | oauth_tls
	IsController bool
}

// BrokerConfig holds all information parsed from one server.properties file.
type BrokerConfig struct {
	NodeID       int
	ProcessRoles string
	Listeners    []ListenerDef
	IsSecure     bool
	KOFHost      string            // advertised host of first non-controller listener
	KOFPort      int               // advertised port of first non-controller listener
	Settings     map[string]string // all properties not in the listener structural set
	SettingKeys  []string          // insertion-ordered keys for Settings
	SettingLines map[string]int    // 1-based source line where each key's entry begins
	SourceFile   string
}

// ParseBrokerConfig reads a Kafka broker server.properties file.
func ParseBrokerConfig(path string) (*BrokerConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	raw, orderedKeys, lines := readProperties(f)

	cfg := &BrokerConfig{
		SourceFile:   path,
		Settings:     map[string]string{},
		SettingLines: lines,
	}

	if v, ok := raw["node.id"]; ok {
		cfg.NodeID, _ = strconv.Atoi(v)
	} else {
		cfg.NodeID = 1
	}
	cfg.ProcessRoles = raw["process.roles"]

	bindMap := parseListenerAddrs(raw["listeners"])
	advMap := parseListenerAddrs(raw["advertised.listeners"])
	protocolMap := parseColonCSV(raw["listener.security.protocol.map"])
	mechMap := parsePerListenerSASL(raw)
	clientAuthMap := parsePerListenerClientAuth(raw)

	controllerSet := map[string]bool{}
	for _, n := range splitCSV(strings.ToUpper(raw["controller.listener.names"])) {
		controllerSet[n] = true
	}

	// Collect listener names in declaration order from the listeners property.
	var names []string
	seen := map[string]bool{}
	for _, part := range splitCSV(raw["listeners"]) {
		idx := strings.Index(part, "://")
		if idx < 0 {
			continue
		}
		n := strings.ToUpper(strings.TrimSpace(part[:idx]))
		if !seen[n] {
			names = append(names, n)
			seen[n] = true
		}
	}

	for _, name := range names {
		bind := bindMap[name]
		adv, hasAdv := advMap[name]
		if !hasAdv {
			adv = bind
		}

		nameLower := strings.ToLower(name)
		saslMech := mechMap[nameLower]
		if saslMech == "" {
			saslMech = mechMap["__global__"]
		}
		clientAuth := clientAuthMap[nameLower] || clientAuthMap["__global__"]
		proto := protocolMap[name]
		authMethod := deriveAuthMethod(proto, saslMech, clientAuth)

		cfg.Listeners = append(cfg.Listeners, ListenerDef{
			Name:         name,
			BindAddr:     bind.addr,
			BindPort:     bind.port,
			AdvAddr:      adv.addr,
			AdvPort:      adv.port,
			Protocol:     proto,
			SASLMech:     saslMech,
			ClientAuth:   clientAuth,
			AuthMethod:   authMethod,
			IsController: controllerSet[name],
		})
	}

	for _, ld := range cfg.Listeners {
		if !ld.IsController && ld.AuthMethod != "none" {
			cfg.IsSecure = true
		}
		if !ld.IsController && cfg.KOFHost == "" {
			cfg.KOFHost = ld.AdvAddr
			cfg.KOFPort = ld.AdvPort
		}
	}

	for _, k := range orderedKeys {
		cfg.Settings[k] = raw[k]
		cfg.SettingKeys = append(cfg.SettingKeys, k)
	}

	return cfg, nil
}

// readProperties parses a Java .properties file, handling line continuations.
// It returns the key/value map, the keys in insertion order, and the 1-based
// source line where each key's (possibly continued) entry begins.
func readProperties(f *os.File) (map[string]string, []string, map[string]int) {
	raw := map[string]string{}
	var orderedKeys []string
	lines := map[string]int{}

	sc := bufio.NewScanner(f)
	var pending strings.Builder
	var startLine int
	flush := func() {
		line := strings.TrimSpace(pending.String())
		pending.Reset()
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			return
		}
		idx := strings.IndexByte(line, '=')
		if idx < 0 {
			idx = strings.IndexByte(line, ':')
		}
		if idx < 0 {
			return
		}
		k := strings.TrimSpace(line[:idx])
		v := strings.TrimSpace(line[idx+1:])
		if _, exists := raw[k]; !exists {
			orderedKeys = append(orderedKeys, k)
			lines[k] = startLine
		}
		raw[k] = v
	}

	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Text()
		if pending.Len() == 0 {
			startLine = lineNo
		}
		trimmed := strings.TrimRight(line, " \t")
		if strings.HasSuffix(trimmed, "\\") {
			pending.WriteString(trimmed[:len(trimmed)-1])
			pending.WriteString(" ")
		} else {
			pending.WriteString(line)
			flush()
		}
	}
	if pending.Len() > 0 {
		flush()
	}
	return raw, orderedKeys, lines
}

type addrPort struct {
	addr string
	port int
}

// parseListenerAddrs parses "NAME://host:port,NAME2://host2:port2".
func parseListenerAddrs(s string) map[string]addrPort {
	m := map[string]addrPort{}
	for _, part := range splitCSV(s) {
		idx := strings.Index(part, "://")
		if idx < 0 {
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(part[:idx]))
		rest := part[idx+3:]
		lastColon := strings.LastIndexByte(rest, ':')
		if lastColon < 0 {
			continue
		}
		addr := rest[:lastColon]
		if addr == "" {
			addr = "0.0.0.0"
		}
		port, _ := strconv.Atoi(rest[lastColon+1:])
		m[name] = addrPort{addr: addr, port: port}
	}
	return m
}

// parseColonCSV parses "KEY:VAL,KEY2:VAL2" (used for listener.security.protocol.map).
func parseColonCSV(s string) map[string]string {
	m := map[string]string{}
	for _, part := range splitCSV(s) {
		idx := strings.IndexByte(part, ':')
		if idx < 0 {
			continue
		}
		k := strings.ToUpper(strings.TrimSpace(part[:idx]))
		v := strings.ToUpper(strings.TrimSpace(part[idx+1:]))
		m[k] = v
	}
	return m
}

// parsePerListenerSASL finds "listener.name.<name>.sasl.enabled.mechanisms".
// Falls back to global "sasl.enabled.mechanisms" stored under "__global__".
func parsePerListenerSASL(raw map[string]string) map[string]string {
	m := map[string]string{}
	const prefix = "listener.name."
	const suffix = ".sasl.enabled.mechanisms"
	for k, v := range raw {
		kl := strings.ToLower(k)
		if strings.HasPrefix(kl, prefix) && strings.HasSuffix(kl, suffix) {
			name := kl[len(prefix) : len(kl)-len(suffix)]
			m[name] = strings.ToUpper(v)
		}
	}
	if g, ok := raw["sasl.enabled.mechanisms"]; ok && g != "" {
		m["__global__"] = strings.ToUpper(g)
	}
	return m
}

// parsePerListenerClientAuth finds "listener.name.<name>.ssl.client.auth=required".
func parsePerListenerClientAuth(raw map[string]string) map[string]bool {
	m := map[string]bool{}
	const prefix = "listener.name."
	const suffix = ".ssl.client.auth"
	for k, v := range raw {
		kl := strings.ToLower(k)
		if strings.HasPrefix(kl, prefix) && strings.HasSuffix(kl, suffix) {
			name := kl[len(prefix) : len(kl)-len(suffix)]
			if strings.EqualFold(v, "required") {
				m[name] = true
			}
		}
	}
	if strings.EqualFold(raw["ssl.client.auth"], "required") {
		m["__global__"] = true
	}
	return m
}

// deriveAuthMethod maps Kafka protocol + SASL mechanism + client-auth flag to an AuthMethod string.
func deriveAuthMethod(protocol, saslMech string, clientAuth bool) string {
	switch strings.ToUpper(protocol) {
	case "PLAINTEXT":
		return "none"
	case "SASL_PLAINTEXT":
		return "sasl"
	case "SSL":
		if clientAuth {
			return "mtls"
		}
		return "tls"
	case "SASL_SSL":
		if strings.Contains(strings.ToUpper(saslMech), "OAUTHBEARER") {
			return "oauth_tls"
		}
		return "sasl_tls"
	default:
		return "none"
	}
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
