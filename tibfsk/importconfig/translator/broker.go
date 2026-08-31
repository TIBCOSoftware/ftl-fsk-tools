/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

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

// RemovedListener records one FTL-native listener entry stripped from the
// client-facing listener properties, for the unsupported.properties report.
type RemovedListener struct {
	Entry   string // the original "NAME://host:port" entry
	NamedBy string // the key that marked it internal (inter.broker.listener.name / controller.listener.names)
}

// NodeIDOrigin records where the node.id written into kof.broker.N.properties came
// from, so the generated file can say so.
type NodeIDOrigin string

const (
	// NodeIDFromSource: the input already named node.id.
	NodeIDFromSource NodeIDOrigin = ""
	// NodeIDFromBrokerID: renamed from the ZooKeeper-era broker.id.
	NodeIDFromBrokerID NodeIDOrigin = "broker.id"
	// NodeIDAssigned: the input carried no usable id, so the tool picked one.
	NodeIDAssigned NodeIDOrigin = "assigned"
)

const (
	nodeIDKey   = "node.id"
	brokerIDKey = "broker.id"
)

// BrokerConfig holds all information parsed from one server.properties file.
type BrokerConfig struct {
	NodeID       int
	NodeIDOrigin NodeIDOrigin
	ProcessRoles string
	Listeners    []ListenerDef
	IsSecure     bool
	KOFHost      string            // advertised host of first non-controller, non-inter-broker listener
	KOFPort      int               // advertised port of first non-controller, non-inter-broker listener
	KafkaHost    string            // advertised host of first non-controller listener (includes inter-broker); used by migration config
	KafkaPort    int               // advertised port of first non-controller listener (includes inter-broker)
	Settings     map[string]string // all properties not in the listener structural set
	SettingKeys  []string          // insertion-ordered keys for Settings
	SettingLines map[string]int    // 1-based source line where each key's entry begins
	SourceFile   string

	// RemovedListeners lists the inter-broker/controller listener entries dropped
	// from listeners/advertised.listeners/listener.security.protocol.map.
	RemovedListeners []RemovedListener
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

	if renameBrokerID(raw, orderedKeys, lines) {
		cfg.NodeIDOrigin = NodeIDFromBrokerID
	}
	if v, ok := raw[nodeIDKey]; ok {
		cfg.NodeID, _ = strconv.Atoi(v)
	} else {
		cfg.NodeID = 1
	}
	cfg.ProcessRoles = raw["process.roles"]

	controllerSet := map[string]bool{}
	for _, n := range splitCSV(strings.ToUpper(raw["controller.listener.names"])) {
		controllerSet[n] = true
	}

	// Capture KafkaHost/KafkaPort from the first non-controller advertised listener
	// before any stripping. This includes inter-broker listeners that are also
	// client-capable (e.g. PLAINTEXT in simple single-listener setups). The
	// migration config uses this to populate target.bootstrap.servers.
	preStripAdvMap := parseListenerAddrs(raw["advertised.listeners"])
	for _, part := range splitCSV(raw["listeners"]) {
		idx := strings.Index(part, "://")
		if idx < 0 {
			continue
		}
		n := strings.ToUpper(strings.TrimSpace(part[:idx]))
		if controllerSet[n] {
			continue
		}
		if ap, ok := preStripAdvMap[n]; ok && ap.port != 0 && ap.addr != "" && ap.addr != "0.0.0.0" {
			cfg.KafkaHost = ap.addr
			cfg.KafkaPort = ap.port
			break
		}
	}

	// Remove the listeners named by inter.broker.listener.name and
	// controller.listener.names from listeners, advertised.listeners, and
	// listener.security.protocol.map before cfg.Listeners is built, so
	// cfg.Listeners and its derived fields (IsSecure, KOFHost/KOFPort) hold
	// only the listeners Kafka clients connect to. Removed entries are
	// recorded on cfg for the unsupported.properties report.
	internalListeners := map[string]bool{}
	for n := range controllerSet {
		internalListeners[n] = true
	}
	// Unlike a controller listener, the inter-broker listener is a normal entry in
	// listeners that Kafka clients may also connect to, and pointing
	// inter.broker.listener.name at the only client listener is a common setup.
	// Treat it as internal only when another client listener remains; otherwise
	// stripping it would leave cfg.Listeners empty and silently clear IsSecure.
	ib := strings.ToUpper(strings.TrimSpace(raw["inter.broker.listener.name"]))
	if ib != "" && hasOtherClientListener(raw["listeners"], controllerSet, ib) {
		internalListeners[ib] = true
	}
	if len(internalListeners) > 0 {
		for _, part := range splitCSV(raw["listeners"]) {
			p := strings.TrimSpace(part)
			idx := strings.Index(p, "://")
			if idx < 0 {
				continue
			}
			n := strings.ToUpper(strings.TrimSpace(p[:idx]))
			if !internalListeners[n] {
				continue
			}
			namedBy := "controller.listener.names"
			if n == ib {
				namedBy = "inter.broker.listener.name"
			}
			cfg.RemovedListeners = append(cfg.RemovedListeners,
				RemovedListener{Entry: p, NamedBy: namedBy})
		}
		raw["listeners"] = dropInternalListeners(raw["listeners"], internalListeners)
		raw["advertised.listeners"] = dropInternalListeners(raw["advertised.listeners"], internalListeners)
		raw["listener.security.protocol.map"] = dropInternalProtocolMap(raw["listener.security.protocol.map"], internalListeners)
	}

	bindMap := parseListenerAddrs(raw["listeners"])
	advMap := parseListenerAddrs(raw["advertised.listeners"])
	protocolMap := parseColonCSV(raw["listener.security.protocol.map"])
	mechMap := parsePerListenerSASL(raw)
	clientAuthMap := parsePerListenerClientAuth(raw)

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

// renameBrokerID moves a ZooKeeper-era broker.id onto node.id, in place, and
// reports whether it did.
//
// A ZooKeeper-mode broker (Kafka 3.9 and earlier) names its identity broker.id;
// KRaft renamed the key to node.id, and node.id is the only spelling the FSK
// pserver accepts -- without it tibftlserver refuses to start with
// "kof.broker.properties: node.id is required and must be a non-negative integer".
// Renaming rather than adding keeps the key in its original position, so the
// generated file still mirrors the source ordering, and stops broker.id from being
// reported as unsupported when its value was in fact used.
//
// A file that names both keys is left alone: node.id is already the authoritative
// one, and the stray broker.id goes to unsupported.properties like any other
// unrecognized key.
func renameBrokerID(raw map[string]string, orderedKeys []string, lines map[string]int) bool {
	if _, ok := raw[nodeIDKey]; ok {
		return false
	}
	v, ok := raw[brokerIDKey]
	if !ok {
		return false
	}
	raw[nodeIDKey] = v
	delete(raw, brokerIDKey)
	for i, k := range orderedKeys {
		if k == brokerIDKey {
			orderedKeys[i] = nodeIDKey
			break
		}
	}
	if ln, ok := lines[brokerIDKey]; ok {
		lines[nodeIDKey] = ln
		delete(lines, brokerIDKey)
	}
	return true
}

// EnsureNodeIDs guarantees every broker in the set carries a usable node.id, and
// that the ids stay distinct. Call it once, on the whole set, before any output is
// written.
//
// The FSK pserver rejects a broker properties file with no node.id, and rejects a
// negative one. Kafka allows both: a KRaft server.properties may omit the key when
// the id is passed to `kafka-storage format` instead, and a ZooKeeper one may set
// broker.id=-1 to ask the broker to generate its own. Either way the generated file
// would not boot, so each such broker is given the lowest positive id no other
// broker in the set already claims.
func EnsureNodeIDs(cfgs []*BrokerConfig) {
	taken := map[int]bool{}
	for _, cfg := range cfgs {
		if id, ok := usableNodeID(cfg); ok {
			taken[id] = true
		}
	}
	next := 1
	for _, cfg := range cfgs {
		if id, ok := usableNodeID(cfg); ok {
			cfg.NodeID = id
			continue
		}
		for taken[next] {
			next++
		}
		taken[next] = true
		cfg.setNodeID(next)
	}
}

// usableNodeID returns the config's node.id when it is present and non-negative.
func usableNodeID(cfg *BrokerConfig) (int, bool) {
	v, ok := cfg.Settings[nodeIDKey]
	if !ok {
		return 0, false
	}
	id, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || id < 0 {
		return 0, false
	}
	return id, true
}

// setNodeID writes an id the tool chose into the generated properties. A node.id
// the source never had leads the remaining properties, where it is easy to spot.
func (cfg *BrokerConfig) setNodeID(id int) {
	cfg.NodeID = id
	cfg.NodeIDOrigin = NodeIDAssigned
	if _, ok := cfg.Settings[nodeIDKey]; !ok {
		cfg.SettingKeys = append([]string{nodeIDKey}, cfg.SettingKeys...)
	}
	cfg.Settings[nodeIDKey] = strconv.Itoa(id)
}

// dropInternalListeners removes entries whose listener name (the token before
// "://") is in exclude, returning the remaining "NAME://addr" entries comma-joined.
// Used to strip the inter-broker/controller listeners, which are FTL-native in FSK.
func dropInternalListeners(s string, exclude map[string]bool) string {
	if strings.TrimSpace(s) == "" {
		return s
	}
	var kept []string
	for _, part := range splitCSV(s) {
		p := strings.TrimSpace(part)
		idx := strings.Index(p, "://")
		if idx >= 0 && exclude[strings.ToUpper(strings.TrimSpace(p[:idx]))] {
			continue
		}
		kept = append(kept, p)
	}
	return strings.Join(kept, ",")
}

// dropInternalProtocolMap removes entries whose listener name (the token before the
// first ":") is in exclude, from a "NAME:PROTOCOL,..." map. Standard protocol
// self-mappings (PLAINTEXT:PLAINTEXT, SSL:SSL, ...) are not listener names, so they
// are kept.
func dropInternalProtocolMap(s string, exclude map[string]bool) string {
	if strings.TrimSpace(s) == "" {
		return s
	}
	var kept []string
	for _, part := range splitCSV(s) {
		p := strings.TrimSpace(part)
		idx := strings.Index(p, ":")
		if idx >= 0 && exclude[strings.ToUpper(strings.TrimSpace(p[:idx]))] {
			continue
		}
		kept = append(kept, p)
	}
	return strings.Join(kept, ",")
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
// hasOtherClientListener reports whether listeners declares a non-controller
// listener other than exclude.
func hasOtherClientListener(listeners string, controllerSet map[string]bool, exclude string) bool {
	for _, part := range splitCSV(listeners) {
		p := strings.TrimSpace(part)
		idx := strings.Index(p, "://")
		if idx < 0 {
			continue
		}
		n := strings.ToUpper(strings.TrimSpace(p[:idx]))
		if n == exclude || controllerSet[n] {
			continue
		}
		return true
	}
	return false
}

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
