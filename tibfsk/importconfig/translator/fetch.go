/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// FetchBrokerConfig connects to the Kafka broker at addr (host:port), retrieves its
// effective configuration via the Admin DescribeConfigs API, and returns a BrokerConfig
// ready for the same translation pipeline used for file-based inputs.
//
// The SourceFile field of the returned BrokerConfig is set to addr so generated
// comments identify the live source.
func FetchBrokerConfig(addr string, timeoutMs int) (*BrokerConfig, error) {
	timeout := time.Duration(timeoutMs) * time.Millisecond

	cfg := sarama.NewConfig()
	cfg.Net.DialTimeout = timeout
	cfg.Net.ReadTimeout = timeout * 2
	cfg.Net.WriteTimeout = timeout
	cfg.Version = sarama.V2_4_0_0 // DescribeConfigs supported since 0.11; 2.4 covers KRaft clusters

	client, err := sarama.NewClient([]string{addr}, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to broker %s: %w", addr, err)
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("create admin client for %s: %w", addr, err)
	}
	defer admin.Close()

	// Discover the nodeID of this broker from cluster metadata.
	brokers := client.Brokers()
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers returned by metadata from %s", addr)
	}
	host, portStr, splitErr := splitHostPort(addr)
	if splitErr != nil {
		return nil, fmt.Errorf("invalid broker address %q: %w", addr, splitErr)
	}
	port, _ := strconv.Atoi(portStr)

	nodeID := brokers[0].ID() // default to first broker returned
	for _, b := range brokers {
		bHost := b.Addr()
		// b.Addr() returns "host:port"
		bh, bp, e := splitHostPort(bHost)
		if e != nil {
			continue
		}
		bPort, _ := strconv.Atoi(bp)
		if strings.EqualFold(bh, host) && bPort == port {
			nodeID = b.ID()
			break
		}
	}

	// DescribeConfig for the specific broker node.
	resource := sarama.ConfigResource{
		Type:        sarama.BrokerResource,
		Name:        strconv.Itoa(int(nodeID)),
		ConfigNames: nil, // nil = fetch all configs
	}
	entries, err := admin.DescribeConfig(resource)
	if err != nil {
		return nil, fmt.Errorf("describe config for broker %d at %s: %w", nodeID, addr, err)
	}

	raw := make(map[string]string, len(entries))
	orderedKeys := make([]string, 0, len(entries))
	for _, e := range entries {
		// DescribeConfigs returns every key the broker knows about, set or not.
		// Kafka's own defaults are not this operator's configuration, and copying
		// them in makes the tool report factory values nobody chose --
		// ssl.keystore.type=JKS on a broker with no TLS at all. Version 0 of the
		// response leaves Source unset, so Default has to be checked too.
		if e.Default || e.Source == sarama.SourceDefault {
			continue
		}
		if _, exists := raw[e.Name]; !exists {
			orderedKeys = append(orderedKeys, e.Name)
		}
		raw[e.Name] = e.Value
	}

	// A ZooKeeper-mode broker reports its identity as broker.id; FSK reads node.id.
	origin := NodeIDFromSource
	if renameBrokerID(raw, orderedKeys, nil) {
		origin = NodeIDFromBrokerID
	}

	// Ensure node.id is present. Cluster metadata already told us this broker's id,
	// which beats anything EnsureNodeIDs could invent later.
	if _, ok := raw[nodeIDKey]; !ok {
		raw[nodeIDKey] = strconv.Itoa(int(nodeID))
		orderedKeys = append(orderedKeys, nodeIDKey)
	}

	// Sort keys for deterministic output (Admin API returns in arbitrary order).
	sort.Strings(orderedKeys)

	brokerCfg := brokerConfigFromRaw(raw, orderedKeys, addr)
	brokerCfg.NodeIDOrigin = origin
	return brokerCfg, nil
}

// brokerConfigFromRaw builds a BrokerConfig from a pre-parsed key/value map.
// It mirrors ParseBrokerConfig but skips the file-reading step.
// orderedKeys controls the order of Settings/SettingKeys.
func brokerConfigFromRaw(raw map[string]string, orderedKeys []string, sourceLabel string) *BrokerConfig {
	cfg := &BrokerConfig{
		SourceFile:   sourceLabel,
		Settings:     map[string]string{},
		SettingLines: map[string]int{},
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

	// Capture KafkaHost/KafkaPort before stripping.
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

	// Strip internal listeners.
	internalListeners := map[string]bool{}
	for n := range controllerSet {
		internalListeners[n] = true
	}
	// Same rule as ParseBrokerConfig: the inter-broker listener is only internal
	// when another client listener remains to serve Kafka clients.
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

	// Same translation ParseBrokerConfig applies: JKS/PKCS12 -> PEM.
	NormalizeKeystores(cfg)

	return cfg
}

// splitHostPort splits "host:port" handling IPv6 brackets.
func splitHostPort(addr string) (host, port string, err error) {
	if len(addr) == 0 {
		return "", "", fmt.Errorf("empty address")
	}
	if addr[0] == '[' {
		end := strings.Index(addr, "]")
		if end < 0 {
			return "", "", fmt.Errorf("missing closing ] in %q", addr)
		}
		host = addr[1:end]
		rest := addr[end+1:]
		if len(rest) == 0 || rest[0] != ':' {
			return "", "", fmt.Errorf("missing port after ] in %q", addr)
		}
		return host, rest[1:], nil
	}
	last := strings.LastIndexByte(addr, ':')
	if last < 0 {
		return "", "", fmt.Errorf("missing port in %q", addr)
	}
	return addr[:last], addr[last+1:], nil
}
