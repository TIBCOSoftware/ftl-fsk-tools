package translator

import "strings"

// This file holds pass-through annotations: properties the tool does NOT change
// the value of, but adds a comment above so the operator understands what happens
// to them. Unlike the RESOLVE-REQUIRED blocks, these do not change the file's
// status -- they are informational.

// interBrokerKeys configure inter-broker or controller traffic. In KoF that
// traffic is the FTL fabric, not a Kafka listener, so the ftlserver ignores these
// keys. The tool keeps them for reference and notes that they are ignored.
var interBrokerKeys = map[string]bool{
	"inter.broker.listener.name":           true,
	"controller.listener.names":            true,
	"control.plane.listener.name":          true,
	"sasl.mechanism.inter.broker.protocol": true,
	"sasl.mechanism.controller.protocol":   true,
}

func isInterBrokerKey(k string) bool { return interBrokerKeys[strings.ToLower(k)] }

// isKeystoreTypeKey reports a ssl keystore or truststore type key (global or
// per-listener).
func isKeystoreTypeKey(k string) bool {
	kl := strings.ToLower(k)
	return kl == "ssl.keystore.type" || kl == "ssl.truststore.type" ||
		strings.HasSuffix(kl, ".ssl.keystore.type") || strings.HasSuffix(kl, ".ssl.truststore.type")
}

// isJavaKeystore reports a keystore type value KoF cannot read directly: KoF reads
// PEM, so a Java keystore (JKS or PKCS12) must be converted first.
func isJavaKeystore(v string) bool {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "JKS", "PKCS12":
		return true
	}
	return false
}
