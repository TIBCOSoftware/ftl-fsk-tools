/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import (
	"path/filepath"
	"strings"
)

// FSK reads PEM keystores, so a JKS or PKCS12 keystore in the source is not a
// setting the tool has to refuse -- it is a setting it can translate. This file
// does the translation up front, before anything reads cfg.Settings: the type
// becomes PEM and the location becomes the matching .pem path, and the record of
// what changed is kept on the config so the generated file can carry the commands
// that actually create the .pem.
//
// The one thing the tool cannot do on its own is produce the file. That is what
// --auto is for (see auto.go), and what the notice after an accepted run says.

// KeystoreConversion records one Java keystore rewritten to its PEM form.
type KeystoreConversion struct {
	TypeKey  string // ssl.keystore.type (global or listener.name.<x>. prefixed)
	LocKey   string // the matching .location key
	FromType string // JKS or PKCS12, as the source spelled it
	FromLoc  string // the Java keystore named by the source
	P12Loc   string // intermediate PKCS12; equal to FromLoc when the source was PKCS12
	ToLoc    string // the .pem the operator must end up with
	Kind     string // "keystore" (key + chain) or "truststore" (CA certs only)
	Done     bool   // --auto ran the conversion in this pass
}

// keystoreBase strips the trailing ".type" from a keystore type key.
func keystoreBase(typeKey string) string { return typeKey[:len(typeKey)-len(".type")] }

// keystoreLocationKey names the .location that goes with a keystore .type key.
func keystoreLocationKey(typeKey string) string { return keystoreBase(typeKey) + ".location" }

// isKeystoreLocationKey reports an ssl keystore or truststore location key (global
// or per-listener). It is the .location counterpart of isKeystoreTypeKey.
func isKeystoreLocationKey(k string) bool {
	kl := strings.ToLower(k)
	return kl == "ssl.keystore.location" || kl == "ssl.truststore.location" ||
		strings.HasSuffix(kl, ".ssl.keystore.location") || strings.HasSuffix(kl, ".ssl.truststore.location")
}

// keystoreKind distinguishes a keystore (private key + chain) from a truststore
// (CA certs only). The openssl step differs: a truststore adds -nokeys.
func keystoreKind(k string) string {
	if strings.Contains(strings.ToLower(k), "truststore") {
		return "truststore"
	}
	return "keystore"
}

// NormalizeKeystores rewrites every JKS/PKCS12 keystore that names a location into
// its PEM form, recording each rewrite on cfg. It must run before the config is read
// for status, emission or --auto, so those all see the translated value.
//
// A keystore type with no location is left alone: there is nothing to convert and
// nothing is being loaded either, so inertReason handles it instead.
func NormalizeKeystores(cfg *BrokerConfig) {
	cfg.KeystoreConversions = nil
	for _, k := range cfg.SettingKeys {
		if !isKeystoreTypeKey(k) || !isJavaKeystore(cfg.Settings[k]) {
			continue
		}
		locKey := keystoreLocationKey(k)
		loc := strings.TrimSpace(cfg.Settings[locKey])
		if loc == "" {
			continue
		}

		srcType := strings.ToUpper(strings.TrimSpace(cfg.Settings[k]))
		stem := strings.TrimSuffix(loc, filepath.Ext(loc))
		kc := KeystoreConversion{
			TypeKey:  k,
			LocKey:   locKey,
			FromType: srcType,
			FromLoc:  loc,
			P12Loc:   loc, // PKCS12 is read by openssl directly; no intermediate
			ToLoc:    stem + ".pem",
			Kind:     keystoreKind(k),
		}
		if srcType == "JKS" {
			kc.P12Loc = stem + ".p12"
		}
		cfg.KeystoreConversions = append(cfg.KeystoreConversions, kc)

		cfg.Settings[k] = "PEM"
		cfg.Settings[locKey] = kc.ToLoc
	}
}

// keystoreConversionByType finds the rewrite recorded for a keystore .type key.
func (cfg *BrokerConfig) keystoreConversionByType(typeKey string) *KeystoreConversion {
	for i := range cfg.KeystoreConversions {
		if cfg.KeystoreConversions[i].TypeKey == typeKey {
			return &cfg.KeystoreConversions[i]
		}
	}
	return nil
}

// keystoreConversionByLoc finds the rewrite recorded for a keystore .location key.
func (cfg *BrokerConfig) keystoreConversionByLoc(locKey string) *KeystoreConversion {
	for i := range cfg.KeystoreConversions {
		if cfg.KeystoreConversions[i].LocKey == locKey {
			return &cfg.KeystoreConversions[i]
		}
	}
	return nil
}

// PendingKeystores lists the conversions whose .pem does not exist yet -- the ones
// the operator still has to run by hand. --auto marks the rest Done.
func PendingKeystores(cfg *BrokerConfig) []KeystoreConversion {
	var out []KeystoreConversion
	for _, kc := range cfg.KeystoreConversions {
		if !kc.Done {
			out = append(out, kc)
		}
	}
	return out
}

// Commands renders the keytool/openssl invocations that turn FromLoc into ToLoc.
// A JKS needs two steps (keytool to PKCS12, then openssl); a PKCS12 needs one.
func (kc KeystoreConversion) Commands() []string {
	nokeys := ""
	if kc.Kind == "truststore" {
		nokeys = "-nokeys "
	}
	var cmds []string
	if kc.FromType == "JKS" {
		cmds = append(cmds,
			"keytool -importkeystore -srckeystore "+kc.FromLoc+" -srcstoretype JKS \\",
			"        -destkeystore "+kc.P12Loc+" -deststoretype PKCS12")
	}
	return append(cmds, "openssl pkcs12 -in "+kc.P12Loc+" -nodes "+nokeys+"-out "+kc.ToLoc)
}
