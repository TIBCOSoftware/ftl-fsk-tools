/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// --auto runs only the deterministic, mechanical conversions: it converts JKS/PKCS12
// keystores to PEM via keytool/openssl and updates the config so they resolve. It
// does NOT make judgment calls -- it never picks a handler backend (the class name
// only suggests one; that is the operator's call in the resolve block) or invents an
// external fact (an OAuth URL, a secret). Every action is narrated with its outcome,
// and anything it cannot do is left as a RESOLVE-REQUIRED block -- nothing changes
// silently.

type commandRunner func(name string, args ...string) (string, error)

func execRunner(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}

// AutoReport summarizes one --auto pass.
type AutoReport struct {
	KeystoresConverted int // JKS/PKCS12 actually converted to PEM
	KeystoresSkipped   int // could not convert here (source file/binary not present)
	KeystoresFailed    int // a conversion command errored
}

const (
	keystoreConverted = "converted"
	keystoreSkipped   = "skipped"
	keystoreFailed    = "failed"
)

// AutoResolve applies the automatic resolutions to cfg, narrating each to log.
func AutoResolve(cfg *BrokerConfig, log io.Writer) AutoReport {
	return autoResolve(cfg, log, execRunner)
}

func autoResolve(cfg *BrokerConfig, log io.Writer, run commandRunner) AutoReport {
	var r AutoReport

	// Convert JKS/PKCS12 keystores to PEM where the files are present.
	for _, k := range cfg.SettingKeys {
		if !isKeystoreTypeKey(k) || !isJavaKeystore(cfg.Settings[k]) {
			continue
		}
		switch convertOneKeystore(cfg, k, log, run) {
		case keystoreConverted:
			r.KeystoresConverted++
		case keystoreSkipped:
			r.KeystoresSkipped++
		default:
			r.KeystoresFailed++
		}
	}
	return r
}

// convertOneKeystore converts a single keystore, mutating cfg on success. It returns
// keystoreConverted, keystoreSkipped (cannot do it here -- file/binary absent), or
// keystoreFailed (a command errored).
func convertOneKeystore(cfg *BrokerConfig, typeKey string, log io.Writer, run commandRunner) string {
	srcType := strings.ToUpper(strings.TrimSpace(cfg.Settings[typeKey]))
	base := typeKey[:len(typeKey)-len(".type")]
	locKey := base + ".location"
	pwKey := base + ".password"
	kind := "keystore"
	if strings.Contains(strings.ToLower(typeKey), "truststore") {
		kind = "truststore"
	}
	loc := cfg.Settings[locKey]
	pw := cfg.Settings[pwKey]

	// Skips are silent: nothing was changed, and the resolve report already lists
	// the keystore with "--auto couldn't here". Only real work (a conversion or a
	// failure) is narrated.
	if loc == "" {
		return keystoreSkipped
	}
	if _, err := os.Stat(loc); err != nil {
		return keystoreSkipped
	}
	fmt.Fprintf(log, "auto: converting %s %s -> PEM\n", kind, loc)

	stem := strings.TrimSuffix(loc, filepath.Ext(loc))
	pem := stem + ".pem"
	p12 := loc // PKCS12 input is read directly by openssl

	if srcType == "JKS" {
		p12 = stem + ".p12"
		args := []string{"-importkeystore", "-srckeystore", loc, "-srcstoretype", "JKS",
			"-destkeystore", p12, "-deststoretype", "PKCS12"}
		if pw != "" {
			args = append(args, "-srcstorepass", pw, "-deststorepass", pw)
		}
		fmt.Fprintf(log, "  run: keytool %s\n", strings.Join(redactPass(args, pw), " "))
		if out, err := run("keytool", args...); err != nil {
			fmt.Fprintf(log, "  FAILED: keytool: %v %s; left as RESOLVE-REQUIRED\n", err, strings.TrimSpace(out))
			return keystoreFailed
		}
		fmt.Fprintf(log, "  ok:  wrote %s\n", p12)
	}

	oargs := []string{"pkcs12", "-in", p12, "-nodes"}
	if pw != "" {
		oargs = append(oargs, "-passin", "pass:"+pw)
	}
	if kind == "truststore" {
		oargs = append(oargs, "-nokeys")
	}
	oargs = append(oargs, "-out", pem)
	fmt.Fprintf(log, "  run: openssl %s\n", strings.Join(redactPass(oargs, pw), " "))
	if out, err := run("openssl", oargs...); err != nil {
		fmt.Fprintf(log, "  FAILED: openssl: %v %s; left as RESOLVE-REQUIRED\n", err, strings.TrimSpace(out))
		return keystoreFailed
	}
	fmt.Fprintf(log, "  ok:  wrote %s\n", pem)

	cfg.Settings[typeKey] = "PEM"
	cfg.Settings[locKey] = pem
	fmt.Fprintf(log, "  rewrote %s: %s -> PEM\n", typeKey, srcType)
	fmt.Fprintf(log, "  rewrote %s: %s -> %s\n", locKey, loc, pem)
	return keystoreConverted
}

// redactPass replaces the keystore password in a command's args before it is logged.
func redactPass(args []string, pw string) []string {
	if pw == "" {
		return args
	}
	out := make([]string, len(args))
	for i, a := range args {
		switch {
		case a == pw:
			a = "****"
		case strings.Contains(a, "pass:"+pw):
			a = strings.ReplaceAll(a, "pass:"+pw, "pass:****")
		}
		out[i] = a
	}
	return out
}
