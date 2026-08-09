package main

// Two-tier help. "tibkafkatokof -h" prints the handful of flags most runs need plus an
// index of flag groups; "tibkafkatokof -h <group>" prints the flags in one group, and
// "-h all" prints every group. Flags absent from every group here are undocumented and
// never appear in any help output.

import (
	"flag"
	"fmt"
	"io"
	"strings"
)

// flagGroup is one section of the help: a topic keyword for "-h <name>", a heading, a
// one-line blurb for the group index, and the flags it covers.
type flagGroup struct {
	name  string
	title string
	blurb string
	flags []string
}

var flagGroups = []flagGroup{
	{
		name:  "core",
		title: "Core flags",
		blurb: "output location, realm name, data dir, server addresses, transport",
		flags: []string{
			"output-dir",
			"realm-name",
			"data-dir",
			"core-servers",
			"transport-type",
			"ftl-loglevel",
			"migration-config",
		},
	},
	{
		name:  "brokers",
		title: "Live broker fetch flags (-from-brokers*)",
		blurb: "read the config from running Kafka brokers instead of properties files",
		flags: []string{
			"from-brokers",
			"from-brokers-timeout-ms",
		},
	},
	{
		name:  "tls",
		title: "TLS and mTLS flags (-tls-*)",
		blurb: "server and client certificates, private keys, trust files",
		flags: []string{
			"tls-cert",
			"tls-key",
			"tls-key-password",
			"tls-ca",
			"tls-server-trust",
			"tls-client-cert",
			"tls-client-key",
			"tls-client-key-password",
		},
	},
	{
		name:  "oauth",
		title: "OAuth2 flags (-oauth-*)",
		blurb: "token/JWKS endpoints, claims, audience, server and UI client credentials",
		flags: []string{
			"oauth-token-url",
			"oauth-jwks-url",
			"oauth-client-id",
			"oauth-client-secret",
			"oauth-provider-trust",
			"oauth-audience",
			"oauth-claim-roles",
			"oauth-claim-username",
			"oauth-ui-auth-url",
			"oauth-ui-token-url",
			"oauth-ui-logout-url",
			"oauth-ui-client-id",
			"oauth-ui-client-secret",
		},
	},
	{
		name:  "auth",
		title: "Authentication flags",
		blurb: "users file, role map, and the FTL service credentials",
		flags: []string{
			"auth-users-file",
			"auth-rolemap",
			"server-user",
			"server-password",
			"realm-service-user",
			"realm-service-password",
		},
	},
	{
		name:  "dr",
		title: "Disaster recovery flags (-dr-*)",
		blurb: "DR server list and DR data directory",
		flags: []string{
			"dr-servers",
			"dr-data-dir",
		},
	},
	{
		name:  "info",
		title: "Inspection and conversion flags",
		blurb: "property listing, colorization, automatic keystore conversion",
		flags: []string{
			"list-properties",
			"color",
			"auto",
		},
	},
}

// commonFlags are the ones shown by a bare "-h": enough to run the tool without
// reading any group.
var commonFlags = []string{
	"output-dir",
	"realm-name",
	"data-dir",
	"core-servers",
	"auto",
	"list-properties",
}

// helpRequested reports whether args ask for help, and for which group. It runs before
// flag.Parse so that "-h oauth" works: the flag package would otherwise treat "oauth"
// as the first positional argument. "-h", "--help", "-help=tls" and "-h tls" are all
// accepted.
func helpRequested(args []string) (topic string, ok bool) {
	for i, a := range args {
		name, value, hasValue := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if strings.HasPrefix(a, "-") && (name == "h" || name == "help") {
			if hasValue {
				return value, true
			}
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				return args[i+1], true
			}
			return "", true
		}
	}
	return "", false
}

// topicList returns the group keywords for an error message.
func topicList() string {
	names := make([]string, 0, len(flagGroups)+1)
	for _, g := range flagGroups {
		names = append(names, g.name)
	}
	return strings.Join(append(names, "all"), ", ")
}

// writeHelp prints the overview (topic "") or one group's flags. It returns false for
// an unrecognized topic so the caller can report it.
func writeHelp(w io.Writer, topic string) bool {
	switch topic {
	case "":
		writeOverview(w)
		return true
	case "all":
		writeSynopsis(w)
		for _, g := range flagGroups {
			fmt.Fprintln(w)
			writeGroup(w, g)
		}
		return true
	}
	for _, g := range flagGroups {
		if g.name == topic {
			writeSynopsis(w)
			fmt.Fprintln(w)
			writeGroup(w, g)
			return true
		}
	}
	return false
}

func writeSynopsis(w io.Writer) {
	fmt.Fprintln(w, "Usage: tibkafkatokof [flags] <server.properties...>")
	fmt.Fprintln(w, "       tibkafkatokof [flags] -from-brokers host:port[,host:port...]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Translates one or more Kafka broker configurations into FTL KOF artifacts.")
	fmt.Fprintln(w, "Pass one server.properties file per broker (1-9 files), or use -from-brokers to")
	fmt.Fprintln(w, "read the config from running brokers over the Kafka Admin API. The two are")
	fmt.Fprintln(w, "mutually exclusive; pserver count is derived from the number of brokers.")
}

func writeOverview(w io.Writer) {
	writeSynopsis(w)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Output files:")
	fmt.Fprintln(w, "  kof-cluster.yaml          FTL pserver cluster configuration (primary, first 3 pservers)")
	fmt.Fprintln(w, "  kof-cluster-auxN.yaml     Additional pserver groups (one per group of 3 pservers beyond the first)")
	fmt.Fprintln(w, "  kof-cluster-secure.yaml   Secure variant with TLS/auth settings for FTL server")
	fmt.Fprintln(w, "  kof-cluster-dr.yaml       DR replica cluster (with -dr-servers)")
	fmt.Fprintln(w, "  realm.json                FTL realm configuration with kof.cluster")
	fmt.Fprintln(w, "  kof.broker.N.properties   Per-pserver broker properties (N is 1-based)")
	fmt.Fprintln(w)

	fmt.Fprintln(w, "Common flags:")
	printFlags(w, commonFlags)
	fmt.Fprintln(w)

	fmt.Fprintln(w, `Flag groups -- run "tibkafkatokof -h <group>" for the flags in one:`)
	width := 0
	for _, g := range flagGroups {
		if len(g.name) > width {
			width = len(g.name)
		}
	}
	for _, g := range flagGroups {
		fmt.Fprintf(w, "  %-*s  %s\n", width, g.name, g.blurb)
	}
	fmt.Fprintf(w, "  %-*s  %s\n", width, "all", "every flag, grouped")
	fmt.Fprintln(w)

	fmt.Fprintln(w, "Examples:")
	fmt.Fprintln(w, "  tibkafkatokof -output-dir ./out server.properties")
	fmt.Fprintln(w, "  tibkafkatokof -output-dir ./out \\")
	fmt.Fprintln(w, "      -core-servers SRV1=h1:5600,SRV2=h2:5601,SRV3=h3:5602 \\")
	fmt.Fprintln(w, "      server-1.properties server-2.properties server-3.properties")
	fmt.Fprintln(w, "  tibkafkatokof -output-dir ./out -from-brokers localhost:9092,localhost:9093")
	fmt.Fprintln(w, "  tibkafkatokof -h oauth")
}

func writeGroup(w io.Writer, g flagGroup) {
	fmt.Fprintf(w, "%s:\n", g.title)
	printFlags(w, g.flags)
}

// printFlags renders the named flags in the flag package's own layout, so the output
// looks like what flag.PrintDefaults would produce for that subset.
func printFlags(w io.Writer, names []string) {
	for _, n := range names {
		fl := flag.Lookup(n)
		if fl == nil {
			continue
		}
		valueType, usage := flag.UnquoteUsage(fl)
		line := "  -" + fl.Name
		if valueType != "" {
			line += " " + valueType
		}
		fmt.Fprintln(w, line)
		fmt.Fprint(w, "    \t", strings.ReplaceAll(usage, "\n", "\n    \t"))
		if !isZeroDefault(fl) {
			// Quote string defaults, print the rest bare -- what flag.PrintDefaults does.
			if valueType == "string" {
				fmt.Fprintf(w, " (default %q)", fl.DefValue)
			} else {
				fmt.Fprintf(w, " (default %v)", fl.DefValue)
			}
		}
		fmt.Fprintln(w)
	}
}

// isZeroDefault reports whether a flag's default is worth omitting from the help --
// the empty string, false for a boolean, or zero for a number.
func isZeroDefault(fl *flag.Flag) bool {
	switch fl.DefValue {
	case "", "false", "0":
		return true
	}
	return false
}
