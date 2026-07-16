package translator

import (
	"reflect"
	"testing"
)

const plainModule = "org.apache.kafka.common.security.plain.PlainLoginModule required"

func TestResolveAuthorizer(t *testing.T) {
	cases := []struct {
		value     string
		recognize bool
	}{
		{"org.apache.kafka.metadata.authorizer.StandardAuthorizer", true},
		{"kafka.security.authorizer.AclAuthorizer", true},
		{"kafka.security.auth.SimpleAclAuthorizer", true},
		{"  org.apache.kafka.metadata.authorizer.StandardAuthorizer  ", true}, // trimmed
		{"com.acme.MyAuthorizer", false},
		{"", false},
	}
	for _, c := range cases {
		recognized, canonical := resolveAuthorizer(c.value)
		if recognized != c.recognize {
			t.Errorf("resolveAuthorizer(%q) recognized=%v, want %v", c.value, recognized, c.recognize)
		}
		if recognized && canonical != authorizerCanonical {
			t.Errorf("resolveAuthorizer(%q) canonical=%q, want %q", c.value, canonical, authorizerCanonical)
		}
	}
}

func TestResolveListenerAuth(t *testing.T) {
	cases := []struct {
		name        string
		jaas        string
		handler     string
		wantBackend AuthBackend
		wantRecog   bool
		wantUsers   []string
		wantGuess   AuthBackend
	}{
		{
			name:        "inline plain users (no handler)",
			jaas:        plainModule + ` username="admin" password="x" user_alice="a" user_bob="b";`,
			wantBackend: BackendInline,
			wantRecog:   true,
			wantUsers:   []string{"alice", "bob"}, // username= ignored
		},
		{
			name:        "recognized plain handler -> inline",
			jaas:        plainModule + ` user_carol="c";`,
			handler:     "org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler",
			wantBackend: BackendInline,
			wantRecog:   true,
			wantUsers:   []string{"carol"},
		},
		{
			name:        "recognized oauth validator -> oauth",
			handler:     "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler",
			wantBackend: BackendOauth,
			wantRecog:   true,
		},
		{
			name:        "unrecognized ldap-named handler -> flagged, no guess",
			handler:     "com.acme.LdapPlainServerCallbackHandler",
			wantBackend: BackendNone,
			wantRecog:   false,
			wantGuess:   BackendNone,
		},
		{
			name:        "unrecognized opaque handler -> flagged, no guess",
			handler:     "com.acme.MagicAuth",
			wantBackend: BackendNone,
			wantRecog:   false,
			wantGuess:   BackendNone,
		},
		{
			name:        "no jaas, no handler -> no source",
			wantBackend: BackendNone,
			wantRecog:   true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := resolveListenerAuth(c.jaas, c.handler)
			if got.Backend != c.wantBackend {
				t.Errorf("Backend = %q, want %q", got.Backend, c.wantBackend)
			}
			if got.Recognized != c.wantRecog {
				t.Errorf("Recognized = %v, want %v", got.Recognized, c.wantRecog)
			}
			if c.wantUsers != nil && !reflect.DeepEqual(got.InlineUsers, c.wantUsers) {
				t.Errorf("InlineUsers = %#v, want %#v", got.InlineUsers, c.wantUsers)
			}
			if got.Guess != c.wantGuess {
				t.Errorf("Guess = %q, want %q", got.Guess, c.wantGuess)
			}
		})
	}
}
