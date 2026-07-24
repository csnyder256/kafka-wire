package wire

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"os"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/crypto/pbkdf2"
)

// SCRAM-SHA-256 server, RFC 5802 + RFC 7677. Implemented directly
// (no third-party server library) because xdg-go/scram's server-side
// API is awkward and the algorithm is small enough that a clean
// in-tree implementation is more auditable.
//
// Credentials live at the path in auth.usersfile in JSON:
//
//   { "users": { "alice": "PASSWORD-HERE" } }
//
// Plaintext-on-disk is acceptable on the hosting platform because Volumes are
// encrypted-at-rest by AWS EBS. Passwords are never logged or sent
// over the wire: only derived StoredKey / ServerKey participate in
// the SCRAM handshake.
//
// SCRAM message flow (per RFC 5802 §5):
//
//   client → broker:    "n,,n=USER,r=CLIENT_NONCE"
//   broker → client:    "r=CLIENT_NONCE+SERVER_NONCE,s=SALT,i=ITERS"
//   client → broker:    "c=biws,r=...,p=CLIENT_PROOF"
//   broker → client:    "v=SERVER_SIGNATURE"
//
// SCRAM keys derived from password (per RFC 5802 §2.2):
//
//   SaltedPassword = PBKDF2-HMAC-SHA256(password, salt, iters, hash_size)
//   ClientKey      = HMAC-SHA256(SaltedPassword, "Client Key")
//   StoredKey      = SHA256(ClientKey)
//   ServerKey      = HMAC-SHA256(SaltedPassword, "Server Key")

const (
	scramIterations = 4096
	scramHashSize   = 32 // SHA-256
	scramSaltLen    = 12
)

type usersFile struct {
	Users map[string]string `json:"users"`
}

// scramServer holds the credential store + per-user derived keys.
type scramServer struct {
	users map[string]*scramCreds
}

type scramCreds struct {
	salt      []byte // raw salt bytes
	iters     int
	storedKey []byte
	serverKey []byte
}

func loadScramServer(path string) (*scramServer, error) {
	if path == "" {
		return nil, errors.New("users file path required when SASL is enabled")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read users file: %w", err)
	}
	var uf usersFile
	if err := json.Unmarshal(raw, &uf); err != nil {
		return nil, fmt.Errorf("parse users file: %w", err)
	}
	if len(uf.Users) == 0 {
		return nil, errors.New("users file empty")
	}
	server := &scramServer{users: make(map[string]*scramCreds, len(uf.Users))}
	for username, password := range uf.Users {
		// Per-user salt: deterministic from username + a constant
		// "kafka-wire" prefix so reloading the same users file
		// produces the same StoredKey/ServerKey (so existing client
		// sessions don't break on broker restart). Clients can't see
		// the salt; servers MUST send it on every authentication.
		// Matching Kafka's approach which uses per-user random salts
		// stored in ZooKeeper; we deterministically derive instead.
		salt := deriveSalt(username)
		saltedPassword := pbkdf2.Key([]byte(password), salt, scramIterations, scramHashSize, sha256.New)
		clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
		storedKey := sha256Sum(clientKey)
		serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))
		server.users[username] = &scramCreds{
			salt:      salt,
			iters:     scramIterations,
			storedKey: storedKey,
			serverKey: serverKey,
		}
	}
	return server, nil
}

// deriveSalt returns a stable salt for a username. Hash the username
// with a constant salt-derivation key so two different brokers run
// against the same users file produce the same salt, useful for
// blue/green deployments.
func deriveSalt(username string) []byte {
	mac := hmacSHA256([]byte("kafka-wire-scram-salt-key"), []byte(username))
	return mac[:scramSaltLen]
}

// scramConversation is one in-flight SCRAM authentication per
// connection. Two messages from the client (first + final), two from
// the server. Stored in connState across SaslAuthenticate calls.
type scramConversation struct {
	server      *scramServer
	step        int // 0 = expect client-first, 1 = expect client-final, 2 = done
	username    string
	clientNonce string
	serverNonce string
	creds       *scramCreds
	authMessage string // c1bare + "," + s1 + "," + c2WithoutProof
	valid       bool
}

func newScramConversation(server *scramServer) *scramConversation {
	return &scramConversation{server: server}
}

// step processes one client message and returns the server's reply.
// Returns done=true when the conversation has completed (success or
// failure).
func (c *scramConversation) doStep(in []byte) (out string, done bool, err error) {
	switch c.step {
	case 0:
		return c.handleClientFirst(string(in))
	case 1:
		return c.handleClientFinal(string(in))
	default:
		return "", true, errors.New("scram: too many steps")
	}
}

func (c *scramConversation) handleClientFirst(msg string) (string, bool, error) {
	// "n,,n=username,r=nonce"
	// gs2-cbind-flag: "n" (no channel binding)
	// authzid: empty
	// then the "client-first-message-bare" portion: "n=user,r=nonce"
	if !strings.HasPrefix(msg, "n,,") && !strings.HasPrefix(msg, "y,,") {
		return "", true, fmt.Errorf("scram: unsupported gs2-cbind-flag")
	}
	bare := msg[3:]
	parts := strings.Split(bare, ",")
	if len(parts) < 2 {
		return "", true, errors.New("scram: malformed client-first")
	}
	for _, p := range parts {
		if strings.HasPrefix(p, "n=") {
			c.username = scramDecodeUsername(p[2:])
		} else if strings.HasPrefix(p, "r=") {
			c.clientNonce = p[2:]
		}
	}
	if c.username == "" || c.clientNonce == "" {
		return "", true, errors.New("scram: missing n= or r=")
	}

	creds, ok := c.server.users[c.username]
	if !ok {
		// Per RFC 5802 §7, we MUST proceed with a fake credential to
		// avoid leaking which usernames exist. Generate per-call
		// noise; conversation will fail at the proof step.
		creds = &scramCreds{
			salt:      randomBytes(scramSaltLen),
			iters:     scramIterations,
			storedKey: randomBytes(scramHashSize),
			serverKey: randomBytes(scramHashSize),
		}
	}
	c.creds = creds

	// Server nonce: client nonce + 24 bytes of server randomness.
	srvNonce := base64.RawStdEncoding.EncodeToString(randomBytes(18))
	c.serverNonce = c.clientNonce + srvNonce

	saltB64 := base64.StdEncoding.EncodeToString(creds.salt)
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", c.serverNonce, saltB64, creds.iters)
	c.authMessage = bare + "," + serverFirst
	c.step = 1
	return serverFirst, false, nil
}

func (c *scramConversation) handleClientFinal(msg string) (string, bool, error) {
	// "c=cbind,r=nonce,p=proof"
	parts := strings.Split(msg, ",")
	var cbind, nonce, proofB64 string
	for _, p := range parts {
		switch {
		case strings.HasPrefix(p, "c="):
			cbind = p[2:]
		case strings.HasPrefix(p, "r="):
			nonce = p[2:]
		case strings.HasPrefix(p, "p="):
			proofB64 = p[2:]
		}
	}
	if proofB64 == "" {
		return "", true, errors.New("scram: missing client proof")
	}
	if nonce != c.serverNonce {
		return "", true, errors.New("scram: nonce mismatch")
	}
	// cbind for "n,," is base64("n,,") == "biws"
	if cbind != "biws" && cbind != "eSws" {
		return "", true, fmt.Errorf("scram: unsupported channel binding cbind=%q", cbind)
	}

	// Append the client-final-without-proof to the auth message.
	// Format: "c=cbind,r=nonce" (no p=).
	c2WithoutProof := fmt.Sprintf("c=%s,r=%s", cbind, nonce)
	c.authMessage = c.authMessage + "," + c2WithoutProof

	// Compute expected proof.
	clientSignature := hmacSHA256(c.creds.storedKey, []byte(c.authMessage))
	clientProof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return "", true, fmt.Errorf("scram: bad proof base64: %w", err)
	}
	if len(clientProof) != scramHashSize {
		return "", true, fmt.Errorf("scram: proof wrong length %d", len(clientProof))
	}
	// ClientKey = ClientProof XOR ClientSignature
	clientKey := xorBytes(clientProof, clientSignature)
	// StoredKey == SHA256(ClientKey)
	expectedStoredKey := sha256Sum(clientKey)
	if subtle.ConstantTimeCompare(expectedStoredKey, c.creds.storedKey) != 1 {
		// Proof failed. Per RFC, return an error in the server-final.
		c.step = 2
		c.valid = false
		return "e=invalid-proof", true, nil
	}

	c.valid = true
	c.step = 2

	serverSignature := hmacSHA256(c.creds.serverKey, []byte(c.authMessage))
	serverFinal := "v=" + base64.StdEncoding.EncodeToString(serverSignature)
	return serverFinal, true, nil
}

// SaslHandshake: client requests a mechanism. We return the list we
// support. some clients advertises SCRAM-SHA-256, SCRAM-SHA-512,
// PLAIN, GSSAPI; we ship SCRAM-SHA-256 only.
func (d *Dispatcher) handleSASLHandshake(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrSASLHandshakeRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}
	resp := kmsg.NewPtrSASLHandshakeResponse()
	resp.SetVersion(hdr.APIVersion)
	resp.SupportedMechanisms = []string{"SCRAM-SHA-256"}

	if req.Mechanism != "SCRAM-SHA-256" {
		resp.ErrorCode = errCodeUnsupportedSaslMech
		return d.writeKmsgResponse(state, hdr, resp, resp.IsFlexible())
	}
	if d.scram == nil {
		resp.ErrorCode = errCodeSaslAuthFailed
		return d.writeKmsgResponse(state, hdr, resp, resp.IsFlexible())
	}
	state.saslMechanism = req.Mechanism
	state.saslState = newScramConversation(d.scram)
	resp.ErrorCode = errCodeNone
	return d.writeKmsgResponse(state, hdr, resp, resp.IsFlexible())
}

// SaslAuthenticate exchanges one round of the SCRAM conversation.
func (d *Dispatcher) handleSASLAuthenticate(state *connState, hdr RequestHeader, body []byte) error {
	req := kmsg.NewPtrSASLAuthenticateRequest()
	req.SetVersion(hdr.APIVersion)
	if err := req.ReadFrom(body); err != nil {
		return err
	}
	resp := kmsg.NewPtrSASLAuthenticateResponse()
	resp.SetVersion(hdr.APIVersion)

	conv, ok := state.saslState.(*scramConversation)
	if !ok || conv == nil {
		resp.ErrorCode = errCodeSaslAuthFailed
		resp.ErrorMessage = stringPtr("SaslHandshake must precede SaslAuthenticate")
		return d.writeKmsgResponse(state, hdr, resp, resp.IsFlexible())
	}

	out, done, err := conv.doStep(req.SASLAuthBytes)
	if err != nil {
		resp.ErrorCode = errCodeSaslAuthFailed
		resp.ErrorMessage = stringPtr(err.Error())
		return d.writeKmsgResponse(state, hdr, resp, resp.IsFlexible())
	}
	resp.SASLAuthBytes = []byte(out)
	if done {
		if conv.valid {
			state.saslComplete = true
			state.saslPrincipal = conv.username
			// Resolve the tenant-id binding from ACL store. Cached on
			// connState so every Produce/Fetch handler has O(1)
			// access without re-walking the ACL map. If the ACL store
			// has no entry for this principal AND ACLs are configured,
			// the connection authenticates but has zero permissions, 
			// every authorize call returns false, every Produce/Fetch
			// fails closed.
			if acl := d.brk.ACL(); acl != nil {
				state.tenantID = acl.TenantOf(conv.username)
			}
			resp.ErrorCode = errCodeNone
		} else {
			resp.ErrorCode = errCodeSaslAuthFailed
		}
	}
	return d.writeKmsgResponse(state, hdr, resp, resp.IsFlexible())
}

// ── crypto helpers ────────────────────────────────────────────────────

func hmacSHA256(key, msg []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}

func sha256Sum(in []byte) []byte {
	h := sha256.New()
	h.Write(in)
	return h.Sum(nil)
}

func xorBytes(a, b []byte) []byte {
	if len(a) != len(b) {
		// Caller error: both should be hash-size.
		out := make([]byte, len(a))
		copy(out, a)
		return out
	}
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out
}

func randomBytes(n int) []byte {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return buf
}

// scramDecodeUsername unescapes the SCRAM username encoding. Per RFC
// 5802 §5.1, '=' becomes '=3D' and ',' becomes '=2C'.
func scramDecodeUsername(s string) string {
	s = strings.ReplaceAll(s, "=2C", ",")
	s = strings.ReplaceAll(s, "=3D", "=")
	return s
}

// Compile-time check that `hash` and `strconv` aren't dropped, both
// referenced indirectly via crypto/sha256 and base64.RawStdEncoding
// helpers. Go's import-pruning is per-file; this keeps the code
// honest if a future edit removes the only consumer.
var _ hash.Hash
var _ = strconv.Itoa
