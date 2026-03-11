package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
)

const (
	keyEnvelopeVersion = "kek-v1"
	keyEnvelopeKDF     = "argon2id"
	keyEnvelopeCipher  = "xchacha20poly1305"
	keyEnvelopeKeyID   = "default"
)

type encryptedKeyEnvelope struct {
	Version       string                  `json:"version"`
	KeyID         string                  `json:"key_id"`
	KDF           string                  `json:"kdf"`
	KDFParams     encryptedKeyEnvelopeKDF `json:"kdf_params"`
	Cipher        string                  `json:"cipher"`
	NonceHex      string                  `json:"nonce_hex"`
	CiphertextHex string                  `json:"ciphertext_hex"`
	AAD           string                  `json:"aad"`
	CreatedAtUnix int64                   `json:"created_at_unix"`
}

type encryptedKeyEnvelopeKDF struct {
	MemoryKiB   uint32 `json:"memory_kib"`
	TimeCost    uint32 `json:"time_cost"`
	Parallelism uint8  `json:"parallelism"`
	SaltHex     string `json:"salt_hex"`
}

func ensureKeyringTable(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS keyring_singleton(
		id INTEGER PRIMARY KEY CHECK(id=1),
		cipher_json TEXT NOT NULL,
		updated_at_unix INTEGER NOT NULL
	)`)
	return err
}

func loadEncryptedKeyEnvelope(db *sql.DB) (*encryptedKeyEnvelope, bool, error) {
	if err := ensureKeyringTable(db); err != nil {
		return nil, false, err
	}
	var raw string
	err := db.QueryRow(`SELECT cipher_json FROM keyring_singleton WHERE id=1`).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var env encryptedKeyEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return nil, true, fmt.Errorf("decode encrypted key envelope: %w", err)
	}
	return &env, true, nil
}

func saveEncryptedKeyEnvelope(db *sql.DB, env encryptedKeyEnvelope) error {
	if err := ensureKeyringTable(db); err != nil {
		return err
	}
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return err
	}
	_, err = db.Exec(
		`INSERT INTO keyring_singleton(id,cipher_json,updated_at_unix) VALUES(1,?,?)
		 ON CONFLICT(id) DO UPDATE SET cipher_json=excluded.cipher_json,updated_at_unix=excluded.updated_at_unix`,
		string(data),
		time.Now().Unix(),
	)
	return err
}

func encryptPrivateKeyEnvelope(appName, privHex, password string) (encryptedKeyEnvelope, error) {
	privHex = strings.ToLower(strings.TrimSpace(privHex))
	password = strings.TrimSpace(password)
	if password == "" {
		return encryptedKeyEnvelope{}, fmt.Errorf("password is required")
	}
	if _, err := normalizeRawSecp256k1PrivKeyHex(privHex); err != nil {
		return encryptedKeyEnvelope{}, err
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return encryptedKeyEnvelope{}, err
	}
	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := rand.Read(nonce); err != nil {
		return encryptedKeyEnvelope{}, err
	}
	memKiB := uint32(64 * 1024)
	timeCost := uint32(3)
	parallelism := uint8(4)
	k := argon2.IDKey([]byte(password), salt, timeCost, memKiB, parallelism, chacha20poly1305.KeySize)
	aead, err := chacha20poly1305.NewX(k)
	if err != nil {
		return encryptedKeyEnvelope{}, err
	}
	privRaw, err := hex.DecodeString(privHex)
	if err != nil {
		return encryptedKeyEnvelope{}, err
	}
	aad := fmt.Sprintf("bitfs-keyring|%s|%s|%s", strings.TrimSpace(appName), keyEnvelopeKeyID, keyEnvelopeVersion)
	ciphertext := aead.Seal(nil, nonce, privRaw, []byte(aad))
	for i := range k {
		k[i] = 0
	}
	for i := range privRaw {
		privRaw[i] = 0
	}
	return encryptedKeyEnvelope{
		Version:       keyEnvelopeVersion,
		KeyID:         keyEnvelopeKeyID,
		KDF:           keyEnvelopeKDF,
		KDFParams:     encryptedKeyEnvelopeKDF{MemoryKiB: memKiB, TimeCost: timeCost, Parallelism: parallelism, SaltHex: hex.EncodeToString(salt)},
		Cipher:        keyEnvelopeCipher,
		NonceHex:      hex.EncodeToString(nonce),
		CiphertextHex: hex.EncodeToString(ciphertext),
		AAD:           aad,
		CreatedAtUnix: time.Now().Unix(),
	}, nil
}

func decryptPrivateKeyEnvelope(appName string, env encryptedKeyEnvelope, password string) (string, error) {
	if strings.TrimSpace(password) == "" {
		return "", fmt.Errorf("password is required")
	}
	if !strings.EqualFold(strings.TrimSpace(env.Version), keyEnvelopeVersion) {
		return "", fmt.Errorf("unsupported key envelope version")
	}
	if !strings.EqualFold(strings.TrimSpace(env.KDF), keyEnvelopeKDF) {
		return "", fmt.Errorf("unsupported key kdf")
	}
	if !strings.EqualFold(strings.TrimSpace(env.Cipher), keyEnvelopeCipher) {
		return "", fmt.Errorf("unsupported key cipher")
	}
	salt, err := hex.DecodeString(strings.TrimSpace(env.KDFParams.SaltHex))
	if err != nil || len(salt) < 8 {
		return "", fmt.Errorf("invalid key kdf salt")
	}
	nonce, err := hex.DecodeString(strings.TrimSpace(env.NonceHex))
	if err != nil || len(nonce) != chacha20poly1305.NonceSizeX {
		return "", fmt.Errorf("invalid key nonce")
	}
	ciphertext, err := hex.DecodeString(strings.TrimSpace(env.CiphertextHex))
	if err != nil || len(ciphertext) == 0 {
		return "", fmt.Errorf("invalid key ciphertext")
	}
	memKiB := env.KDFParams.MemoryKiB
	timeCost := env.KDFParams.TimeCost
	parallelism := env.KDFParams.Parallelism
	if memKiB == 0 || timeCost == 0 || parallelism == 0 {
		return "", fmt.Errorf("invalid key kdf params")
	}
	k := argon2.IDKey([]byte(strings.TrimSpace(password)), salt, timeCost, memKiB, parallelism, chacha20poly1305.KeySize)
	aead, err := chacha20poly1305.NewX(k)
	if err != nil {
		return "", err
	}
	aad := strings.TrimSpace(env.AAD)
	if aad == "" {
		aad = fmt.Sprintf("bitfs-keyring|%s|%s|%s", strings.TrimSpace(appName), keyEnvelopeKeyID, keyEnvelopeVersion)
	}
	plain, err := aead.Open(nil, nonce, ciphertext, []byte(aad))
	for i := range k {
		k[i] = 0
	}
	if err != nil {
		return "", fmt.Errorf("invalid password or corrupted key material")
	}
	defer func() {
		for i := range plain {
			plain[i] = 0
		}
	}()
	if len(plain) != 32 {
		return "", fmt.Errorf("invalid password or corrupted key material")
	}
	return normalizeRawSecp256k1PrivKeyHex(hex.EncodeToString(plain))
}
