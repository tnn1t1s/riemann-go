// Package rule holds the rule document, its compiler, and the nine
// combinators. A compiled Program is immutable and shared; an Instance holds
// the state one partition keeps for it.
package rule

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// The three values of a rule's `partition` key.
const (
	PartitionHost        = "host"
	PartitionHostService = "host,service"
	PartitionGlobal      = "global"
)

// Document is a parsed rule document, before compilation.
type Document struct {
	ID           string
	Owner        string
	Partition    string
	Match        string
	Enabled      bool
	ExpiresAt    float64
	HasExpiresAt bool

	// Hash is the SHA-256 of the canonical form, hex encoded.
	Hash string

	stream    any
	bindings  map[string]any
	canonical map[string]any
}

func decodeJSON(body []byte, into any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(into); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data after the JSON value")
	}
	return nil
}

// ParseDocument reads one rule document.
func ParseDocument(body []byte) (*Document, error) {
	var raw map[string]any
	if err := decodeJSON(body, &raw); err != nil {
		return nil, fmt.Errorf("rule document is not a JSON object: %v", err)
	}
	return fromMap(raw)
}

// ParseDocuments reads the array of rule documents a --rules file holds.
func ParseDocuments(body []byte) ([]*Document, error) {
	var raws []map[string]any
	if err := decodeJSON(body, &raws); err != nil {
		return nil, fmt.Errorf("expected a JSON array of rule documents: %v", err)
	}
	out := make([]*Document, 0, len(raws))
	for i, raw := range raws {
		d, err := fromMap(raw)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %v", i, err)
		}
		out = append(out, d)
	}
	return out, nil
}

func requiredString(raw map[string]any, key string) (string, error) {
	v, ok := raw[key]
	if !ok {
		return "", fmt.Errorf("rule key %q is required", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("rule key %q must be a string", key)
	}
	if s == "" {
		return "", fmt.Errorf("rule key %q must not be empty", key)
	}
	return s, nil
}

// SPEC-GAP: the rule document table lists the keys a rule may carry and does
// not say what a key outside it does. Chosen: it is kept in the stored
// document, takes part in the content hash, and has no effect.
func fromMap(raw map[string]any) (*Document, error) {
	if raw == nil {
		return nil, errors.New("rule document is not a JSON object")
	}
	d := &Document{Partition: PartitionHost, Enabled: true}
	var err error
	if d.ID, err = requiredString(raw, "id"); err != nil {
		return nil, err
	}
	if d.Owner, err = requiredString(raw, "owner"); err != nil {
		return nil, err
	}
	if d.Match, err = requiredString(raw, "match"); err != nil {
		return nil, err
	}
	stream, ok := raw["stream"]
	if !ok {
		return nil, errors.New(`rule key "stream" is required`)
	}
	d.stream = stream

	if v, ok := raw["partition"]; ok {
		s, isString := v.(string)
		if !isString {
			return nil, errors.New(`rule key "partition" must be a string`)
		}
		switch s {
		case PartitionHost, PartitionHostService, PartitionGlobal:
			d.Partition = s
		default:
			return nil, fmt.Errorf(`rule key "partition" is %q; expected "host", "host,service" or "global"`, s)
		}
	}
	if v, ok := raw["enabled"]; ok {
		b, isBool := v.(bool)
		if !isBool {
			return nil, errors.New(`rule key "enabled" must be a boolean`)
		}
		d.Enabled = b
	}
	if v, ok := raw["expires_at"]; ok && v != nil {
		f, isNumber := number(v)
		if !isNumber {
			return nil, errors.New(`rule key "expires_at" must be a number of seconds since epoch`)
		}
		d.ExpiresAt, d.HasExpiresAt = f, true
	}
	if v, ok := raw["bindings"]; ok && v != nil {
		m, isObject := v.(map[string]any)
		if !isObject {
			return nil, errors.New(`rule key "bindings" must be an object`)
		}
		d.bindings = m
	}

	// SPEC-GAP: property 14 hashes "the canonical form" without defining it.
	// Chosen: the document with `version` removed, since that key is
	// server-owned and a client's value is ignored, with `partition` and
	// `enabled` written at their defaults when absent, encoded as compact
	// JSON with object keys sorted. Numbers keep the text the client sent, so
	// 5 and 5.0 are different content.
	canonical := make(map[string]any, len(raw)+2)
	for k, v := range raw {
		if k == "version" {
			continue
		}
		canonical[k] = v
	}
	canonical["partition"] = d.Partition
	canonical["enabled"] = d.Enabled
	d.canonical = canonical
	encoded, err := marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("rule document cannot be canonicalised: %v", err)
	}
	sum := sha256.Sum256(encoded)
	d.Hash = hex.EncodeToString(sum[:])
	return d, nil
}

func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Stored returns the document as the rule endpoints write it: the canonical
// form plus the server-owned version.
func (d *Document) Stored(version int) map[string]any {
	out := make(map[string]any, len(d.canonical)+2)
	for k, v := range d.canonical {
		out[k] = v
	}
	out["version"] = version
	return out
}

func number(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case float64:
		return n, true
	}
	return 0, false
}
