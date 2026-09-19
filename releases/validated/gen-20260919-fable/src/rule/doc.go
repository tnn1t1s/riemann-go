// Package rule holds the rule document, its compiler, and the combinator
// runtime. It knows nothing about shards, HTTP or any sink client.
package rule

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// Partition values a rule document may declare.
const (
	PartitionHost        = "host"
	PartitionHostService = "host,service"
	PartitionGlobal      = "global"
)

// Doc is a parsed rule document.
type Doc struct {
	ID        string
	Owner     string
	Partition string
	Match     string
	Stream    json.RawMessage
	Bindings  map[string]json.RawMessage
	Enabled   bool
	ExpiresAt *float64

	// Hash is the SHA-256 of the canonical form: the document with version
	// removed, defaults filled in, and object keys sorted.
	// SPEC-GAP: the spec says "canonical form" without defining it; this is
	// the definition used.
	Hash string

	canon map[string]any
}

// ParseDoc parses and canonicalizes a rule document. pathID is the {id} path
// segment on PUT, or empty when the document comes from the --rules file.
// SPEC-GAP: a PUT body with no id takes the id from the path; a body whose id
// differs from the path is a 400.
func ParseDoc(body []byte, pathID string) (*Doc, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("rule document is not a JSON object: %v", err)
	}
	delete(m, "version") // server-owned; a client-supplied version is ignored
	if _, ok := m["id"]; !ok && pathID != "" {
		m["id"] = pathID
	}
	if _, ok := m["partition"]; !ok {
		m["partition"] = PartitionHost
	}
	if _, ok := m["enabled"]; !ok {
		m["enabled"] = true
	}

	d := &Doc{canon: m}
	var err error
	if d.ID, err = reqString(m, "id"); err != nil {
		return nil, err
	}
	if pathID != "" && d.ID != pathID {
		return nil, fmt.Errorf("id %q does not match the path segment %q", d.ID, pathID)
	}
	if d.Owner, err = reqString(m, "owner"); err != nil {
		return nil, err
	}
	if d.Match, err = reqString(m, "match"); err != nil {
		return nil, err
	}
	if d.Partition, err = reqString(m, "partition"); err != nil {
		return nil, err
	}
	switch d.Partition {
	case PartitionHost, PartitionHostService, PartitionGlobal:
	default:
		return nil, fmt.Errorf("partition %q is not one of host, host,service, global", d.Partition)
	}
	en, ok := m["enabled"].(bool)
	if !ok {
		return nil, errors.New("enabled must be a boolean")
	}
	d.Enabled = en
	if v, ok := m["expires_at"]; ok && v != nil {
		n, ok := v.(json.Number)
		if !ok {
			return nil, errors.New("expires_at must be a number")
		}
		f, err := n.Float64()
		if err != nil {
			return nil, errors.New("expires_at must be a number")
		}
		d.ExpiresAt = &f
	}
	st, ok := m["stream"]
	if !ok || st == nil {
		return nil, errors.New("stream is required")
	}
	if d.Stream, err = json.Marshal(st); err != nil {
		return nil, err
	}
	if b, ok := m["bindings"]; ok && b != nil {
		bm, ok := b.(map[string]any)
		if !ok {
			return nil, errors.New("bindings must be an object")
		}
		d.Bindings = make(map[string]json.RawMessage, len(bm))
		for name, sub := range bm {
			raw, err := json.Marshal(sub)
			if err != nil {
				return nil, err
			}
			d.Bindings[name] = raw
		}
	}
	canon, err := json.Marshal(m) // encoding/json sorts object keys
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canon)
	d.Hash = hex.EncodeToString(sum[:])
	return d, nil
}

// Document returns the stored rule document with version, as a fresh map the
// caller may extend.
func (d *Doc) Document(version int) map[string]any {
	out := make(map[string]any, len(d.canon)+2)
	for k, v := range d.canon {
		out[k] = v
	}
	out["version"] = version
	return out
}

func reqString(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", fmt.Errorf("%s is required", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	if s == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return s, nil
}
