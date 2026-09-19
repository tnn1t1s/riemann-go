package rules

import (
	"sort"
	"sync"
	"sync/atomic"
)

// Set is an immutable snapshot of the installed rules, in evaluation order.
//
// SPEC-GAP: the spec does not pin the order rules are evaluated in, which
// decides the stored entry when two rules index one identity (SEMANTICS open
// question 9). Chosen: ascending rule id, so the order is at least the same
// on every run.
type Set struct {
	Rules []*Compiled
}

// Store holds the installed rule versions. Owners of rule state read the
// current Set through an atomic pointer and notice a change by identity.
type Store struct {
	mu       sync.Mutex
	rules    map[string]*Compiled
	versions map[string]int // survives DELETE so a version is monotonic per id
	current  atomic.Pointer[Set]
	Shared   *Shared
}

// NewStore returns an empty store.
func NewStore(shared *Shared) *Store {
	s := &Store{rules: map[string]*Compiled{}, versions: map[string]int{}, Shared: shared}
	s.current.Store(&Set{})
	return s
}

// Current is the snapshot to evaluate against.
func (s *Store) Current() *Set { return s.current.Load() }

func (s *Store) publish() {
	set := &Set{Rules: make([]*Compiled, 0, len(s.rules))}
	for _, c := range s.rules {
		set.Rules = append(set.Rules, c)
	}
	sort.Slice(set.Rules, func(i, j int) bool { return set.Rules[i].ID < set.Rules[j].ID })
	s.current.Store(set)
}

// Put installs a document under id. When the canonical form hashes to the
// stored rule's hash it is a no-op returning the stored version; otherwise it
// compiles, increments the version and replaces the rule, whose previous
// state is not carried across. An error is always a *Error, a 400.
func (s *Store) Put(id string, body []byte) (c *Compiled, created bool, err error) {
	doc, err := ParseDoc(body)
	if err != nil {
		return nil, false, err
	}
	// SPEC-GAP: the spec says the document's `id` "matches the path segment"
	// and does not say what a mismatch or an absent `id` does. Chosen: both
	// are a 400, since `id` is a required key.
	docID, ok := doc["id"].(string)
	if !ok || docID == "" {
		return nil, false, errf("rule: id is required and must be a non-empty string")
	}
	if id != "" && docID != id {
		return nil, false, errf("rule: id %q does not match the path segment %q", docID, id)
	}
	canon, hash, err := Canonical(doc)
	if err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old := s.rules[docID]; old != nil && old.Hash == hash {
		return old, false, nil
	}
	compiled, err := Compile(canon, hash, s.versions[docID]+1)
	if err != nil {
		return nil, false, err
	}
	s.versions[docID] = compiled.Version
	s.rules[docID] = compiled
	s.publish()
	return compiled, true, nil
}

// Delete removes a rule. It reports whether the id was known.
func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rules[id]; !ok {
		return false
	}
	delete(s.rules, id)
	s.publish()
	return true
}

// Get returns the installed version of a rule, or nil.
func (s *Store) Get(id string) *Compiled {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rules[id]
}
