package event

import (
	"errors"
	"testing"
)

func TestDecodeBatch(t *testing.T) {
	d := Defaults{Time: 100, TTL: DefaultTTL}

	es, err := DecodeBatch([]byte(`{"host":"h","service":"s"}`), d)
	if err != nil || len(es) != 1 {
		t.Fatalf("single object: %v %v", es, err)
	}
	if es[0].Time != 100 || es[0].TTL != DefaultTTL || es[0].Metric != nil {
		t.Errorf("defaults not applied: %+v", es[0])
	}

	es, err = DecodeBatch([]byte(`[{"host":"h","service":"s","metric":0,"time":5,"ttl":0}]`), d)
	if err != nil {
		t.Fatal(err)
	}
	if es[0].Metric == nil || *es[0].Metric != 0 || es[0].Time != 5 || es[0].TTL != 0 {
		t.Errorf("explicit zeros lost: %+v", es[0])
	}

	_, err = DecodeBatch([]byte(`[{"service":"s"}]`), d)
	if !errors.Is(err, ErrMissingHost) {
		t.Errorf("want ErrMissingHost, got %v", err)
	}
	_, err = DecodeBatch([]byte(`{"host":"h"}`), d)
	if !errors.Is(err, ErrMissingService) {
		t.Errorf("want ErrMissingService, got %v", err)
	}
	if _, err = DecodeBatch([]byte(`nope`), d); err == nil {
		t.Error("bad json accepted")
	}
}
