package aggregate

import "testing"

func TestCountSumMergeRoundtrip(t *testing.T) {
	c, _ := New(Count)
	c.Add(5)
	c.Add(9)
	if c.Value() != 2 {
		t.Fatalf("count=%d want 2", c.Value())
	}
	data, err := c.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	c2, _ := New(Count)
	if err := c2.Unmarshal(data); err != nil {
		t.Fatal(err)
	}
	c2.Add(1)
	if c2.Value() != 3 {
		t.Fatalf("count after load+add=%d", c2.Value())
	}

	s, _ := New(Sum)
	s.Add(4)
	s2, _ := New(Sum)
	s2.Add(6)
	s.Merge(s2)
	if s.Value() != 10 {
		t.Fatalf("sum merge=%d want 10", s.Value())
	}
	d, _ := s.Marshal()
	s3, _ := New(Sum)
	if err := s3.Unmarshal(d); err != nil || s3.Value() != 10 {
		t.Fatalf("sum roundtrip err=%v value=%d", err, s3.Value())
	}
}
