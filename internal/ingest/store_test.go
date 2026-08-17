package ingest

import "testing"

func TestSpikeStreakOverridesAfterPersistence(t *testing.T) {
	// A corrupt first observation must not wedge the offer forever: a
	// price that keeps looking like a "spike" against the held junk is
	// the truth, and the streak accepts it.
	s := NewStore(nil)
	if s.noteSpike(1, 42) || s.noteSpike(1, 42) {
		t.Fatal("streak overrode before spikeAcceptAfter rejections")
	}
	if !s.noteSpike(1, 42) {
		t.Fatal("third consecutive spike not overridden")
	}
	// The override resets the streak.
	if s.noteSpike(1, 42) {
		t.Fatal("streak not reset after override")
	}
}

func TestSpikeStreakResetOnNormalAccept(t *testing.T) {
	s := NewStore(nil)
	s.noteSpike(1, 42)
	s.noteSpike(1, 42)
	s.clearSpike(1, 42) // a plausible price arrived in between
	if s.noteSpike(1, 42) {
		t.Fatal("streak survived a normal accept")
	}
}

func TestSpikeStreakKeyedPerRetailer(t *testing.T) {
	s := NewStore(nil)
	s.noteSpike(1, 42)
	s.noteSpike(1, 42)
	if s.noteSpike(2, 42) {
		t.Fatal("streak leaked across retailers")
	}
}
