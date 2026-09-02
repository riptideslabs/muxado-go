package muxado

import (
	"testing"
	"time"
)

// TestWriteDoesNotReuseAnAbandonedFrame is a regression test for a data race on
// the one data frame each stream packs its writes into.
//
// The race:
//  1. A write packs the stream's frame and hands it to the writer goroutine
//  2. The writer blocks partway through writing it out
//  3. writeFrame gives up waiting, on the deadline here and on session death in
//     the wild, and returns with the frame still in the writer's hands
//  4. The next write on that stream packs the same frame, racing the writer
//
// The peer then reads a frame whose header and payload came from different
// writes, and loses the framing of everything after it. Run with -race.
func TestWriteDoesNotReuseAnAbandonedFrame(t *testing.T) {
	t.Parallel()

	local, remote := newFakeConnPair()
	t.Cleanup(func() { local.Close(); remote.Close() })

	str, err := Client(local, nil).OpenStream()
	if err != nil {
		t.Fatal(err)
	}

	if err := str.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	go str.Write([]byte("the frame the writer is holding"))

	// one read takes one write off the pipe, which is the frame's header: that
	// leaves the writer goroutine in the payload write, where it has already
	// read the frame's payload slice
	if _, err := remote.Read(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}

	// the deadline passes with the frame still out there. Nothing is waited on
	// here: a signal would order the writer's read before the write below,
	// which is the ordering whose absence is the bug.
	time.Sleep(300 * time.Millisecond)

	if err := str.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	str.Write([]byte("packed into the frame the writer is still holding"))
}
