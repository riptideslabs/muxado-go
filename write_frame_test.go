package muxado

import (
	"runtime"
	"testing"
	"time"

	"golang.ngrok.com/muxado/v2/frame"
)

// TestWriteFrameKeepsAWriteThatCompletedBeforeTheSessionDied is a regression
// test for a write that went out being reported as "session closed".
//
// The race:
//  1. A write hands its frame to the writer goroutine and waits for the writer to
//     report how it went, or for the session to die
//  2. The writer puts the frame on the wire and reports success
//  3. The peer answers and closes the connection, which kills the session
//  4. Only now does the goroutine that is waiting get to run, and finds both
//     channels ready: select chooses at random, and half of the time reports
//     "session closed" for a frame the peer has already received and answered
//
// A slow machine makes step 4 late enough to happen, which is how a client that
// is refused and disconnected straight away saw its first write fail. Here the
// ordering is forced: with one P, the writer goroutine's work and the death of the
// session are both done before the waiting goroutine runs again.
func TestWriteFrameKeepsAWriteThatCompletedBeforeTheSessionDied(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	for i := 0; i < 200; i++ {
		s := &session{
			writeFrames: make(chan writeReq),
			dead:        make(chan struct{}),
		}

		done := make(chan error, 1)

		go func() { done <- s.writeFrame(new(frame.Data), time.Time{}) }()

		// let the writeFrame goroutine run up to its wait to hand over the frame
		runtime.Gosched()

		req := <-s.writeFrames
		req.err <- nil
		close(s.dead)

		if err := <-done; err != nil {
			t.Fatalf("iteration %d: the write completed before the session died, but failed with: %v", i, err)
		}
	}
}
