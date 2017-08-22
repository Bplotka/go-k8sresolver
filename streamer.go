package k8sresolver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/jpillora/backoff"
	"github.com/pkg/errors"
)

type streamWatcher struct {
	target                  targetEntry
	epClient                endpointClient
	eventsCh                chan<- watchResult
	retryBackoff            *backoff.Backoff
	lastSeenResourceVersion int
}

func startWatchingEndpointsChanges(
	ctx context.Context,
	target targetEntry,
	epClient endpointClient,
	eventsCh chan<- watchResult,
	retryBackoff *backoff.Backoff,
	lastSeenResourceVersion int,
) *streamWatcher {
	w := &streamWatcher{
		target:                  target,
		epClient:                epClient,
		eventsCh:                eventsCh,
		retryBackoff:            retryBackoff,
		lastSeenResourceVersion: lastSeenResourceVersion,
	}
	go w.watch(ctx)
	return w
}

type eventType string

const (
	added    eventType = "ADDED"
	modified eventType = "MODIFIED"
	deleted  eventType = "DELETED"
	failed   eventType = "ERROR"
)

// event represents a single event to a watched resource.
type event struct {
	Type   eventType `json:"type"`
	Object endpoints `json:"object"`
}

// watch starts a stream and reads connection for every change event. If connection is broken (and ctx is still valid)
// it retries the stream. We read connection from separate go routine because read is blocking with no timeout/cancel logic.
// TODO(bplotka): Ugly method, refactor.
func (w *streamWatcher) watch(ctx context.Context) {
	// Retry stream loop.
	for ctx.Err() == nil {
		stream, err := w.epClient.StartChangeStream(ctx, w.target, w.lastSeenResourceVersion)
		if err != nil {
			fmt.Println(errors.Wrap(err, "k8sresolver stream: Failed to do start stream"))
			time.Sleep(w.retryBackoff.Duration())

			// TODO(bplotka): On X retry on failed, consider returning failed to Next() via watchResult that we
			// cannot connect.
			continue
		}

		err = w.proxyEvents(ctx, stream)
		if ctx.Err() != nil {
			return
		}

		if err != nil {
			fmt.Println(errors.Wrap(err, "k8sresolver stream: Error on read and proxy Events. Retrying"))
		}
	}
}

// proxyEvents is blocking method that gets events in loop and on success proxies to eventsCh.
// It ends only when context is cancelled and/or stream is broken.
func (w *streamWatcher) proxyEvents(ctx context.Context, stream io.ReadCloser) error {
	defer stream.Close()

	decoder := json.NewDecoder(stream)
	connectionErrCh := make(chan error)
	go func() {
		defer close(connectionErrCh)

		for {
			var got event

			// Blocking read.
			if err := decoder.Decode(&got); err != nil {
				if ctx.Err() != nil {
					// Stopping state.
					return
				}
				switch err {
				case io.EOF:
					// Watch closed normally - weird.
					connectionErrCh <- errors.Wrap(err, "EOF during watch stream event decoding")
					return
				case io.ErrUnexpectedEOF:
					connectionErrCh <- errors.Wrap(err, "Unexpected EOF during watch stream event decoding")
					return
				default:

				}
				// This is odd case. We return error as well as recreate stream.
				err := errors.Wrap(err, "Unable to decode an event from the watch stream")
				connectionErrCh <- err
				w.eventsCh <- watchResult{
					err: errors.Wrap(err, "Unable to decode an event from the watch stream"),
				}
				return
			}

			switch got.Type {
			case added, modified, deleted, failed:
				rv, err := strconv.Atoi(got.Object.Metadata.ResourceVersion)
				if err != nil {
					w.eventsCh <- watchResult{
						ep:  &got,
						err: err,
					}
					continue
				}
				w.lastSeenResourceVersion = rv
				w.eventsCh <- watchResult{
					ep: &got,
				}
			default:
				w.eventsCh <- watchResult{
					err: errors.Errorf("Got invalid watch event type: %v", got.Type),
				}
			}
		}

	}()

	// Wait until context is done or connection ends.
	select {
	case <-ctx.Done():
		// Stopping state.
		return ctx.Err()
	case err := <-connectionErrCh:
		return err
	}
}
