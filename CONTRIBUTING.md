# Contributing

Thanks for looking. This project is maintained by one person, so here is an
honest statement of what to expect: bug reports with a reproduction get read and
usually acted on, small focused pull requests get reviewed, and large
architectural changes may sit while I think about them or be declined.

## Running things

```sh
go build ./...
go test ./...                 # unit tests plus end-to-end against a real broker
go test ./e2e/ -v             # just the end-to-end suite
go vet ./...
```

The end-to-end suite compiles the real binary, starts it as a subprocess, and
drives it with an independent Kafka client. It needs no Docker and no network.

To exercise the S3 cold tier against a real server:

```sh
docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin quay.io/minio/minio server /data
KAFKA_WIRE_TEST_S3_ENDPOINT=http://127.0.0.1:9000 go test ./internal/objstore/ -v
```

The object-store conformance suite runs the *same* assertions against the
filesystem backend and the S3 backend. If you add a backend, it must pass that
suite unchanged; that is the entire contract.

## What a good pull request looks like

- A test that fails before the change and passes after it. For anything touching
  the protocol, prefer a test in `e2e/` that drives a real client, because the
  bugs that matter here are the ones a real client sees and a unit test does not.
- No new dependency without a sentence explaining why the standard library is
  insufficient.
- Comments that explain *why*, especially for anything that looks arbitrary. A
  surprising line without a reason attached tends to get "simplified" later by
  someone who does not know what it was protecting against.

## Protocol changes

If you implement a new API, advertise it in `internal/wire/api_versions.go` and
add it to the dispatch switch. Two things are easy to get wrong and worth
checking explicitly:

- **Flexible versions.** If the version is flexible, the request header carries a
  tagged-fields section that must be consumed before the body decoder runs, and
  the response header needs its own tag byte. `internal/wire/frame_test.go`
  covers both directions.
- **Sentinel defaults.** Several protocol fields default to something other than
  zero (`-1`, `-2147483648`). Go's zero value is a legal-looking wrong answer
  that clients act on. Set them explicitly.

## Style

Standard `gofmt`. Keep the existing comment voice: plain sentences, no shouting,
and an explanation of the reasoning rather than a restatement of the code.
