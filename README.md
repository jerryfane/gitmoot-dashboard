# gitmoot-dashboard

Read-only web backend for the [gitmoot](https://github.com/jerryfane/gitmoot)
orchestration dashboard. It serves a live orchestration-DAG UI and streams run
state from gitmoot's store.

This is a small Go module (stdlib only, no third-party dependencies). The React
UI lives under `web/` and is built into `web/dist`, which is embedded into the
binary; a minimal placeholder ships until the UI lands.

## Contract

The package exposes a frozen `DataSource` interface and an HTTP `Serve` entry
point (see `datasource.go`):

```go
type DataSource interface {
    Runs(ctx context.Context) ([]RunSummary, error)
    State(ctx context.Context, runID string) (State, error) // runID "" => active/most-recent
    Job(ctx context.Context, jobID string) (Node, error)
    Subscribe(ctx context.Context, runID string) (<-chan State, func(), error) // for SSE
}

func Serve(ds DataSource) http.Handler
```

HTTP surface:

| Route                   | Returns                              |
| ----------------------- | ------------------------------------ |
| `GET /api/runs`         | `[]RunSummary`                       |
| `GET /api/state?run=<id>` | `State`                            |
| `GET /api/job/{id}`     | `Node`                               |
| `GET /events?run=<id>`  | SSE stream of `State`                |
| everything else         | embedded static UI (SPA fallback)    |

The server is **read-only** and must never run in the gitmoot daemon path.

## Development

Serve the embedded UI against an in-memory fake feed:

```sh
go run ./cmd/gitmoot-dashboard-dev   # http://localhost:8099
```

Gate:

```sh
go build ./... && go vet ./... && go test ./...
```

## License

Apache-2.0. See [LICENSE](LICENSE).
