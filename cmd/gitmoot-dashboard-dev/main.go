// Command gitmoot-dashboard-dev serves the dashboard backed by a FakeDataSource
// for local development. It is not part of the production gitmoot binary.
package main

import (
	"log"
	"net/http"

	dashboard "github.com/jerryfane/gitmoot-dashboard"
)

const addr = ":8099"

func main() {
	handler := dashboard.Serve(dashboard.NewFakeDataSource())
	log.Printf("gitmoot-dashboard-dev listening on %s", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatal(err)
	}
}
