package dashboard

import (
	"embed"
	"io/fs"
)

//go:embed all:web/dist
var webDist embed.FS

// webDistFS returns the embedded UI assets rooted at web/dist.
func webDistFS() fs.FS {
	sub, err := fs.Sub(webDist, "web/dist")
	if err != nil {
		// The embed directive guarantees web/dist exists, so this is
		// unreachable in a correctly built binary.
		panic(err)
	}
	return sub
}
