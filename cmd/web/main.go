// Command web serves the dev client in web/ over HTTP.
//
// It exists because the page cannot be opened as a file. The API allows one
// browser origin — http://localhost:5173 — in two separate places: the CORS
// middleware in internal/server/routes.go, and the WebSocket CheckOrigin in
// internal/server/ws.go. A page opened with file:// sends "Origin: null" and
// is refused by both, with an error that looks like a broken backend and is
// not one.
//
//	make run    # the API on :8080
//	make web    # this, on :5173
package main

import (
	"flag"
	"log"
	"net/http"
)

func main() {
	addr := flag.String("addr", "localhost:5173", "address to listen on")
	dir := flag.String("dir", "web", "directory to serve")
	flag.Parse()

	files := http.FileServer(http.Dir(*dir))

	// No caching. This is a page that is edited and reloaded all day, and a
	// cached copy of the JavaScript is a confusing way to lose an hour.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		files.ServeHTTP(w, r)
	})

	log.Printf("dev client on http://%s (serving %s)", *addr, *dir)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatal(err)
	}
}
