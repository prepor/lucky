package lucky

import (
	"fmt"

	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

func handler(sys *System, w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
}

func httpServer(sys *System) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { handler(sys, w, r) })
	http.Handle("/metrics", prometheus.Handler())
	http.ListenAndServe(":7000", nil)
}
