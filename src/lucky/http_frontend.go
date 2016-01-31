package lucky

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

func NewHttpFrontend(sys *System, endpoint string) {
	http.Handle("/metrics", prometheus.Handler())
	go http.ListenAndServe(endpoint, nil)
}
