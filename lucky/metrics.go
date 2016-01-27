package lucky

import (
	"github.com/prometheus/client_golang/prometheus"
)

var requestsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "lucky_requests_ms",
	Help:    "Lucky requests",
	Buckets: prometheus.LinearBuckets(1, 50, 6),
})

func init() {
	prometheus.MustRegister(requestsHistogram)
}
