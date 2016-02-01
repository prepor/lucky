package lucky

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	RequestsHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "lucky_requests_ms",
		Help:    "Lucky requests",
		Buckets: prometheus.ExponentialBuckets(1, 5, 6),
	},
		[]string{"backend", "method"})
	BackendsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lucky_backends",
		Help: "Lucky backends",
	},
		[]string{"backend"})

	FrontendsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lucky_frontends",
		Help: "Lucky frontends",
	},
		[]string{"frontend", "type"})
)

func init() {
	prometheus.MustRegister(RequestsHistogram)
	prometheus.MustRegister(BackendsGauge)
	prometheus.MustRegister(FrontendsGauge)
}
