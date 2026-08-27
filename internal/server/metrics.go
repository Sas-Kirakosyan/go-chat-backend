package server

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"go-chat-backend/internal/metrics"
)

// unroutedLabel is the route label for a request that matched no route.
//
// A 404 is asked for by whoever sent it — /wp-login.php, /.env, and whatever a
// scanner tries next. Labelling those with the real path would let a stranger
// create a new Prometheus time series per request, which is a memory leak with
// an open door in front of it. They all count as one route.
const unroutedLabel = "other"

// observeRequests measures every request: how many, how long, how many at
// once.
//
// It sits outermost, in front of the rate limiter and the auth check, so a
// refused request is counted too. "We are serving 20 requests per second" and
// "we are refusing 4000 per second" are both things you want to see, and a
// metric that only counts the requests that went well hides the outage.
func observeRequests() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		metrics.HTTPInFlight.Inc()

		// A defer, not a plain call after c.Next(), so a panic on the way
		// through still decrements the gauge. Otherwise every panic would leave
		// the in-flight count one higher forever, and after a while the graph
		// would show a busy server doing nothing.
		defer func() {
			metrics.HTTPInFlight.Dec()

			// FullPath is the ROUTE TEMPLATE — "/conversations/:id/messages" —
			// not the path that was requested. That is what keeps ten thousand
			// rooms down to one time series.
			route := c.FullPath()
			if route == "" {
				route = unroutedLabel
			}

			metrics.HTTPDuration.
				WithLabelValues(c.Request.Method, route).
				Observe(time.Since(start).Seconds())

			metrics.HTTPRequests.
				WithLabelValues(c.Request.Method, route, strconv.Itoa(c.Writer.Status())).
				Inc()
		}()

		c.Next()
	}
}
