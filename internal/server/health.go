package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Three endpoints, three different questions.
//
//	/livez   Is this process alive? Restart me if not.
//	/readyz  Should traffic come to me right now? Take me out if not.
//	/health  What is going on in there? For a human.
//
// Mixing the first two is the classic mistake, and it is expensive. If the
// liveness probe checks the database, then a database that goes down makes
// Kubernetes kill every API pod, over and over, in a restart loop — and none
// of that helps, because the broken thing is the database. The pods were fine.
//
// Split, the behaviour is right: the database goes down, /readyz starts
// failing, the pods are taken out of the load balancer, nothing is restarted,
// and the moment the database comes back they are put in again on their own.

// livezHandler answers "is the process running and able to serve HTTP?".
//
// It deliberately checks nothing. Its answer arriving at all IS the check: the
// process is up, the accept loop is working, and a goroutine got scheduled to
// write this. If the process were deadlocked or out of memory, nothing would
// come back, which is exactly the signal the caller wants.
func (s *Server) livezHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "alive"})
}

// readyzHandler answers "should this node be sent traffic right now?".
//
// Two things can make the answer no:
//
//  1. We are shutting down. This is the important one. It goes false at the
//     very start of Shutdown, before the drain, so the load balancer stops
//     sending new requests to a node that is about to close. Without it every
//     deploy drops a handful of requests into a dying process.
//  2. The database is unreachable. Nearly every route needs it, so answering
//     them here would only produce 500s.
func (s *Server) readyzHandler(c *gin.Context) {
	if s.draining.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "shutting down"})
		return
	}

	if status := s.db.Health()["status"]; status != "up" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "database": status})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// healthHandler is the detailed one: the pool counters, for a person looking
// at a server that feels slow. Stage 1 found the real bottleneck here, in
// wait_count and wait_duration, and those numbers are now on /metrics as well
// so they can be graphed over time instead of read once.
func (s *Server) healthHandler(c *gin.Context) {
	stats := s.db.Health()
	if stats["status"] != "up" {
		c.JSON(http.StatusServiceUnavailable, stats)
		return
	}
	c.JSON(http.StatusOK, stats)
}
