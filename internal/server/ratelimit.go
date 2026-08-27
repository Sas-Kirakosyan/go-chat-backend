package server

import (
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"go-chat-backend/internal/metrics"
)

// Rate limits, and why there are two of them.
//
// The two groups of routes are attacked in completely different ways.
//
// /login and /register are guessed at: a script tries a thousand passwords
// against one account, or one password against a thousand accounts. There is
// no user id yet — that is the thing being guessed — so the only key available
// is the client's IP. The limit is low, because a real person logs in a
// handful of times a day, and every attempt costs the server a bcrypt hash on
// purpose.
//
// Everything behind AuthMiddleware is different. The caller is known, so the
// key is the user id, and one bad client cannot hurt anyone else by sharing an
// office IP with them. The limit is high, because a chat client legitimately
// bursts: opening the app fires a room list plus history for the room you were
// last in.
const (
	defaultAuthRPS   = 5
	defaultAuthBurst = 20

	defaultAPIRPS   = 20
	defaultAPIBurst = 40
)

// rateLimits is the configuration for both limiters. The zero value means
// "use the defaults", which is what keeps a hand-built Server (in a test)
// working instead of refusing every request.
type rateLimits struct {
	authRPS, authBurst float64
	apiRPS, apiBurst   float64
}

func (l rateLimits) orDefaults() rateLimits {
	if l.authRPS <= 0 || l.authBurst <= 0 {
		l.authRPS, l.authBurst = defaultAuthRPS, defaultAuthBurst
	}
	if l.apiRPS <= 0 || l.apiBurst <= 0 {
		l.apiRPS, l.apiBurst = defaultAPIRPS, defaultAPIBurst
	}
	return l
}

// rateLimitsFromEnv reads the four limits, falling back to the defaults.
//
// They are configurable because a limit that fits real users does not fit a
// load test: cmd/wsload logs in thousands of users from one machine, and at
// five logins a second that alone would take a quarter of an hour. Raising the
// numbers for a load run is honest; adding a back door that skips the limiter
// would mean load testing a server that is not the one we ship.
func rateLimitsFromEnv() rateLimits {
	return rateLimits{
		authRPS:   envFloat("RATE_LIMIT_AUTH_RPS", defaultAuthRPS),
		authBurst: envFloat("RATE_LIMIT_AUTH_BURST", defaultAuthBurst),
		apiRPS:    envFloat("RATE_LIMIT_API_RPS", defaultAPIRPS),
		apiBurst:  envFloat("RATE_LIMIT_API_BURST", defaultAPIBurst),
	}
}

func envFloat(key string, fallback float64) float64 {
	value, err := strconv.ParseFloat(os.Getenv(key), 64)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

// bucket is one caller's allowance.
//
// This is a token bucket. It holds up to burst tokens, refills at rate tokens
// per second, and a request costs one token. It is written down as a number
// and a timestamp instead of a real timer: nothing has to tick, and a caller
// that goes quiet costs nothing at all until it comes back.
//
// A token bucket is chosen over a fixed window ("100 requests per minute")
// because a fixed window lets a client spend its whole allowance in the last
// second of one window and again in the first second of the next — a double
// burst at exactly the wrong moment.
type bucket struct {
	tokens float64
	last   time.Time
}

// rateLimiter holds one bucket per caller.
//
// A mutex is right here, unlike in the hub. The hub does real work per event
// and would serialise a whole node behind one goroutine; this is a map lookup
// and two multiplications, and it is over in nanoseconds.
type rateLimiter struct {
	rate  float64 // tokens added per second
	burst float64 // the most tokens a bucket may hold

	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time

	// now is the clock, so a test can move time without sleeping.
	now func() time.Time
}

// sweepEvery is how often idle buckets are cleared out. See sweep.
const sweepEvery = time.Minute

func newRateLimiter(rate, burst float64) *rateLimiter {
	return &rateLimiter{
		rate:    rate,
		burst:   burst,
		buckets: make(map[string]*bucket),
		now:     time.Now,
	}
}

// allow takes a token for key. When there is none it returns false and how
// long the caller should wait for the next one.
func (l *rateLimiter) allow(key string) (bool, time.Duration) {
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.sweep(now)

	b, ok := l.buckets[key]
	if !ok {
		// A new caller starts full, so the first request is never refused.
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}

	// Refill for the time that has passed since we last looked. This is the
	// whole trick: no timer, no background goroutine, just arithmetic on a
	// timestamp.
	b.tokens = math.Min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now

	if b.tokens < 1 {
		wait := time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
		return false, wait
	}

	b.tokens--
	return true, 0
}

// sweep drops buckets that have refilled all the way.
//
// Without it the map grows forever: one entry per IP that ever touched the
// server, which is a slow memory leak an attacker can steer. A full bucket is
// exactly the same as no bucket — a new caller starts full — so deleting it
// gives the owner nothing they did not already have.
//
// It runs inline, under the lock the caller already holds, instead of in a
// background goroutine. A goroutine here would be one more thing to start,
// stop and get wrong at shutdown, for work that takes microseconds.
//
// The caller must hold l.mu.
func (l *rateLimiter) sweep(now time.Time) {
	if now.Sub(l.lastSweep) < sweepEvery {
		return
	}
	l.lastSweep = now

	for key, b := range l.buckets {
		if b.tokens+now.Sub(b.last).Seconds()*l.rate >= l.burst {
			delete(l.buckets, key)
		}
	}
}

// rateLimit refuses a caller that is going too fast.
//
// scope names the limiter in logs and metrics. keyFor decides what counts as
// one caller: the user id behind AuthMiddleware, the IP in front of it.
func rateLimit(l *rateLimiter, scope string, keyFor func(*gin.Context) string) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := keyFor(c)

		ok, wait := l.allow(key)
		if ok {
			c.Next()
			return
		}

		metrics.RateLimited.WithLabelValues(scope).Inc()

		// Retry-After is whole seconds, rounded up, and never zero: a client
		// that is told to wait 0 comes straight back.
		seconds := int(math.Ceil(wait.Seconds()))
		if seconds < 1 {
			seconds = 1
		}
		c.Header("Retry-After", strconv.Itoa(seconds))

		logFrom(c).Warn("rate limited",
			"scope", scope,
			"path", c.Request.URL.Path,
			"retry_after_s", seconds,
		)
		c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "Too many requests"})
	}
}

// userKey identifies a caller by user id. It must only be used behind
// AuthMiddleware, which is what puts the id on the context.
//
// The ip: fallback should be unreachable. It exists so that a route wired into
// the wrong group fails safe — limited by IP — instead of putting every
// unidentified caller into one shared "" bucket, where a single client would
// lock out everybody.
func userKey(c *gin.Context) string {
	if userID, _ := currentUser(c); userID != 0 {
		return "user:" + strconv.FormatUint(uint64(userID), 10)
	}
	return "ip:" + c.ClientIP()
}

// ipKey identifies a caller by address, for the routes where nobody has
// proved who they are yet.
//
// c.ClientIP reads X-Forwarded-For when the request came through a trusted
// proxy. Gin trusts every proxy by default, which means a client can currently
// claim any IP it likes by sending that header — so this limit is a speed bump
// against a plain script, not a defence against a determined attacker. Fixing
// it means setting r.SetTrustedProxies to the real proxy, and that address is
// only known once there is an nginx in front, in Stage 3.
func ipKey(c *gin.Context) string {
	return "ip:" + c.ClientIP()
}
