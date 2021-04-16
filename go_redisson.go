package go_redisson

import (
	"context"
	"github.com/cockroachdb/errors"

	"time"

	"github.com/go-redis/redis/v8"
	"github.com/satori/go.uuid"
)

var (
	luaRelease = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("redislock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("redislock: lock not held")
)

// RedisClient is a minimal client interface.
type RedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}

// Client wraps a redis client.
type Client struct {
	client *redis.Client
}

// New creates a new Client instance with a custom namespace.
func New(client *redis.Client) *Client {
	return &Client{client: client}
}

// Obtain tries to obtain a new lock using a key with the given TTL.
// May return ErrNotObtained if not successful.
func (c *Client) TryLock(ctx context.Context, key string, waitTime time.Duration, ttl time.Duration, opt *Options) (*Lock, error) {
	var field string
	var valueCtx context.Context
	lf := ctx.Value("lock_field")
	if lf == nil {
		field = uuid.NewV4().String()
		valueCtx = context.WithValue(ctx, "lock_field", field)
	} else {
		field = lf.(string)
		valueCtx = ctx
	}
	lock := &Lock{c.client, key, field, ttl, waitTime, valueCtx}
	deadlineCtx, cancel := context.WithDeadline(valueCtx, time.Now().Add(waitTime))
	defer cancel()
	var timer *time.Timer
	retry := opt.getRetryStrategy()
	for {
		ok, err := lock.obtain(deadlineCtx)
		if err != nil {
			return nil, err
		} else if ok {
			return lock, nil
		}
		// backoff: interval duration
		backoff := retry.NextBackoff()
		if backoff < 1 {
			return nil, ErrNotObtained
		}

		if timer == nil {
			timer = time.NewTimer(backoff)
			defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-deadlineCtx.Done():
			return nil, ErrNotObtained
		case <-timer.C:
			// retry
		}
	}
}

// --------------------------------------------------------------------

// Lock represents an obtained, distributed lock.
type Lock struct {
	client   *redis.Client
	Key      string
	field    string
	ttl      time.Duration
	waitTime time.Duration
	Ctx context.Context
}

// Obtain is a short-cut for New(...).Obtain(...).
func TryLock(ctx context.Context, client *redis.Client, key string, waitTime time.Duration, ttl time.Duration, opt *Options) (*Lock, error) {
	c := &Client{client}
	return c.TryLock(ctx, key, waitTime, ttl, opt)
}

// --------------------------------------------------------------------

// Options describe the options for the lock
type Options struct {
	// RetryStrategy allows to customise the lock retry strategy.
	// Default: do not retry
	RetryStrategy RetryStrategy
}

func (o *Options) getRetryStrategy() RetryStrategy {
	if o != nil && o.RetryStrategy != nil {
		return o.RetryStrategy
	}
	return LinearBackoff(time.Second)
}

// --------------------------------------------------------------------

// RetryStrategy allows to customise the lock retry strategy.
type RetryStrategy interface {
	// NextBackoff returns the next backoff duration.
	NextBackoff() time.Duration
}

type linearBackoff time.Duration

// LinearBackoff allows retries regularly with customized intervals
func LinearBackoff(backoff time.Duration) RetryStrategy {
	return linearBackoff(backoff)
}

// NoRetry acquire the lock only once.
func NoRetry() RetryStrategy {
	return linearBackoff(0)
}

func (r linearBackoff) NextBackoff() time.Duration {
	return time.Duration(r)
}

type limitedRetry struct {
	s RetryStrategy

	cnt, max int
}

// LimitRetry limits the number of retries to max attempts.
func LimitRetry(s RetryStrategy, max int) RetryStrategy {
	return &limitedRetry{s: s, max: max}
}

func (r *limitedRetry) NextBackoff() time.Duration {
	if r.cnt >= r.max {
		return 0
	}
	r.cnt++
	return r.s.NextBackoff()
}

