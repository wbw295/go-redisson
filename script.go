package go_redisson

import (
	"context"
	"github.com/go-redis/redis/v8"
	"strconv"
)

const UNLOCK_MESSAGE = 0
const READ_UNLOCK_MESSAGE = 1
const channelPrefix = "redisson_lock__channel"

var lockScript = redis.NewScript(`
		if (redis.call('exists', KEYS[1]) == 0) then 
		redis.call('hincrby', KEYS[1], ARGV[2], 1); 
		redis.call('pexpire', KEYS[1], ARGV[1]); 
		return nil; 
		end; 
		if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then 
		redis.call('hincrby', KEYS[1], ARGV[2], 1); 
		redis.call('pexpire', KEYS[1], ARGV[1]); 
		return nil; 
		end; 
		return redis.call('pttl', KEYS[1]);
	`)

var unlockScript = redis.NewScript(`
		if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then 
		return nil;
		end; 
		local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); 
		if (counter > 0) then 
		redis.call('pexpire', KEYS[1], ARGV[2]); 
		return 0; 
		else 
		redis.call('del', KEYS[1]); 
		redis.call('publish', KEYS[2], ARGV[1]); 
		return 1; 
		end; 
		return nil;
	`)

func getChannelName(key string) string {
	return channelPrefix + ":{" + key + "}"
}

// https://blog.csdn.net/qq_41768400/article/details/106630059
func (l *Lock) obtain(ctx context.Context) (bool, error) {
	sha, err := lockScript.Load(ctx, l.client).Result()
	if err != nil {
		return false, err
	}
	ttlms := strconv.Itoa(int(l.ttl / 1e6))
	_, err = l.client.EvalSha(ctx, sha, []string{l.Key}, ttlms, l.field).Result()
	if err != nil {
		if err == redis.Nil {
			// success get lock
			return true, nil
		}
		return false, err
	}
	//log.DefaultSugar.Warnf("Failed to get lock %s, ttlms: %d", l.Key, result.(int64))
	return false, nil
}

func (l *Lock) Unlock() error {
	if l == nil {
		return nil
	}
	client := l.client
	sha, err := unlockScript.Load(client.Context(), client).Result()
	if err != nil {
		return err
	}
	ttlms := strconv.Itoa(int(l.ttl / 1e6))
	result, err := client.EvalSha(client.Context(), sha, []string{l.Key, getChannelName(l.Key)}, UNLOCK_MESSAGE, ttlms, l.field).Result()
	if err != nil {
		if err == redis.Nil {
			// no necessary unlock, because no hold lock
			return nil
		}
		return err
	}
	i := result.(int64)
	if i == 0 {
		// not complete unlock, counter -1
		//log.DefaultSugar.Infof("Reentrant lock %s: counter -1", l.Key)
	} else if i == 1 {
		// completed unlock
		//log.DefaultSugar.Infof("Unlocked lock %s", l.Key)
	}
	return nil
}
