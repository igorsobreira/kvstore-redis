package kvstoreredis

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/igorsobreira/kvstore"
)

// Conn implements the kvstore.Conn interface wrapping a Redis connection
type Conn struct {
	c    redis.Conn
	hash string
}

// Set key associated to value. Override existing value.
func (c *Conn) Set(key string, value []byte) error {
	_, err := c.c.Do("HSET", c.hash, key, value)
	return err
}

// Get value associated with key. Return kvstore.ErrNotFound if
// key doesn't exist
func (c *Conn) Get(key string) (value []byte, err error) {
	reply, err := c.c.Do("HGET", c.hash, key)
	if reply == nil {
		return value, kvstore.ErrNotFound
	}
	value, ok := reply.([]byte)
	if !ok {
		return value, fmt.Errorf("kvstore redis: invalid value found for %#v: %#v", key, reply)
	}
	return value, err
}

// Delete key. No-op if key not found.
func (c *Conn) Delete(key string) error {
	_, err := c.c.Do("HDEL", c.hash, key)
	return err
}

// Close closed the redis connection.
func (c *Conn) Close() error {
	return c.c.Close()
}
