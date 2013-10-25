package kvstoreredis

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/igorsobreira/kvstore"
)

func init() {
	kvstore.Register("redis", &Driver{})
}

// Driver implementes kvstore.Driver interface using Redis
type Driver struct {
	cli  redis.Conn
	hash string
}

// Open is called by kvstore.New, will open a connection to redis
//
// info format is: "tcp://localhost:6379?hash=foo&timeout=5s"
//
func (d *Driver) Open(info string) error {

	network, address, hash, timeout, err := parseInfo(info)
	if err != nil {
		return err
	}

	if timeout != 0 {
		d.cli, err = redis.DialTimeout(network, address, timeout, timeout, timeout)
	} else {
		d.cli, err = redis.Dial(network, address)
	}

	d.hash = hash

	return err
}

func parseInfo(info string) (network string, address string, hash string, timeout time.Duration, err error) {

	var data *url.URL

	data, err = url.Parse(info)

	if err != nil {
		err = fmt.Errorf("kvstore redis: failed to parse info (%s)", err)
		return
	}

	network = strings.Replace(data.Scheme, "://", "", 1)
	address = data.Host

	if h := data.Query().Get("hash"); h != "" {
		hash = h
	} else {
		hash = "kvstore"
	}

	if t := data.Query().Get("timeout"); t != "" {
		timeout, err = time.ParseDuration(t)
		if err != nil {
			err = fmt.Errorf("kvstore redis: invalid timeout format (%s)", err)
		}
	}
	return
}

// Set key associated to value. Override existing value.
func (d *Driver) Set(key string, value []byte) error {
	_, err := d.cli.Do("HSET", d.hash, key, value)
	return err
}

// Get value associated with key. Return kvstore.ErrNotFound if
// key doesn't exist
func (d *Driver) Get(key string) (value []byte, err error) {
	reply, err := d.cli.Do("HGET", d.hash, key)
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
func (d *Driver) Delete(key string) error {
	_, err := d.cli.Do("HDEL", d.hash, key)
	return err
}
