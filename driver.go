// Package kvstoreredis is a Redis driver for github.com/igorsobreira/kvstore
//
// See Open() for the info format required to kvstore.New().
//
// Make sure you call kvstore.Close() after kvstore.New().
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

// Driver implementes kvstore.Driver interface
type Driver struct{}

// Open is called by kvstore.New, will open a connection to redis
//
// info format is: "tcp://localhost:6379?hash=foo&timeout=5s".
//
// where 'hash' is the HASH in redis where the keys will be stored
func (d *Driver) Open(info string) (kvstore.Conn, error) {

	network, address, hash, timeout, err := parseInfo(info)
	if err != nil {
		return nil, err
	}

	var cli redis.Conn

	if timeout != 0 {
		cli, err = redis.DialTimeout(network, address, timeout, timeout, timeout)
	} else {
		cli, err = redis.Dial(network, address)
	}

	if err != nil {
		return nil, err
	}

	return &Conn{cli, hash}, nil
}

// parseInfo parses the info provided to Driver.Open and returns the individual
// items.
//
// note that both hash and timeout are option, so caller must verify if they
// were provided.
//
// network is returned as "tcp", even though it was provided as a url scheme "tcp://".
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
