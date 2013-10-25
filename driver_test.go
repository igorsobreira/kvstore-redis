package kvstoreredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
	"github.com/igorsobreira/kvstore"
	"github.com/igorsobreira/kvstore/testutil"
)

const (
	infoNoHash = "tcp://:6379?timeout=2s"
	info       = infoNoHash + "&hash=kvstore_test"
)

func TestRequiredAPI(t *testing.T) {
	testutil.TestRequiredAPI(t, teardown, "redis", info)
}

func TestDifferentHash(t *testing.T) {
	defer teardown()

	one, _ := kvstore.New("redis", infoNoHash+"&hash=one")
	two, _ := kvstore.New("redis", infoNoHash+"&hash=two")
	defer one.Close()
	defer two.Close()

	var val []byte
	var err error

	// key saved in hash 'one'...
	err = one.Set("foo", []byte("one"))
	if err != nil {
		t.Fatal("failed to set on 'one'", err)
	}

	// ...will not be available on 'two'
	val, err = two.Get("foo")
	if err != kvstore.ErrNotFound {
		t.Error("should not find key from 'one' on 'two'")
	}
	if len(val) != 0 {
		t.Errorf("should not find key from 'one' on 'two', found %q", val)
	}

	// deleting something from 'two'...
	err = two.Set("foo", []byte("two"))
	if err != nil {
		t.Fatal("failed to set on 'two'", err)
	}
	err = two.Delete("foo")
	if err != nil {
		t.Fatal("failed to delete from 'two'", err)
	}

	// ...will not affect 'one'
	val, err = one.Get("foo")
	if err != nil {
		t.Errorf("deleting something from 'two' should not affect 'one' (%s)", err)
	}
	if !testutil.ByteSliceEqual(val, []byte("one")) {
		t.Errorf("deleted 'foo' from 'two' and found %q on 'one'", val)
	}
}

func teardown() {
	cli, _ := redis.Dial("tcp", ":6379")
	for _, hash := range []string{"kvstore_test", "one", "two"} {
		_, err := cli.Do("DEL", hash)
		if err != nil {
			panic(err)
		}
	}
	cli.Close()
}
