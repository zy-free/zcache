package zcache

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

const (
	notFoundPlaceholder = "*"
	// make the expiry unstable to avoid lots of cached items expire at the same time
	// make the unstable expiry to be [0.95, 1.05] * seconds
	expiryDeviation = 0.05
)

var ErrPlaceholder = errors.New("placeholder")

func (cc Conn) delCache(ctx context.Context, keys ...string) (err error) {
	conn, err := cc.redis.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	var inkeys []interface{}
	for _, v := range keys {
		inkeys = append(inkeys, v)
	}
	_, err = conn.Do("DEL", inkeys...)
	return
}

func (cc Conn) getCache(ctx context.Context, key string, v interface{}) (err error) {
	conn, err := cc.redis.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	var data string

	data, err = redis.String(conn.Do("GET", key))
	if err != nil {
		if err == redis.ErrNil {
			return cc.errNotFound
		}
		return err
	}
	if data == notFoundPlaceholder {
		return ErrPlaceholder
	}

	err = json.Unmarshal([]byte(data), v)
	if err == nil {
		return
	}

	// delete invalid cache , do trace
	conn.Do("DEL", key)

	return cc.errNotFound
}

func (cc Conn) setCache(ctx context.Context, key string, v interface{}) (err error) {
	return cc.setCacheWithExpire(ctx, key, v, cc.expiry)
}

func (cc Conn) setCacheWithExpire(ctx context.Context, key string, v interface{}, expire time.Duration) (err error) {
	conn, err := cc.redis.GetContext(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	_, err = conn.Do("SET", key, v, "EX", int(cc.unstableExpiry.AroundDuration(expire).Seconds()))

	return
}

func (cc Conn) Exec(ctx context.Context, exec ExecFn, keys ...string) (sql.Result, error) {
	res, err := exec(cc.db)
	if err != nil {
		return nil, err
	}

	if err := cc.delCache(ctx, keys...); err != nil {
		return nil, err
	}

	return res, nil
}

func (cc Conn) ExecNoCache(q string, args ...interface{}) (sql.Result, error) {
	return cc.db.Exec(q, args...)
}

func (cc Conn) QueryRow(ctx context.Context, key string, v interface{}, query QueryFn) (err error) {

	var rr interface{}
	// 压测即可发现 同时会有很多访问，但是db只有一条，防止击穿
	rr, err, _ = cc.singleFlight.Do(key, func() (r interface{}, e error) {
		fmt.Println("do singleFlight")
		if e = cc.getCache(ctx, key, v); e != nil {
			if e == ErrPlaceholder {
				return nil, cc.errNotFound
			} else if e != cc.errNotFound {
				return nil, e
			}
			//prom.CacheMiss.Incr("member test")
			fmt.Println("do mysql")
			e = query(cc.db)
			fmt.Println("mysql:",v, e)
			if e != nil {
				if e == cc.errNotFound {
					_ = cc.setCacheWithExpire(ctx, key, notFoundPlaceholder, defaultNotFoundExpiry)
					return nil, cc.errNotFound
				}
				return nil, e
			}

			s, _ := json.Marshal(v)
			_ = cc.setCache(ctx, key, s)

		}

		return json.Marshal(v)
	})
	if err != nil {
		return err
	}

	return json.Unmarshal(rr.([]byte), v)
}

//func (cc CachedConn) QueryRowIndex(v interface{}, key string, keyer func(primary interface{}) string,
//	indexQuery IndexQueryFn, primaryQuery PrimaryQueryFn) error {
//	var primaryKey interface{}
//	var found bool
//
//	// if don't use convert numeric primary key into int64,
//	// then it will be represented as scientific notion, like 2e6
//	// which will make the cache doesn't match with the previous insert one
//	keyer = floatKeyer(keyer)
//	if err := cc.cache.TakeWithExpire(&primaryKey, key, func(val interface{}, expire time.Duration) (err error) {
//		primaryKey, err = indexQuery(cc.db, v)
//		if err != nil {
//			return
//		}
//
//		found = true
//		return cc.cache.SetCacheWithExpire(keyer(primaryKey), v, expire+cacheSafeGapBetweenIndexAndPrimary)
//	}); err != nil {
//		return err
//	}
//
//	if found {
//		return nil
//	}
//
//	return cc.cache.Take(v, keyer(primaryKey), func(v interface{}) error {
//		return primaryQuery(cc.db, v, primaryKey)
//	})
//}
//
//
//func floatKeyer(fn func(interface{}) string) func(interface{}) string {
//	return func(primary interface{}) string {
//		switch v := primary.(type) {
//		case float32:
//			return fn(int64(v))
//		case float64:
//			return fn(int64(v))
//		default:
//			return fn(primary)
//		}
//	}
//}
