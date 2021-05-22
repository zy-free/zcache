package zcache

import (
	"database/sql"
	"github.com/gomodule/redigo/redis"
	"golang.org/x/sync/singleflight"
	"time"
)

type (
	ExecFn         func(conn *sql.DB) (sql.Result, error)
	IndexQueryFn   func(conn *sql.DB, v interface{}) (interface{}, error)
	PrimaryQueryFn func(conn *sql.DB, v, primary interface{}) error
	QueryFn        func(conn *sql.DB) error
)

type Conn struct {
	db             *sql.DB
	redis          *redis.Pool
	expiry         time.Duration
	notFoundExpiry time.Duration
	unstableExpiry Unstable
	singleFlight   *singleflight.Group
	errNotFound    error
}

func New(db *sql.DB, redis *redis.Pool, errNotFound error, opts ...Option) Conn {
	o := newOptions(opts...)
	return Conn{
		db:             db,
		redis:          redis,
		expiry:         o.Expiry,
		notFoundExpiry: o.NotFoundExpiry,
		unstableExpiry: NewUnstable(expiryDeviation),
		singleFlight:   &singleflight.Group{},
		errNotFound:    errNotFound,
	}
}
