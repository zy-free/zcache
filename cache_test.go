package zcache

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"sync"
	"testing"
	"time"

	// database driver
	_ "github.com/go-sql-driver/mysql"
)

func dbConnect(dataSourceName string) (*sql.DB, error) {
	d, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}
	d.SetMaxOpenConns(5)
	d.SetMaxIdleConns(2)
	d.SetConnMaxLifetime(time.Hour)
	return d, nil
}

func redisConnect(host string, pass string) *redis.Pool {
	return &redis.Pool{
		// 从配置文件获取maxidle以及maxactive，取不到则用后面的默认值
		MaxIdle:     3,
		MaxActive:   10,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host, redis.DialPassword(pass), redis.DialDatabase(0))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
}

var (
	cache Conn
	id      = int64(52)
	phone   = "157t3e3t311te"
	name    = "157tete"
	age     = 28
	address = "dahdua"
)

func TestMain(m *testing.M) {
	db, err := dbConnect("root:root@tcp(127.0.0.1:3306)/kartos_member?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true&loc=Local&charset=utf8,utf8mb4")
	if err != nil {
		panic(err)
	}
	cache = New(db, redisConnect("127.0.0.1:6379", ""), sql.ErrNoRows)
	m.Run()
}

func Test_Cache_Insert(t *testing.T) {
	var (
		err     error
	)
	query := `insert into member (id,phone,name,age,address) values (?,?, ?, ?, ?)`
	result, err := cache.ExecNoCache(query, id, phone, name, age, address)
	if err != nil {
		t.Fatal(err)
	}

	id, err = result.LastInsertId()
	if id == 0 || err != nil {
		t.Fatal("insert 0")
	}
}

func Test_Cache_QueryRow(t *testing.T) {
	type member struct {
		ID   int64
		Phone string
		Name string
	}
	var (
		err error
	)

	memberIdKey := fmt.Sprintf("test:member:id:%d", id)

	wg := sync.WaitGroup{}
	wg.Add(10)
	// get
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			m := member{}
			err = cache.QueryRow(context.TODO(), memberIdKey, &m, func(conn *sql.DB) error {
				query := `select id,phone,name from member where id = ? limit 1`
				return conn.QueryRow(query, id).Scan(&m.ID,&m.Phone, &m.Name)
			})
			fmt.Println(m, err)
		}()
	}

	wg.Wait()

	// getindex

}

func Test_Cache_Update(t *testing.T){

	memberIdKey := fmt.Sprintf("test:member:id:%d", id)
	memberPhoneKey := fmt.Sprintf("test:member:phone:%s", phone)
	_, err := cache.Exec(context.TODO(), func(conn *sql.DB) (result sql.Result, err error) {
		query := `update member set name = ? where id = ?`
		return conn.Exec(query, "update-name",id)
	}, memberPhoneKey, memberIdKey)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Cache_Delete(t *testing.T) {

	memberIdKey := fmt.Sprintf("test:member:id:%d", id)
	memberPhoneKey := fmt.Sprintf("test:member:phone:%s", phone)
	_, err := cache.Exec(context.TODO(), func(conn *sql.DB) (result sql.Result, err error) {
		query := `delete from member where id = ?`
		return conn.Exec(query, id)
	}, memberPhoneKey, memberIdKey)
	if err != nil {
		t.Fatal(err)
	}
}
