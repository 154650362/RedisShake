package client

import (
	"fmt"
	"github.com/alibaba/RedisShake/internal/log"
	"strconv"
	"testing"
	"time"
)

var address = "127.0.0.1:6379"
var username = ""
var password = ""
var isTls bool

//type RedisClient struct {
//	conn *net.Conn
//	redis *Redis
//}
//
//func NewRedis(conn *net.Conn, redis *Redis) *RedisClient {
//	r := NewRedisClient(address,username,password,isTls)
//	return &RedisClient{conn: conn,redis: r}
//}


func TestRedis_Receive(t *testing.T) {
	r := NewRedisClient(address,username,password,isTls)
	//r.DoWithStringReply()
	go func() {
		for i := 0; i < 10 ; i++ {
			key := strconv.Itoa(i)
			args := []string{"set",key,key}
			r.Send(args...)
		}
	}()

	time.Sleep(1 * time.Second)





	go func() {
		for i := 0; i < 10 ; i++ {
			key := strconv.Itoa(i)
			args := []string{"get",key}
			r.Send(args...)
		}
	}()


	for {
		replyInterface, err := r.Receive()
		if err != nil {
			log.PanicError(err)
		}
		reply := replyInterface.(string)
		fmt.Println("reply: ", reply)
	}




}
