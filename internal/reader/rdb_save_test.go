package reader

import (
	"fmt"
	"github.com/alibaba/RedisShake/internal/log"
	"io/ioutil"
	"path/filepath"
	"testing"
)

var address = "192.168.30.201:36379"
var username = ""
var password = "CS_redisacl_703"
var isTls bool

func Test_Save(t *testing.T) {
	address = "127.0.0.1:6379"
	password = ""
	read := NewPSyncRDB(address,username,password,isTls)
	read.saveRDB2File("dump.rdb")

}

// 下面是备份端的日志
//11338:M 08 Mar 2023 11:27:55.675 * Replica 192.168.30.144:10007 asks for synchronization
//11338:M 08 Mar 2023 11:27:55.675 * Full resync requested by replica 192.168.30.144:10007
//11338:M 08 Mar 2023 11:27:55.675 * Starting BGSAVE for SYNC with target: disk
//11338:M 08 Mar 2023 11:27:55.675 * Background saving started by pid 20153
//20153:C 08 Mar 2023 11:27:55.695 * DB saved on disk
//20153:C 08 Mar 2023 11:27:55.695 * Fork CoW for RDB: current 0 MB, peak 0 MB, average 0 MB
//11338:M 08 Mar 2023 11:27:55.745 * Background saving terminated with success
//11338:M 08 Mar 2023 11:27:55.757 * Synchronization with replica 192.168.30.144:10007 succeeded
//11338:M 08 Mar 2023 11:27:55.815 # Connection with replica 192.168.30.144:10007 lost.

func Test_Clear(t *testing.T) {
	path := "dump.rdb"
	filePath := filepath.Dir(path)
	if filePath == "." {
		filePath = "./"
	}
	fmt.Printf("%#v\n", filePath)
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.PanicError(err)
	}
	fmt.Printf("%#v", files)
}