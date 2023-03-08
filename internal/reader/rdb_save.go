package reader

import (
	"github.com/alibaba/RedisShake/internal/client"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/statistics"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)


func NewPSyncRDB(address string, username string, password string, isTls bool) *psyncReader {
	r := new(psyncReader)
	r.address = address
	r.client = client.NewRedisClient(address, username, password, isTls)
	r.rd = r.client.BufioReader()
	log.Infof("psyncRdb connected to redis successful. address=[%s]", address)
	return r
}

func (r *psyncReader) StartSaveRDB(path string) {
	r.clearPathDir(path)
	go r.sendReplconfAck()
	r.saveRDB2File(path)

}

func (r *psyncReader) clearPathDir(path string) {
	filePath := filepath.Dir(path)
	if filePath == "." {
		filePath = "./"
	}
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.PanicError(err)
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".rdb") || strings.HasSuffix(f.Name(), ".aof") {
			err = os.Remove(f.Name())
			if err != nil {
				log.PanicError(err)
			}
			log.Warnf("remove file. filename=[%s]", f.Name())
		}
	}
}

func (r *psyncReader) saveRDB2File(rdbSavePath string) {
	log.Infof("start save RDB. address=[%s]", r.address)
	argv := []string{"replconf", "listening-port", "10007"} // 10007 is magic number
	log.Infof("send %v", argv)
	reply := r.client.DoWithStringReply(argv...)
	if reply != "OK" {
		log.Warnf("send replconf command to redis server failed. address=[%s], reply=[%s], error=[]", r.address, reply)
	}

	// send psync
	argv = []string{"PSYNC", "?", "-1"}
	if r.elastiCachePSync != "" {
		argv = []string{r.elastiCachePSync, "?", "-1"}
	}
	r.client.Send(argv...)
	log.Infof("send %v", argv)
	// format: \n\n\n$<reply>\r\n
	for true {
		// \n\n\n$
		b, err := r.rd.ReadByte()
		if err != nil {
			log.PanicError(err)
		}
		if b == '\n' {
			continue
		}
		if b == '-' {
			reply, err := r.rd.ReadString('\n')
			if err != nil {
				log.PanicError(err)
			}
			reply = strings.TrimSpace(reply)
			log.Panicf("psync error. address=[%s], reply=[%s]", r.address, reply)
		}
		if b != '+' {
			log.Panicf("invalid psync reply. address=[%s], b=[%s]", r.address, string(b))
		}
		break
	}
	reply, err := r.rd.ReadString('\n')
	if err != nil {
		log.PanicError(err)
	}
	reply = strings.TrimSpace(reply)
	log.Infof("receive [%s]", reply)
	masterOffset, err := strconv.Atoi(strings.Split(reply, " ")[2])
	if err != nil {
		log.PanicError(err)
	}
	r.receivedOffset = int64(masterOffset)

	log.Infof("source db is doing bgsave. address=[%s]", r.address)
	statistics.Metrics.IsDoingBgsave = true

	timeStart := time.Now()
	// format: \n\n\n$<length>\r\n<rdb>
	for true {
		// \n\n\n$
		b, err := r.rd.ReadByte()
		if err != nil {
			log.PanicError(err)
		}
		if b == '\n' {
			continue
		}
		if b != '$' {
			log.Panicf("invalid rdb format. address=[%s], b=[%s]", r.address, string(b))
		}
		break
	}
	statistics.Metrics.IsDoingBgsave = false
	log.Infof("source db bgsave finished. timeUsed=[%.2f]s, address=[%s]", time.Since(timeStart).Seconds(), r.address)
	lengthStr, err := r.rd.ReadString('\n')
	if err != nil {
		log.PanicError(err)
	}
	lengthStr = strings.TrimSpace(lengthStr)
	length, err := strconv.ParseInt(lengthStr, 10, 64)
	if err != nil {
		log.PanicError(err)
	}
	log.Infof("received rdb length. length=[%d]", length)
	statistics.SetRDBFileSize(uint64(length))

	// create rdb file
	rdbFilePath := rdbSavePath
	log.Infof("create dump.rdb file. filename_path=[%s]", rdbFilePath)
	rdbFileHandle, err := os.OpenFile(rdbFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.PanicError(err)
	}

	// read rdb
	remainder := length
	const bufSize int64 = 32 * 1024 * 1024 // 32MB
	buf := make([]byte, bufSize)
	for remainder != 0 {
		readOnce := bufSize
		if remainder < readOnce {
			readOnce = remainder
		}
		n, err := r.rd.Read(buf[:readOnce])
		if err != nil {
			log.PanicError(err)
		}
		remainder -= int64(n)
		statistics.UpdateRDBReceivedSize(uint64(length - remainder))
		_, err = rdbFileHandle.Write(buf[:n])
		if err != nil {
			log.PanicError(err)
		}
	}
	err = rdbFileHandle.Close()
	if err != nil {
		log.PanicError(err)
	}
	log.Infof("save RDB finished. address=[%s], total_bytes=[%d]", r.address, length)
}
