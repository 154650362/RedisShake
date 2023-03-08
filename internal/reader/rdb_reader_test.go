package reader

import (
	"fmt"
	"github.com/alibaba/RedisShake/internal/commands"
	"testing"
)

func TestNewRDBReader(t *testing.T) {
	reader := NewRDBReader("dump.rdb")

	ch := reader.StartRead()


	//for e := range ch {
	//	fmt.Printf("entry is %#v, string is %s", e, e.ToString())
	//}
	id := uint64(0)
	for e := range ch {
		fmt.Printf("UpdateInQueueEntriesCount is %d\n", uint64(len(ch)))
		// calc arguments
		e.Id = id
		id++
		e.CmdName, e.Group, e.Keys = commands.CalcKeys(e.Argv)
		e.Slots = commands.CalcSlots(e.Keys)
		fmt.Printf("entry is %#v, string is %s\n", e, e.ToString())

	}

}
