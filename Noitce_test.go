package notice_test

import (
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	notice "github.com/ssdo/user"
	"testing"
	"time"
)

func TestPush(t *testing.T) {

	notice.RegisterSender("WXPush", func(targets []string, url, key, secret, setting string, template string, params map[string]interface{}) (err error) {
		return errors.New("bad url")
	})

	notice.RegisterSender("LYSMS", func(targets []string, url, key, secret, setting string, template string, params map[string]interface{}) (err error) {
		return nil
	})

	notice.Init()

	traceId := notice.Push("hsau5D", "u1,g1,u2", map[string]interface{}{"1": "3721", "2": "3"}, nil)
	traceId = notice.Push("hsau5D", "u1,g1,u2", map[string]interface{}{"1": "3721", "2": "3"}, nil)

	notice.Start("node1", nil)
	time.Sleep(time.Millisecond * 100)

	// TODO 查看是否发送成功

	traceId = notice.Push("hsau5D", "u1,g1,u2222", map[string]interface{}{"1": "3721", "2": "3"}, nil)
	time.Sleep(time.Millisecond * 1200)

	notice.Stop()
	notice.Wait()
	fmt.Println(" ## 3", traceId)
}
