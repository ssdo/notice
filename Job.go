package notice

import (
	"github.com/ssgo/log"
	"github.com/ssgo/u"
	"time"
)

var _running = false
var _stopChan chan bool
var _node string

func Start(node string, logger *log.Logger) {
	_node = node
	if logger == nil {
		logger = log.DefaultLogger
	}
	if !inited {
		logger.Error("notice never init")
		return
	}

	_running = true
	go run(logger)
}

func run(logger *log.Logger) {
	rd := Config.Redis.CopyByLogger(logger)

	for {
		// 取出一个Job
		if r := rd.RPOP("_NOTICE_JOBS"); r.Error == nil && len(r.String()) > 0 {
			job := Job{}
			r.To(&job)

			succeed := send(&job, logger)

			// 失败的Job，放入失败队列
			if !succeed {
				now := time.Now()
				job.LastTime = now.Format("2006-01-02 15:04:05")
				job.Times++

				badKey := "_NOTICE_BAD_JOBS_" + now.Format("20060102")
				if rd.LPUSH(badKey, u.Json(job)) == 1 {
					rd.EXPIRE(badKey, 86400*Config.BadJobAliveDays)
				}
			}
		} else {
			time.Sleep(time.Second)
		}

		if !_running {
			break
		}
	}

	if _stopChan != nil {
		_stopChan <- true
	}
}

func Stop() {
	_stopChan = make(chan bool)
	_running = false
}

func Wait() {
	if _stopChan != nil {
		<-_stopChan
		_stopChan = nil
	}
}
