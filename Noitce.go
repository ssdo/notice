package notice

import (
	"fmt"
	"github.com/ssgo/log"
	"github.com/ssgo/u"
	"strings"
	"time"
)

type Task struct {
	Targets   []string
	ChannelId string
	Type      string
	Url       string
	Key       string
	Secret    string
	Setting   string
	Template  string
	Params    map[string]interface{}
	Errors    []string
}

type Job struct {
	TraceId    string
	BusinessId string
	Tasks      []Task
	LastTime   string
	Times      int
}

// 将具体的发送信息存入队列
func Push(businessId, target string, params map[string]interface{}, logger *log.Logger) (traceId string) {
	if logger == nil {
		logger = log.DefaultLogger
	}
	if !inited {
		logger.Error("notice never init")
		return ""
	}

	job := makeJob(businessId, target, params, logger)

	rd := Config.Redis.CopyByLogger(logger)
	if rd.LPUSH("_NOTICE_JOBS", u.Json(job)) == 0 {
		return ""
	}

	return job.TraceId
}

// 直接发送
func Send(businessId, target string, params map[string]interface{}, logger *log.Logger) (succeed bool, job *Job) {
	if logger == nil {
		logger = log.DefaultLogger
	}
	if !inited {
		logger.Error("notice never init")
		return false, nil
	}

	job = makeJob(businessId, target, params, logger)

	succeed = send(job, logger)
	return
}

func makeJob(businessId, target string, params map[string]interface{}, logger *log.Logger) *Job {
	db := Config.DB.CopyByLogger(logger)

	// 读取当前业务支持的渠道列表
	channelsStr := db.Query("SELECT `"+Config.BusinessTable.Channels+"` FROM `"+Config.BusinessTable.Table+"` WHERE `"+Config.BusinessTable.Id+"`=?", businessId).StringOnR1C1()
	channelsStr = strings.ReplaceAll(channelsStr, " ", "")
	if channelsStr == "" {
		logger.Error("business not exists", "businessId", businessId)
		return nil
	}

	// 获取各渠道下发送对象的ID
	targetsByChannel := Config.TargetsFetcher(target, channelsStr)
	if len(targetsByChannel) == 0 {
		logger.Error("no targets", "businessId", businessId)
		return nil
	}

	// 产生查询渠道所需的参数
	channelQueryArgs := make([]interface{}, 0)
	for channelId := range targetsByChannel {
		if channelId == "" {
			continue
		}
		channelQueryArgs = append(channelQueryArgs, channelId)
	}

	// 获取渠道信息
	channelInfos := map[string]map[string]string{}
	db.Query("SELECT `"+Config.ChannelTable.Id+"`,`"+Config.ChannelTable.Type+"`,`"+Config.ChannelTable.Url+"`,`"+Config.ChannelTable.Key+"`,`"+Config.ChannelTable.Secret+"`,`"+Config.ChannelTable.Setting+"` FROM `"+Config.ChannelTable.Table+"` WHERE `"+Config.ChannelTable.Id+"` IN "+db.InKeys(len(channelQueryArgs)), channelQueryArgs...).ToKV(&channelInfos)
	if len(channelInfos) == 0 {
		logger.Error("no channel", "businessId", businessId)
		return nil
	}

	// 获取模版信息
	templateByChannel := make(map[string]string)
	channelQueryArgs = append([]interface{}{businessId}, channelQueryArgs...)
	db.Query("SELECT `"+Config.TemplateTable.ChannelId+"`,`"+Config.TemplateTable.Template+"` FROM `"+Config.TemplateTable.Table+"` WHERE `"+Config.TemplateTable.BusinessId+"` = ? AND `"+Config.TemplateTable.ChannelId+"` IN "+db.InKeys(len(channelQueryArgs)-1), channelQueryArgs...).ToKV(&templateByChannel)
	if len(templateByChannel) == 0 {
		logger.Error("no template", "businessId", businessId)
		return nil
	}

	// 创建任务
	tasks := make([]Task, 0)
	for _, channelId := range strings.Split(channelsStr, ",") {
		// 发送对象
		targets := targetsByChannel[channelId]
		if targets == nil {
			continue
		}

		// 渠道信息
		channelInfo := channelInfos[channelId]
		if channelInfo == nil {
			continue
		}

		// 模版ID
		template := templateByChannel[channelId]
		if template == "" {
			continue
		}

		tasks = append(tasks, Task{
			Targets:   targets,
			ChannelId: channelId,
			Type:      channelInfo[Config.ChannelTable.Type],
			Url:       channelInfo[Config.ChannelTable.Url],
			Key:       channelInfo[Config.ChannelTable.Key],
			Secret:    channelInfo[Config.ChannelTable.Secret],
			Setting:   channelInfo[Config.ChannelTable.Setting],
			Template:  template,
			Params:    params,
			Errors:    make([]string, 0),
		})
	}

	if len(tasks) == 0 {
		return nil
	}

	return &Job{
		TraceId:    u.UniqueId(),
		BusinessId: businessId,
		Tasks:      tasks,
	}
}

func send(job *Job, logger *log.Logger) bool {
	// 按顺序处理任务，成功一个即终止
	succeed := false
	for _, task := range job.Tasks {

		// 发送器
		sender := _senders[task.Type]
		if sender == nil {
			continue
		}

		targets := make([]string, 0)
		for _, target := range task.Targets {
			if !TargetLimiter.Check(target, logger) {
				logger.Error("target limited", "target", target, "businessId", job.BusinessId, "traceId", job.TraceId, "channelId", task.ChannelId)
				continue
			}

			if !TargetKindLimiter.Check(target+task.Template, logger) {
				logger.Error("target kind limited", "target", target, "businessId", job.BusinessId, "traceId", job.TraceId, "channelId", task.ChannelId)
				continue
			}

			if !RepeatLimiter.Check(target+task.Template+u.Json(task.Params), logger) {
				logger.Error("repeat limited", "target", target, "businessId", job.BusinessId, "traceId", job.TraceId, "channelId", task.ChannelId)
				continue
			}

			targets = append(targets, target)
		}

		startTime := time.Now()
		err := sender(targets, task.Url, task.Key, u.DecryptAes(task.Secret, settedKey, settedIv), task.Setting, task.Template, task.Params)
		succeed = err == nil

		taskName := fmt.Sprintf("notice task %s:%s", job.BusinessId, task.ChannelId)
		memo := ""
		if err != nil {
			memo = err.Error()
			task.Errors = append(task.Errors, err.Error())
		}

		args := map[string]interface{}{
			"targets":  strings.Join(task.Targets, ","),
			"template": task.Template,
		}
		for k, v := range task.Params {
			args[k] = v
		}
		logger.Task(taskName, args, succeed, _node, startTime, log.MakeUesdTime(startTime, time.Now()), memo)

		if succeed {
			break
		}
	}

	return succeed
}
