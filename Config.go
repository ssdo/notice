package notice

import (
	"github.com/ssdo/utility"
	"github.com/ssgo/db"
	"github.com/ssgo/redis"
	"time"
)

var inited = false

var settedKey = []byte("?GQ$0K0GgLdO=f+~L68PLm$uhKr4'=tV")
var settedIv = []byte("VFs7@sK61cj^f?HZ")
var keysSetted = false

func SetEncryptKeys(key, iv []byte) {
	if !keysSetted {
		settedKey = key
		settedIv = iv
		keysSetted = true
	}
}

var TargetLimiter *utility.Limiter     // 用户频次限制（用户ID）
var TargetKindLimiter *utility.Limiter // 同一对象相同类型的频次限制（用户ID+模版编号）
var RepeatLimiter *utility.Limiter     // 重复的内容发送的频次限制（用户ID+模版编号+参数）

// 业务表
type BusinessTable struct {
	Table string // 表名
	Id    string // ID
	//Name     string // 名称
	Channels string // 支持的渠道列表，逗号间隔
}

// 渠道表
type ChannelTable struct {
	Table string // 表名
	Id    string // ID
	//Name    string // 名称
	Type    string // 类型
	Url     string // 发送地址
	Key     string // Key
	Secret  string // 密钥
	Setting string // 其他设置
}

// 模版表
type TemplateTable struct {
	Table      string // 表名
	BusinessId string // 业务ID
	ChannelId  string // 渠道ID
	Template   string // 模版编号
}

var _senders = map[string]func(targets []string, url, key, secret, setting string, template string, params map[string]interface{}) (err error){}

var Config = struct {
	Redis                   *redis.Redis                                                         // Redis连接池
	DB                      *db.DB                                                               // 数据库连接池
	TargetLimitDuration     time.Duration                                                        // 发送对象限制器时间间隔（用户ID）
	TargetLimitTimes        int                                                                  // 发送对象限制器时间单位内允许的次数（用户ID）
	TargetKindLimitDuration time.Duration                                                        // 同类限制器时间间隔（用户ID+模版编号）
	TargetKindLimitTimes    int                                                                  // 同类限制器时间单位内允许的次数（用户ID+模版编号）
	RepeatLimitDuration     time.Duration                                                        // 重复发送限制器时间间隔（用户ID+模版编号+参数）
	RepeatLimitTimes        int                                                                  // 重复发送限制器时间单位内允许的次数（用户ID+模版编号+参数）
	TargetsFetcher          func(target, channels string) (targetsByChannel map[string][]string) // 获取发送对象
	BusinessTable           BusinessTable
	ChannelTable            ChannelTable
	TemplateTable           TemplateTable
	BadJobAliveDays         int
}{
	Redis:                   nil,
	DB:                      nil,
	TargetLimitDuration:     5 * time.Minute,
	TargetLimitTimes:        10,
	TargetKindLimitDuration: 5 * time.Minute,
	TargetKindLimitTimes:    5,
	RepeatLimitDuration:     5 * time.Minute,
	RepeatLimitTimes:        3,
	TargetsFetcher:          DefaultTargetsFetcher, // 获取发送对象
	BadJobAliveDays:         10,
	BusinessTable: BusinessTable{
		Table:    "Business",
		Id:       "id",
		Channels: "channels",
	},
	ChannelTable: ChannelTable{
		Table:   "Channel",
		Id:      "id",
		Type:    "type",
		Url:     "url",
		Key:     "key",
		Secret:  "secret",
		Setting: "setting",
	},
	TemplateTable: TemplateTable{
		Table:      "Template",
		BusinessId: "businessId",
		ChannelId:  "channelId",
		Template:   "template",
	},
}

func Init() {
	if inited {
		return
	}
	inited = true

	if Config.Redis == nil {
		Config.Redis = redis.GetRedis("user", nil)
	}

	if Config.DB == nil {
		Config.DB = db.GetDB("user", nil)
	}

	TargetLimiter = utility.NewLimiter("Target", Config.TargetLimitDuration, Config.TargetLimitTimes, Config.Redis)
	TargetKindLimiter = utility.NewLimiter("TargetKind", Config.TargetKindLimitDuration, Config.TargetKindLimitTimes, Config.Redis)
	RepeatLimiter = utility.NewLimiter("Repeat", Config.RepeatLimitDuration, Config.RepeatLimitTimes, Config.Redis)
}

func RegisterSender(name string, sender func(targets []string, url, key, secret, setting string, template string, params map[string]interface{}) (err error)) {
	_senders[name] = sender
}
