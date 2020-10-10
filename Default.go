package notice

import "strings"

func DefaultTargetsFetcher(target, channels string) (targetsByChannel map[string][]string) {
	targetsByChannel = make(map[string][]string)
	targets := strings.Split(target, ",")
	for _, channelName := range strings.Split(channels, ",") {
		targetsByChannel[channelName] = targets
	}
	return
}
