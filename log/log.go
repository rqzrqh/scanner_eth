package log

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync_eth/config"

	"github.com/sirupsen/logrus"
)

var (
	enableDefaultFieldMap = false
	defaultFieldMap       = make(map[string]string)
)

func callerPrettyfier(f *runtime.Frame) (string, string) {
	fileName := fmt.Sprintf("%s %d", f.File, f.Line)
	funcName := f.Function
	list := strings.Split(funcName, "/")
	if len(list) > 0 {
		funcName = list[len(list)-1]
	}
	return funcName, fileName
}

func callerFormatter(f *runtime.Frame) string {
	funcName, fileName := callerPrettyfier(f)
	return " @" + funcName + " " + fileName
}

func AddField(key, value string) {
	if len(key) == 0 {
		return
	}
	if len(value) == 0 {
		return
	}
	enableDefaultFieldMap = true
	defaultFieldMap[key] = value
}

func DisableDefaultConsole() {
	logrus.SetOutput(ioutil.Discard)
}

func getHookLevel(level int) []logrus.Level {
	if level < 0 || level > 5 {
		level = 5
	}
	return logrus.AllLevels[:level+1]
}

func init() {
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: false,
		CallerPrettyfier: callerPrettyfier,
	})
}

func Init(name string, env string, config config.Log) error {
	if config.Stdout.Enable {
		AddConsoleOut(config.Stdout.Level)
	}

	AddField("app", name)
	AddField("env", env)

	return nil
}
