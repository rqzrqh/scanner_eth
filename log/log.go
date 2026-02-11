package log

import (
	"fmt"
	"io"
	"runtime"
	"scanner_eth/config"
	"strings"

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

func addField(key, value string) {
	if len(key) == 0 {
		return
	}
	if len(value) == 0 {
		return
	}
	enableDefaultFieldMap = true
	defaultFieldMap[key] = value
}

func getHookLevel(logLevel string) []logrus.Level {
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		level = logrus.InfoLevel
	}

	return logrus.AllLevels[:level+1]
}

func InitLogger(name string, env string, config config.Log) error {

	if config.Console.Enable {
		addConsoleOut(config.Console.Level)
	}

	if config.File.Enable {
		addFileOut(config.File.Name, config.File.MaxSize, config.File.MaxBackups, config.File.MaxAge, config.File.Level)
	}

	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.TraceLevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: false,
		CallerPrettyfier: callerPrettyfier,
	})
	logrus.SetOutput(io.Discard)

	addField("app", name)
	addField("env", env)

	return nil
}
