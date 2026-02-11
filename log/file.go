package log

import (
	"fmt"
	"os"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

func addFileOut(name string, maxSize int, maxBackups int, maxAge int, level string) {
	logrus.AddHook(newFileHook(name, maxSize, maxBackups, maxAge, level))
}

func newFileHook(name string, maxSize int, maxBackups int, maxAge int, level string) *fileHook {
	plainFormatter := &nested.Formatter{
		NoFieldsColors:        true,
		CustomCallerFormatter: callerFormatter,
	}

	// rotate file logger
	fileLogger := &lumberjack.Logger{
		Filename:   name,
		MaxSize:    maxSize,
		MaxBackups: maxBackups,
		MaxAge:     maxAge,
		LocalTime:  true,
	}

	return &fileHook{fileLogger, plainFormatter, getHookLevel(level)}
}

type fileHook struct {
	fileLogger *lumberjack.Logger
	formatter  logrus.Formatter
	levels     []logrus.Level
}

func (c *fileHook) Fire(entry *logrus.Entry) error {
	if enableDefaultFieldMap {
		for key, value := range defaultFieldMap {
			if _, exist := entry.Data[key]; !exist {
				entry.Data[key] = value
			}
		}
	}

	formatBytes, err := c.formatter.Format(entry)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "unable format the log line on fileHook %s", err)
		return err
	}

	_, _ = c.fileLogger.Write(formatBytes)
	return nil
}

func (c *fileHook) Levels() []logrus.Level {
	return c.levels
}
