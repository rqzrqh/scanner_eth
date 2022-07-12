package log

import (
	"fmt"
	"os"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/sirupsen/logrus"
)

func AddConsoleOut(level int) {
	DisableDefaultConsole()
	logrus.AddHook(newConsoleHook(level))
}

type consoleHook struct {
	formatter logrus.Formatter
	levels    []logrus.Level
}

func (c *consoleHook) Fire(entry *logrus.Entry) error {
	if enableDefaultFieldMap {
		for key, value := range defaultFieldMap {
			if _, exist := entry.Data[key]; !exist {
				entry.Data[key] = value
			}
		}
	}

	formatBytes, err := c.formatter.Format(entry)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "unable format the log line on consoleHook %s", err)
		return err
	}

	fmt.Print(string(formatBytes))
	return nil
}

func (c *consoleHook) Levels() []logrus.Level {
	return c.levels
}

func newConsoleHook(level int) *consoleHook {
	plainFormatter := &nested.Formatter{
		NoFieldsColors:        true,
		CustomCallerFormatter: callerFormatter,
	}

	return &consoleHook{plainFormatter, getHookLevel(level)}
}
