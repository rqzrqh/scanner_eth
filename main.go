package main

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "scanner_eth",
		Usage: "ethereum scanner",
		Commands: []*cli.Command{
			cmdScanner,
			cmdReporter,
		},
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}
