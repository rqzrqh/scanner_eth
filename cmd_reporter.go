package main

import (
	"fmt"
	"os"
	"scanner_eth/internal/logreport"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
)

var cmdReporter = &cli.Command{
	Name:    "reporter",
	Aliases: []string{"logreport"},
	Usage:   "Generate scanner_eth log analysis report",
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:  "input",
			Usage: "input log file path, repeatable or comma-separated",
		},
		&cli.StringFlag{
			Name:  "output",
			Usage: "output report path",
			Value: "report.html",
		},
		&cli.StringFlag{
			Name:  "format",
			Usage: "report format: html",
			Value: "html",
		},
		&cli.StringFlag{
			Name:  "title",
			Usage: "report title",
			Value: "scanner_eth Log Analysis Report",
		},
		&cli.StringFlag{
			Name:  "since",
			Usage: "start time, RFC3339 or 2006-01-02 15:04:05",
		},
		&cli.StringFlag{
			Name:  "until",
			Usage: "end time, RFC3339 or 2006-01-02 15:04:05",
		},
	},
	Action: runReporter,
}

func runReporter(cctx *cli.Context) error {
	inputs := normalizeReporterInputs(cctx.StringSlice("input"))
	if len(inputs) == 0 {
		return cli.Exit("missing -input", 2)
	}
	if strings.ToLower(strings.TrimSpace(cctx.String("format"))) != "html" {
		return cli.Exit("only -format html is supported", 2)
	}

	since, err := parseOptionalTime(cctx.String("since"))
	if err != nil {
		return cli.Exit(fmt.Sprintf("invalid -since: %v", err), 2)
	}
	until, err := parseOptionalTime(cctx.String("until"))
	if err != nil {
		return cli.Exit(fmt.Sprintf("invalid -until: %v", err), 2)
	}

	report, err := logreport.AnalyzeFiles(inputs, logreport.Options{
		Title: cctx.String("title"),
		Since: since,
		Until: until,
	})
	if err != nil {
		return fmt.Errorf("analyze logs failed: %w", err)
	}

	output := cctx.String("output")
	f, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("create output failed: %w", err)
	}
	defer f.Close()

	if err := logreport.RenderHTML(f, report); err != nil {
		return fmt.Errorf("render report failed: %w", err)
	}
	fmt.Printf("report written to %s\n", output)
	return nil
}

func normalizeReporterInputs(rawInputs []string) []string {
	inputs := make([]string, 0, len(rawInputs))
	for _, raw := range rawInputs {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				inputs = append(inputs, part)
			}
		}
	}
	return inputs
}

func parseOptionalTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	var lastErr error
	for _, layout := range layouts {
		t, err := time.ParseInLocation(layout, raw, time.Local)
		if err == nil {
			return t, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}
