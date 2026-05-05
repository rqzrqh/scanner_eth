package main

import (
	"flag"
	"fmt"
	"os"
	"scanner_eth/internal/logreport"
	"strings"
	"time"
)

type inputList []string

func (l *inputList) String() string {
	return strings.Join(*l, ",")
}

func (l *inputList) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			*l = append(*l, part)
		}
	}
	return nil
}

func main() {
	var inputs inputList
	var output string
	var format string
	var title string
	var sinceRaw string
	var untilRaw string

	flag.Var(&inputs, "input", "input log file path, repeatable or comma-separated")
	flag.StringVar(&output, "output", "report.html", "output report path")
	flag.StringVar(&format, "format", "html", "report format: html")
	flag.StringVar(&title, "title", "scanner_eth Log Analysis Report", "report title")
	flag.StringVar(&sinceRaw, "since", "", "start time, RFC3339 or 2006-01-02 15:04:05")
	flag.StringVar(&untilRaw, "until", "", "end time, RFC3339 or 2006-01-02 15:04:05")
	flag.Parse()

	if len(inputs) == 0 {
		fmt.Fprintln(os.Stderr, "missing -input")
		os.Exit(2)
	}
	if strings.ToLower(strings.TrimSpace(format)) != "html" {
		fmt.Fprintln(os.Stderr, "only -format html is supported")
		os.Exit(2)
	}

	since, err := parseOptionalTime(sinceRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -since: %v\n", err)
		os.Exit(2)
	}
	until, err := parseOptionalTime(untilRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -until: %v\n", err)
		os.Exit(2)
	}

	report, err := logreport.AnalyzeFiles(inputs, logreport.Options{
		Title: title,
		Since: since,
		Until: until,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "analyze logs failed: %v\n", err)
		os.Exit(1)
	}

	f, err := os.Create(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create output failed: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	if err := logreport.RenderHTML(f, report); err != nil {
		fmt.Fprintf(os.Stderr, "render report failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("report written to %s\n", output)
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
