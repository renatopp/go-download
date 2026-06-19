package main

import (
	"context"
	"io"
	"net/http"
	"os"

	"github.com/renatopp/cli-tools/download/downloader"
	"github.com/renatopp/go-cli"
	"github.com/renatopp/x/fmtx"
	"github.com/renatopp/x/strx"
)

func main() {
	cli.Name("download")
	cli.Description("Download one or more files from URLs.")
	cli.AutoHelp(true)

	outputFlag := cli.FlagString("output", "o", "Output file path (single URL only).").WithDefault("")
	dirFlag := cli.FlagString("dir", "d", "Output directory.").WithDefault(".")
	methodFlag := cli.FlagString("method", "m", "HTTP method to use.").WithDefault("GET")
	quietFlag := cli.FlagBool("quiet", "q", "Quiet mode (disable progress and verifications).").WithDefault(false)
	checksumFlag := cli.FlagString("checksum", "c", "Checksum algorithm (sha256|md5|none).").WithDefault("sha256")
	parallelFlag := cli.FlagInt("parallel", "p", "Number of parallel downloads.").WithDefault(4)
	failFastFlag := cli.FlagBool("fail-fast", "f", "Stop on first error.").WithDefault(false)
	retriesFlag := cli.FlagInt("retries", "r", "Number of retries per URL.").WithDefault(2)
	headersFlag := cli.FlagString("headers", "H", "Custom header. Repeatable: -H \"Key: Value\"").AsRepeatable()
	noAtomicFlag := cli.FlagBool("no-atomic", "", "Disable atomic write (write directly to output).").WithDefault(false)
	noResumeFlag := cli.FlagBool("no-resume", "", "Disable resumable downloads.").WithDefault(false)
	noProgressFlag := cli.FlagBool("no-progress", "", "Disable progress output.").WithDefault(false)
	urlsPos := cli.PosString("urls", "URLs to download.").AsVariadic()
	cli.Parse()

	headers, err := parseHeaders(headersFlag.Values())
	if err != nil {
		cli.Error("%s\n", err.Error())
		return
	}

	urls, err := collectURLs(urlsPos.Values())
	if err != nil {
		cli.Error("%s\n", err.Error())
		return
	}
	if len(urls) == 0 {
		cli.ShowHelp()
		return
	}

	cfg := downloader.Config{
		Urls:       urls,
		Output:     outputFlag.Value(),
		Dir:        dirFlag.Value(),
		Method:     methodFlag.Value(),
		Quiet:      quietFlag.Value(),
		Checksum:   checksumFlag.Value(),
		Parallel:   parallelFlag.Value(),
		FailFast:   failFastFlag.Value(),
		Retries:    retriesFlag.Value(),
		Headers:    headers,
		NoAtomic:   noAtomicFlag.Value(),
		NoResume:   noResumeFlag.Value(),
		NoProgress: noProgressFlag.Value(),
		Statusf: func(format string, args ...any) {
			cli.Print(format+"\n", args...)
		},
	}

	if err := downloader.Download(context.Background(), cfg); err != nil {
		cli.Error("%s\n", err.Error())
	}
}

func parseHeaders(rawHeaders []string) (http.Header, error) {
	headers := make(http.Header)
	for _, raw := range rawHeaders {
		parts := strx.SplitN(raw, ":", 2)
		if len(parts) != 2 {
			return nil, fmtx.Error("invalid header %q, expected \"Key: Value\"", raw)
		}

		key := strx.TrimSpace(parts[0])
		val := strx.TrimSpace(parts[1])
		if key == "" {
			return nil, fmtx.Error("invalid header %q, missing key", raw)
		}
		headers.Add(key, val)
	}
	return headers, nil
}

func collectURLs(args []string) ([]string, error) {
	urls := make([]string, 0, len(args))
	for _, raw := range args {
		u := strx.TrimSpace(raw)
		if u == "" {
			continue
		}
		urls = append(urls, u)
	}
	if len(urls) > 0 {
		return urls, nil
	}

	stat, err := os.Stdin.Stat()
	if err != nil {
		return nil, fmtx.Error("failed to inspect stdin: %w", err)
	}

	if (stat.Mode() & os.ModeCharDevice) != 0 {
		return nil, nil
	}

	buf, err := io.ReadAll(os.Stdin)
	if err != nil {
		return nil, fmtx.Error("failed reading stdin: %w", err)
	}

	for _, line := range strx.Split(string(buf), "\n") {
		line = strx.TrimSpace(line)
		if line == "" || strx.HasPrefix(line, "#") {
			continue
		}
		urls = append(urls, line)
	}
	return urls, nil
}
