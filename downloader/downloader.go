package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"sync"
	"time"

	"github.com/hedzr/progressbar"
	"github.com/renatopp/x/fmtx"
	"github.com/renatopp/x/fsx"
	"github.com/renatopp/x/strx"
)

type Config struct {
	Urls       []string
	Output     string
	Dir        string
	Method     string
	Quiet      bool
	Checksum   string
	Parallel   int
	FailFast   bool
	Retries    int
	Headers    http.Header
	NoAtomic   bool
	NoResume   bool
	NoProgress bool
	Pipe       bool
	Statusf    func(format string, args ...any)
}

type job struct {
	index int
	url   string
	path  string
}

func Download(ctx context.Context, cfg Config) error {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return err
	}

	jobs, err := buildJobs(cfg, cfg.Urls)
	if err != nil {
		return err
	}

	return runDownloads(ctx, cfg, jobs)
}

func normalizeConfig(cfg Config) (Config, error) {
	cfg.Output = strx.TrimSpace(cfg.Output)
	cfg.Dir = strx.TrimSpace(cfg.Dir)
	cfg.Method = strx.ToUpper(strx.TrimSpace(cfg.Method))
	cfg.Checksum = strx.ToLower(strx.TrimSpace(cfg.Checksum))
	cfg.Headers = cloneHeaders(cfg.Headers)

	if cfg.Dir == "" {
		cfg.Dir = "."
	}
	if cfg.Method == "" {
		cfg.Method = http.MethodGet
	}
	if cfg.Checksum == "" {
		cfg.Checksum = "sha256"
	}
	if cfg.Parallel == 0 {
		cfg.Parallel = 4
	}

	if cfg.Parallel < 1 {
		return cfg, fmtx.Error("--parallel must be >= 1")
	}
	if cfg.Retries < 0 {
		return cfg, fmtx.Error("--retries must be >= 0")
	}
	if !contains([]string{"none", "sha256", "md5"}, cfg.Checksum) {
		return cfg, fmtx.Error("unsupported checksum algorithm %q (use sha256, md5 or none)", cfg.Checksum)
	}

	return cfg, nil
}

func normalizeURL(raw string) string {
	if strx.Contains(raw, "://") {
		return raw
	}
	host := raw
	if idx := strx.IndexOf(host, "/"); idx >= 0 {
		host = host[:idx]
	}
	if idx := strx.IndexOf(host, ":"); idx >= 0 {
		host = host[:idx]
	}
	if host == "localhost" || host == "127.0.0.1" {
		return "http://" + raw
	}
	return "https://" + raw
}

func buildJobs(cfg Config, urls []string) ([]job, error) {
	if cfg.Output != "" && len(urls) > 1 {
		return nil, fmtx.Error("--output supports only a single URL")
	}
	if cfg.Pipe && len(urls) > 1 {
		return nil, fmtx.Error("-o - supports only a single URL")
	}
	if cfg.Pipe {
		u := normalizeURL(urls[0])
		if _, err := neturl.ParseRequestURI(u); err != nil {
			return nil, fmtx.Error("invalid URL %q: %w", u, err)
		}
		return []job{{index: 0, url: u, path: ""}}, nil
	}

	jobs := make([]job, 0, len(urls))
	pathSet := map[string]struct{}{}

	for i, u := range urls {
		u = normalizeURL(u)
		if _, err := neturl.ParseRequestURI(u); err != nil {
			return nil, fmtx.Error("invalid URL %q: %w", u, err)
		}

		var outPath string
		if cfg.Output != "" {
			outPath = cfg.Output
		} else {
			outPath = filenameFromURL(u)
		}

		if !fsx.IsAbsolutePath(outPath) {
			outPath = fsx.JoinPath(cfg.Dir, outPath)
		}

		outPath = fsx.CleanPath(outPath)
		if _, exists := pathSet[outPath]; exists {
			return nil, fmtx.Error("multiple URLs resolve to the same output path %q", outPath)
		}
		pathSet[outPath] = struct{}{}

		jobs = append(jobs, job{
			index: i,
			url:   u,
			path:  outPath,
		})
	}

	return jobs, nil
}

func runDownloads(parentCtx context.Context, cfg Config, jobs []job) error {
	client := &http.Client{Timeout: 0}

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	errByIdx := make([]error, len(jobs))

	if !cfg.Quiet && !cfg.NoProgress {
		mpb := progressbar.New()

		sem := make(chan struct{}, max(cfg.Parallel, 1))
		var wg sync.WaitGroup

		for i, j := range jobs {
			i, j := i, j
			wg.Add(1)
			mpb.Add(1, fsx.GetPathBase(j.path), progressbar.WithBarWorker(
				func(bar progressbar.PB, _ <-chan struct{}) bool {
					defer wg.Done()

					sem <- struct{}{}
					defer func() { <-sem }()

					if err := downloadWithRetries(ctx, client, cfg, j, nil, bar); err != nil {
						errByIdx[i] = err
						if cfg.FailFast {
							cancel()
						}
					}

					// Force bar to 100% (handles unknown Content-Length or errors)
					_, ub, prog := bar.Bounds()
					if ub > prog {
						bar.Step(ub - prog)
					}

					return false
				},
			))
		}

		wg.Wait()
		mpb.Close()
	} else {
		jobCh := make(chan job)
		var wg sync.WaitGroup
		var outMu sync.Mutex

		workerCount := cfg.Parallel
		if len(jobs) < workerCount {
			workerCount = len(jobs)
		}
		if workerCount < 1 {
			workerCount = 1
		}

		for range workerCount {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobCh {
					if err := downloadWithRetries(ctx, client, cfg, j, &outMu, nil); err != nil {
						errByIdx[j.index] = err
						if cfg.FailFast {
							cancel()
						}
					}
				}
			}()
		}

	enqueue:
		for _, j := range jobs {
			select {
			case <-ctx.Done():
				break enqueue
			case jobCh <- j:
			}
		}
		close(jobCh)
		wg.Wait()
	}

	failures := make([]string, 0, len(jobs))
	for _, err := range errByIdx {
		if err != nil {
			failures = append(failures, err.Error())
		}
	}

	if len(failures) == 0 {
		return nil
	}

	if len(failures) == 1 {
		return fmtx.Error("%s", failures[0])
	}

	return fmtx.Error("%d downloads failed:\n- %s", len(failures), strx.Join(failures, "\n- "))
}

func filenameFromURL(rawURL string) string {
	parsed, err := neturl.Parse(rawURL)
	if err != nil {
		return "download"
	}
	name := fsx.GetPathBase(parsed.Path)
	if name == "" || name == "/" || name == "." {
		return "download"
	}
	return name
}

func downloadWithRetries(ctx context.Context, client *http.Client, cfg Config, j job, outMu *sync.Mutex, bar progressbar.PB) error {
	var lastErr error
	attempts := cfg.Retries + 1

	for attempt := 1; attempt <= attempts; attempt++ {
		if ctx.Err() != nil {
			return fmtx.Error("%s: canceled", j.url)
		}

		if bar != nil && attempt > 1 {
			bar.SetInitialValue(0)
			bar.UpdateRange(0, 1)
		}

		err := downloadOnce(ctx, client, cfg, j, outMu, bar)
		if err == nil {
			return nil
		}

		lastErr = err
		if attempt == attempts {
			break
		}

		backoff := time.Duration(attempt) * time.Second
		select {
		case <-ctx.Done():
			return fmtx.Error("%s: canceled", j.url)
		case <-time.After(backoff):
		}
	}

	return fmtx.Error("%s: %w", j.url, lastErr)
}

func downloadOnce(ctx context.Context, client *http.Client, cfg Config, j job, outMu *sync.Mutex, bar progressbar.PB) error {
	if cfg.Pipe {
		req, err := http.NewRequestWithContext(ctx, cfg.Method, j.url, nil)
		if err != nil {
			return fmtx.Error("failed creating request: %w", err)
		}
		for key, values := range cfg.Headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		res, err := client.Do(req)
		if err != nil {
			return fmtx.Error("request failed: %w", err)
		}
		defer res.Body.Close()
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return fmtx.Error("unexpected status %d", res.StatusCode)
		}
		_, err = io.Copy(os.Stdout, res.Body)
		return err
	}

	if err := fsx.CreateDir(fsx.GetPathParent(j.path)); err != nil {
		return fmtx.Error("failed creating output directory for %q: %w", j.path, err)
	}

	tempPath := j.path
	if !cfg.NoAtomic {
		tempPath = j.path + ".part"
	}

	resumeFrom := int64(0)
	useRange := !cfg.NoResume && cfg.Method == http.MethodGet
	if useRange {
		if size, err := fsx.Size(tempPath); err == nil {
			resumeFrom = size
		} else if !errors.Is(err, fsx.ErrNotExist) {
			return fmtx.Error("failed checking existing file %q: %w", tempPath, err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, cfg.Method, j.url, nil)
	if err != nil {
		return fmtx.Error("failed creating request: %w", err)
	}

	for key, values := range cfg.Headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	if resumeFrom > 0 {
		req.Header.Set("Range", fmtx.Sprint("bytes=%d-", resumeFrom))
	}

	if bar == nil && !cfg.Quiet && !cfg.NoProgress {
		if resumeFrom > 0 {
			printStatusf(cfg.Statusf, outMu, "Resuming %s -> %s (%d bytes)", j.url, j.path, resumeFrom)
		} else {
			printStatusf(cfg.Statusf, outMu, "Downloading %s -> %s", j.url, j.path)
		}
	}

	res, err := client.Do(req)
	if err != nil {
		return fmtx.Error("request failed: %w", err)
	}
	defer res.Body.Close()

	if resumeFrom > 0 && res.StatusCode == http.StatusOK {
		resumeFrom = 0
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmtx.Error("unexpected status %d", res.StatusCode)
	}

	var outFile *os.File
	if resumeFrom > 0 && res.StatusCode == http.StatusPartialContent {
		outFile, err = os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	} else {
		outFile, err = os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	}
	if err != nil {
		return fmtx.Error("failed opening output file %q: %w", tempPath, err)
	}

	if bar != nil {
		total := res.ContentLength
		if resumeFrom > 0 && res.StatusCode == http.StatusPartialContent {
			total += resumeFrom
			bar.SetInitialValue(resumeFrom)
		}
		if total > 0 {
			bar.UpdateRange(0, total)
		}
	}

	dst := io.Writer(outFile)
	if bar != nil && res.ContentLength > 0 {
		dst = io.MultiWriter(outFile, bar)
	}

	written, copyErr := io.Copy(dst, res.Body)
	closeErr := outFile.Close()
	if copyErr != nil {
		return fmtx.Error("write failed: %w", copyErr)
	}
	if closeErr != nil {
		return fmtx.Error("failed closing output file %q: %w", tempPath, closeErr)
	}

	if !cfg.Quiet && cfg.Checksum != "none" {
		if err := verifyChecksum(tempPath, cfg.Checksum, res.Header); err != nil {
			return err
		}
	}

	if !cfg.NoAtomic {
		if err := fsx.Rename(tempPath, j.path); err != nil {
			return fmtx.Error("failed finalizing %q: %w", j.path, err)
		}
	}

	if bar == nil && !cfg.Quiet && !cfg.NoProgress {
		printStatusf(cfg.Statusf, outMu, "Done %s (%d bytes)", j.path, written)
	}

	return nil
}

func verifyChecksum(path, checksumAlgorithm string, headers http.Header) error {
	if checksumAlgorithm != "sha256" {
		return nil
	}

	expected := readExpectedSHA256(headers)
	if expected == "" {
		return nil
	}

	data, err := fsx.ReadFile(path)
	if err != nil {
		return fmtx.Error("failed opening file for checksum %q: %w", path, err)
	}

	hash := sha256.New()
	hash.Write(data)
	actual := strx.ToLower(hex.EncodeToString(hash.Sum(nil)))

	if actual != expected {
		return fmtx.Error("checksum mismatch for %q: expected %s, got %s", path, expected, actual)
	}

	return nil
}

func readExpectedSHA256(headers http.Header) string {
	if v := strx.TrimSpace(headers.Get("X-Checksum-Sha256")); v != "" {
		return strx.ToLower(v)
	}

	digest := headers.Get("Digest")
	if digest == "" {
		return ""
	}

	parts := strx.Split(digest, ",")
	for _, part := range parts {
		part = strx.TrimSpace(part)
		if !strx.HasPrefix(strx.ToLower(part), "sha-256=") {
			continue
		}

		raw := strx.TrimPrefix(part, "sha-256=")
		raw = strx.Trim(raw, "\"")
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return ""
		}
		return strx.ToLower(hex.EncodeToString(decoded))
	}

	return ""
}

func printStatusf(statusf func(format string, args ...any), outMu *sync.Mutex, format string, args ...any) {
	if statusf == nil {
		return
	}

	outMu.Lock()
	defer outMu.Unlock()
	statusf(format, args...)
}

func cloneHeaders(headers http.Header) http.Header {
	if headers == nil {
		return make(http.Header)
	}
	cloned := make(http.Header, len(headers))
	for key, values := range headers {
		copied := make([]string, len(values))
		copy(copied, values)
		cloned[key] = copied
	}
	return cloned
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
