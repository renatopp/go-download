# go-download

Fetch files effortless — whether you're downloading one file or many.

**go-download** is a fast, reliable, and developer-friendly CLI tool and library for downloading files from the web. With built-in support for:

- Resumable downloads
- Concurrent transfers
- Automatic retries
- Checksum verification
- Atomic writes
- Custom headers and different http methods

## Installation

To install the `download` CLI, just run:

```bash
go install github.com/renatopp/go-download/cmd/download@latest
```

To install as a library, run:

```bash
go get github.com/renatopp/go-download
```

Then, import the package `github.com/renatopp/go-download` in your Go code.

## CLI Usage

```bash
Usage: download [options] [<urls>]

Options:
  -o, --output      (default=) Output file path (single URL only).
  -d, --dir         (default=.) Output directory.
  -m, --method      (default=GET) HTTP method to use.
  -q, --quiet       (default=false) Quiet mode (disable progress and verifications).
  -c, --checksum    (default=sha256) Checksum algorithm (sha256|md5|none).
  -p, --parallel    (default=4) Number of parallel downloads.
  -f, --fail-fast   (default=false) Stop on first error.
  -r, --retries     (default=2) Number of retries per URL.
  -H, --headers     Custom header. Repeatable: -H "Key: Value"
  --no-atomic       (default=false) Disable atomic write (write directly to output).
  --no-resume       (default=false) Disable resumable downloads.
  --no-progress     (default=false) Disable progress output.
  -h, --help        Show help message

Arguments:
  urls              URLs to download.
```

Examples:

- Download a single file:

  `download https://example.com/file.txt`

- Download multiple files:

  `download https://example.com/file1.txt https://example.com/file1.txt`

- Download files from file:

  `cat urls.txt | download`

- Downloading file piping to stdout

  `download -o - https://example.com/file.txt | cat`

## Library Usage

To use `go-download` as a library, import `github.com/renatopp/go-download/downloader` in your Go code.

```go
import (
	"context"
	"github.com/renatopp/go-download/downloader"
)

func main() {
	ctx := context.Background()
	err := downloader.Download(ctx, downloader.Config{
		Urls: []string{"https://example.com/file1.txt", "https://example.com/file2.txt"}
	})
	if err != nil {
		panic(err)
	}
}
