package main

// zipextract.go — fallback zip extraction used only when the tree API
// is unavailable or returns a truncated result (repos with >100k files).
// Normal downloads use downloadRawFiles in github.go instead.

import (
	"archive/zip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

func extractZipImpl(zipPath, repo, branch, subdir, destDir string,
	workers int, db *HashDB, prog progressFn,
	capRemaining *int64) DownloadStats {

	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return DownloadStats{Errors: 1}
	}
	defer zr.Close()

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return DownloadStats{Errors: 1}
	}

	zipPfx := strings.ToLower(repo + "-" + branch + "/")
	subPfx := ""
	if subdir != "" {
		subPfx = zipPfx + strings.ToLower(subdir) + "/"
	}

	var imgs []*zip.File
	for _, f := range zr.File {
		if f.FileInfo().IsDir() || !isImage(f.Name) {
			continue
		}
		lower := strings.ToLower(f.Name)
		if subPfx != "" && !strings.HasPrefix(lower, subPfx) {
			continue
		}
		imgs = append(imgs, f)
	}

	var stats DownloadStats
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for _, zf := range imgs {
		if capRemaining != nil && atomic.LoadInt64(capRemaining) <= 0 {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(f *zip.File) {
			defer wg.Done()
			defer func() { <-sem }()

			if capRemaining != nil && atomic.LoadInt64(capRemaining) <= 0 {
				return
			}

			rc, err := f.Open()
			if err != nil {
				atomic.AddInt64(&stats.Errors, 1)
				if prog != nil {
					prog(0, 0, 1)
				}
				return
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				atomic.AddInt64(&stats.Errors, 1)
				if prog != nil {
					prog(0, 0, 1)
				}
				return
			}

			digest := md5hex(data)
			if db.has(digest) {
				atomic.AddInt64(&stats.Dupes, 1)
				if prog != nil {
					prog(0, 1, 0)
				}
				return
			}

			fname := filepath.Base(f.Name)
			outPath := flatSavePath(destDir, fname, digest)

			// Claim a slot under the target cap BEFORE saving so concurrent
			// workers cannot overshoot the requested count.
			if capRemaining != nil && atomic.AddInt64(capRemaining, -1) < 0 {
				atomic.AddInt64(capRemaining, 1)
				return
			}

			if err := os.WriteFile(outPath, data, 0644); err != nil {
				atomic.AddInt64(&stats.Errors, 1)
				if capRemaining != nil {
					atomic.AddInt64(capRemaining, 1)
				}
				if prog != nil {
					prog(0, 0, 1)
				}
				return
			}

			db.add(digest, outPath)
			atomic.AddInt64(&stats.New, 1)
			if prog != nil {
				prog(1, 0, 0)
			}
		}(zf)
	}
	wg.Wait()
	return stats
}
