package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ── Image extension filter ────────────────────────────────────────────────────

var imgExts = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true,
	".webp": true, ".gif": true, ".bmp": true,
	".tiff": true, ".tif": true, ".heic": true,
	".heif": true, ".avif": true, ".jxl": true,
	".svg": true, ".ico": true, ".psd": true,
	".raw": true, ".arw": true, ".cr2": true,
	".nef": true, ".orf": true, ".dng": true,
	".exr": true, ".hdr": true, ".rgbe": true,
	".pnm": true, ".ppm": true, ".pgm": true,
	".pbm": true, ".pcx": true, ".tga": true,
	".xbm": true, ".xpm": true, ".wbmp": true,
}

func isImage(name string) bool {
	return imgExts[strings.ToLower(filepath.Ext(name))]
}

// ── Core types ────────────────────────────────────────────────────────────────

type DownloadStats struct {
	New    int64
	Dupes  int64
	Errors int64
}

type RepoSpec struct {
	Slug       string
	Owner      string
	Repo       string
	BranchHint string
	Subdir     string
}

type progressFn func(new, dupe, errInc int)

type ResolvedRepo struct {
	Spec   RepoSpec
	Branch string
	Paths  []string // image paths from tree API — populated during resolve
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

// Rotating User-Agents reduce the chance GitHub fingerprints us.
var userAgents = []string{
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
	"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
}

func randomUA() string { return userAgents[rand.Intn(len(userAgents))] }

func doGET(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", randomUA())
	return SharedClient.Do(req)
}

func doHEAD(url string) (*http.Response, error) {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", randomUA())
	return SharedClient.Do(req)
}

func retryAfterSecs(h http.Header) int {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
		return n
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return int(d.Seconds()) + 1
		}
	}
	return 0
}

// getBytes fetches a URL with retry+backoff. Used for API calls and small files.
func getBytes(url string, maxAttempts int) ([]byte, int, error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			base := time.Duration(1<<uint(attempt)) * time.Second
			jitter := time.Duration(rand.Int63n(int64(base)/2 + 1))
			time.Sleep(base + jitter)
		}
		resp, err := doGET(url)
		if err != nil {
			lastErr = err
			continue
		}
		code := resp.StatusCode
		switch {
		case code == 200:
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				lastErr = err
				continue
			}
			return data, 200, nil
		case code == 429 || code == 403:
			wait := retryAfterSecs(resp.Header)
			resp.Body.Close()
			if wait <= 0 {
				wait = 30 * (attempt + 1)
			}
			if wait > 120 {
				wait = 120
			}
			time.Sleep(time.Duration(wait) * time.Second)
			lastErr = fmt.Errorf("HTTP %d", code)
		case code >= 500:
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d", code)
		default:
			resp.Body.Close()
			return nil, code, fmt.Errorf("HTTP %d", code)
		}
	}
	return nil, 0, fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

// ── Branch resolution ─────────────────────────────────────────────────────────
// All candidate branches are HEAD-checked simultaneously. First hit by priority wins.

func resolveBranch(owner, repo, hint string) string {
	seen := map[string]bool{}
	var candidates []string
	for _, b := range []string{hint, "main", "master"} {
		if b != "" && !seen[b] {
			seen[b] = true
			candidates = append(candidates, b)
		}
	}

	type result struct {
		branch string
		order  int
	}
	ch := make(chan result, len(candidates))
	for i, b := range candidates {
		go func(idx int, branch string) {
			url := fmt.Sprintf("https://github.com/%s/%s/archive/%s.zip", owner, repo, branch)
			resp, err := doHEAD(url)
			if resp != nil {
				resp.Body.Close()
			}
			if err == nil && resp != nil && resp.StatusCode == 200 {
				ch <- result{branch, idx}
			} else {
				ch <- result{"", idx}
			}
		}(i, b)
	}
	out := make([]string, len(candidates))
	for range candidates {
		r := <-ch
		out[r.order] = r.branch
	}
	for _, b := range out {
		if b != "" {
			return b
		}
	}
	return ""
}

// ── GitHub tree API ───────────────────────────────────────────────────────────
//
// One API call per repo returns every path in the repo.
// raw.githubusercontent.com image downloads don't count against the API rate
// limit — only this tree call does (60 req/hr unauthenticated, 19 repos is fine).

type treeResponse struct {
	Tree []struct {
		Path string `json:"path"`
		Type string `json:"type"`
	} `json:"tree"`
	Truncated bool `json:"truncated"`
}

// listRepoImages fetches the repo tree and returns all image paths.
// Returns nil if the tree is truncated (>100k files) or the API fails —
// caller should fall back to zip download in that case.
func listRepoImages(owner, repo, branch, subdir string) []string {
	url := fmt.Sprintf(
		"https://api.github.com/repos/%s/%s/git/trees/%s?recursive=1",
		owner, repo, branch,
	)
	data, code, err := getBytes(url, 3)
	if err != nil || code != 200 {
		return nil
	}

	var tree treeResponse
	if err := json.Unmarshal(data, &tree); err != nil {
		return nil
	}
	if tree.Truncated {
		// Tree too large — fall back to zip
		return nil
	}

	prefix := ""
	if subdir != "" {
		prefix = strings.ToLower(subdir) + "/"
	}

	var paths []string
	for _, entry := range tree.Tree {
		if entry.Type != "blob" {
			continue
		}
		lower := strings.ToLower(entry.Path)
		if !isImage(lower) {
			continue
		}
		if prefix != "" && !strings.HasPrefix(lower, prefix) {
			continue
		}
		paths = append(paths, entry.Path)
	}
	return paths
}

// CountRepoImages uses the tree API to count images (used by scan command).
func CountRepoImages(owner, repo, branch, subdir string) int {
	return len(listRepoImages(owner, repo, branch, subdir))
}

// ── Resolve + count (for scan) ────────────────────────────────────────────────

func ResolveAllBranches(specs []RepoSpec) []ResolvedRepo {
	results := make([]ResolvedRepo, len(specs))
	var wg sync.WaitGroup
	for i, spec := range specs {
		wg.Add(1)
		go func(idx int, s RepoSpec) {
			defer wg.Done()
			results[idx] = ResolvedRepo{
				Spec:   s,
				Branch: resolveBranch(s.Owner, s.Repo, s.BranchHint),
			}
		}(i, spec)
	}
	wg.Wait()
	return results
}

func CountAllRepos(specs []RepoSpec) int {
	resolved := ResolveAllBranches(specs)
	var total int64
	// Cap tree API concurrency at 8 — well within 60 req/hr for 19 repos.
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for _, r := range resolved {
		if r.Branch == "" {
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(rr ResolvedRepo) {
			defer wg.Done()
			defer func() { <-sem }()
			n := CountRepoImages(rr.Spec.Owner, rr.Spec.Repo, rr.Branch, rr.Spec.Subdir)
			atomic.AddInt64(&total, int64(n))
		}(r)
	}
	wg.Wait()
	return int(total)
}

// ── Direct file download (primary fast path) ──────────────────────────────────
//
// Strategy:
//   1. Resolve branch (parallel HEAD race)
//   2. Fetch repo tree via GitHub tree API (1 API call, instant)
//   3. Download every image directly from raw.githubusercontent.com
//      using imgWorkers goroutines — files land on disk immediately,
//      no waiting for a zip archive to finish downloading first.
//
// raw.githubusercontent.com is served by GitHub's CDN (Fastly) with no
// documented rate limit on individual file GETs. This approach is:
//   - ~5-10x faster than zip-then-extract for large repos
//   - Zero temp disk usage (no archive file)
//   - Zero extraction CPU overhead
//   - Progress updates start from the very first file

func ResolveAndDownload(specs []RepoSpec, wdir string,
	imgWorkers, repoConcurrency int,
	db *HashDB, prog progressFn, capRemaining *int64) {

	if repoConcurrency <= 0 {
		repoConcurrency = 16
	}

	// Pipeline: resolution feeds immediately into downloads.
	// A repo's files start downloading the moment its branch resolves —
	// no waiting for all 19 repos to resolve first.
	resolvedCh := make(chan ResolvedRepo, len(specs))

	var resolveWg sync.WaitGroup
	for _, spec := range specs {
		resolveWg.Add(1)
		go func(s RepoSpec) {
			defer resolveWg.Done()
			branch := resolveBranch(s.Owner, s.Repo, s.BranchHint)
			if branch == "" {
				resolvedCh <- ResolvedRepo{Spec: s, Branch: ""}
				return
			}
			// Fetch tree immediately after branch resolves.
			paths := listRepoImages(s.Owner, s.Repo, branch, s.Subdir)
			resolvedCh <- ResolvedRepo{Spec: s, Branch: branch, Paths: paths}
		}(spec)
	}
	go func() {
		resolveWg.Wait()
		close(resolvedCh)
	}()

	// repoConcurrency repos download simultaneously.
	sem := make(chan struct{}, repoConcurrency)
	var dlWg sync.WaitGroup

	for rr := range resolvedCh {
		if rr.Branch == "" {
			continue
		}
		if capRemaining != nil && atomic.LoadInt64(capRemaining) <= 0 {
			break
		}
		sem <- struct{}{}
		dlWg.Add(1)
		go func(r ResolvedRepo) {
			defer dlWg.Done()
			defer func() { <-sem }()
			if r.Paths != nil {
				// Fast path: direct raw file downloads
				downloadRawFiles(r, wdir, imgWorkers, db, prog, capRemaining)
			} else {
				// Fallback: zip download (truncated tree or API failure)
				downloadZip(r.Spec, r.Branch, wdir, imgWorkers, db, prog, capRemaining)
			}
		}(rr)
	}
	dlWg.Wait()
}

// DownloadAllRepos kept for compatibility — delegates to ResolveAndDownload.
func DownloadAllRepos(resolved []ResolvedRepo, wdir string,
	imgWorkers, repoConcurrency int,
	db *HashDB, prog progressFn, capRemaining *int64) {

	if repoConcurrency <= 0 {
		repoConcurrency = 16
	}
	sem := make(chan struct{}, repoConcurrency)
	var wg sync.WaitGroup
	for _, r := range resolved {
		if r.Branch == "" {
			continue
		}
		if capRemaining != nil && atomic.LoadInt64(capRemaining) <= 0 {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(rr ResolvedRepo) {
			defer wg.Done()
			defer func() { <-sem }()
			DownloadRepoBranch(rr.Spec, rr.Branch, wdir, imgWorkers, db, prog, capRemaining)
		}(r)
	}
	wg.Wait()
}

// downloadRawFiles downloads all images in rr.Paths directly from
// raw.githubusercontent.com using imgWorkers concurrent goroutines.
func downloadRawFiles(rr ResolvedRepo, wdir string, workers int,
	db *HashDB, prog progressFn, capRemaining *int64) DownloadStats {

	if err := os.MkdirAll(wdir, 0755); err != nil {
		return DownloadStats{Errors: 1}
	}

	var stats DownloadStats
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for _, path := range rr.Paths {
		if capRemaining != nil && atomic.LoadInt64(capRemaining) <= 0 {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			defer func() { <-sem }()

			if capRemaining != nil && atomic.LoadInt64(capRemaining) <= 0 {
				return
			}

			rawURL := fmt.Sprintf(
				"https://raw.githubusercontent.com/%s/%s/%s/%s",
				rr.Spec.Owner, rr.Spec.Repo, rr.Branch, p,
			)

			// Download with retry — raw CDN is very reliable, 2 attempts enough.
			data, _, err := getBytes(rawURL, 2)
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

			fname := filepath.Base(p)
			outPath := flatSavePath(wdir, fname, digest)

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
		}(path)
	}
	wg.Wait()
	return stats
}

// ── Zip fallback (truncated trees / API unavailable) ─────────────────────────

func DownloadRepo(spec RepoSpec, wdir string, workers int,
	db *HashDB, prog progressFn) DownloadStats {
	branch := resolveBranch(spec.Owner, spec.Repo, spec.BranchHint)
	if branch == "" {
		return DownloadStats{Errors: 1}
	}
	return DownloadRepoBranch(spec, branch, wdir, workers, db, prog, nil)
}

func DownloadRepoBranch(spec RepoSpec, branch, wdir string,
	workers int, db *HashDB, prog progressFn,
	capRemaining *int64) DownloadStats {

	// Try fast direct-download path first.
	paths := listRepoImages(spec.Owner, spec.Repo, branch, spec.Subdir)
	if paths != nil {
		rr := ResolvedRepo{Spec: spec, Branch: branch, Paths: paths}
		return downloadRawFiles(rr, wdir, workers, db, prog, capRemaining)
	}
	// Fall back to zip if tree API failed or tree was truncated.
	return downloadZip(spec, branch, wdir, workers, db, prog, capRemaining)
}

// downloadZip downloads the repo archive to a temp file and extracts images.
// Used only as fallback when the tree API is unavailable or the tree is truncated.
func downloadZip(spec RepoSpec, branch, wdir string,
	workers int, db *HashDB, prog progressFn,
	capRemaining *int64) DownloadStats {

	if err := os.MkdirAll(wdir, 0755); err != nil {
		return DownloadStats{Errors: 1}
	}

	archiveURL := fmt.Sprintf(
		"https://github.com/%s/%s/archive/%s.zip",
		spec.Owner, spec.Repo, branch,
	)

	tmpPath, err := fetchToTempFile(archiveURL, 4)
	if err != nil {
		return cloneFallback(spec, branch, wdir, db, prog, capRemaining)
	}
	defer os.Remove(tmpPath)

	return extractZipFile(tmpPath, spec.Repo, branch, spec.Subdir,
		wdir, workers, db, prog, capRemaining)
}

// fetchToTempFile streams a URL body to a temp file. Caller removes it.
func fetchToTempFile(url string, maxAttempts int) (string, error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			base := time.Duration(1<<uint(attempt)) * time.Second
			jitter := time.Duration(rand.Int63n(int64(base)/2 + 1))
			time.Sleep(base + jitter)
		}
		resp, err := doGET(url)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == 429 || resp.StatusCode == 403 {
			wait := retryAfterSecs(resp.Header)
			resp.Body.Close()
			if wait <= 0 {
				wait = 30 * (attempt + 1)
			}
			if wait > 120 {
				wait = 120
			}
			time.Sleep(time.Duration(wait) * time.Second)
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			return "", fmt.Errorf("HTTP %d", resp.StatusCode)
		}
		f, err := os.CreateTemp("", "wallpimp-*.zip")
		if err != nil {
			resp.Body.Close()
			return "", err
		}
		_, err = io.Copy(f, resp.Body)
		resp.Body.Close()
		f.Close()
		if err != nil {
			os.Remove(f.Name())
			lastErr = err
			continue
		}
		return f.Name(), nil
	}
	return "", fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

// ── Zip extraction (fallback only) ───────────────────────────────────────────

func flatSavePath(dir, base, digest string) string {
	p := filepath.Join(dir, base)
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return p
	}
	existing, err := os.ReadFile(p)
	if err == nil && md5hex(existing) == digest {
		return p
	}
	ext := filepath.Ext(base)
	stem := base[:len(base)-len(ext)]
	return filepath.Join(dir, stem+"_"+digest[:8]+ext)
}

func extractZipFile(zipPath, repo, branch, subdir, destDir string,
	workers int, db *HashDB, prog progressFn,
	capRemaining *int64) DownloadStats {
	// Implemented in zipextract.go
	return extractZipImpl(zipPath, repo, branch, subdir, destDir,
		workers, db, prog, capRemaining)
}

// ── Git clone last-resort fallback ────────────────────────────────────────────

func cloneFallback(spec RepoSpec, branch, destDir string,
	db *HashDB, prog progressFn, capRemaining *int64) DownloadStats {

	var stats DownloadStats
	cloneDir := filepath.Join(os.TempDir(),
		fmt.Sprintf("wallpimp-clone-%s-%d", spec.Slug, os.Getpid()))
	defer os.RemoveAll(cloneDir)

	cloneURL := fmt.Sprintf("https://github.com/%s/%s.git", spec.Owner, spec.Repo)
	cmd := exec.Command("git", "clone", "--depth=1", "--single-branch",
		"--branch", branch, cloneURL, cloneDir)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		stats.Errors++
		return stats
	}

	_ = filepath.WalkDir(cloneDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !isImage(path) {
			return nil
		}
		if capRemaining != nil && atomic.LoadInt64(capRemaining) <= 0 {
			return filepath.SkipAll
		}
		if spec.Subdir != "" {
			rel, _ := filepath.Rel(cloneDir, path)
			if !strings.HasPrefix(strings.ToLower(rel),
				strings.ToLower(spec.Subdir)+string(os.PathSeparator)) {
				return nil
			}
		}
		imgData, err := os.ReadFile(path)
		if err != nil {
			stats.Errors++
			return nil
		}
		digest := md5hex(imgData)
		if db.has(digest) {
			stats.Dupes++
			if prog != nil {
				prog(0, 1, 0)
			}
			return nil
		}
		dest := flatSavePath(destDir, filepath.Base(path), digest)
		if err := os.WriteFile(dest, imgData, 0644); err != nil {
			stats.Errors++
			return nil
		}
		db.add(digest, dest)
		stats.New++
		if capRemaining != nil {
			atomic.AddInt64(capRemaining, -1)
		}
		if prog != nil {
			prog(1, 0, 0)
		}
		return nil
	})
	return stats
}
