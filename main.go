package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
)

// --- Konfigurasi ---
const (
	maxWorkers      = 3
	outputDir       = "output_spa"
	userAgent       = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
	pageLoadTimeout = 45 * time.Second
	launchTimeout   = 2 * time.Minute
)

// Job merepresentasikan tugas crawling
type Job struct {
	URL string
}

// Result merepresentasikan hasil crawling
type Result struct {
	URL     string
	Status  string
	Message string
	Data    map[string]interface{}
}

// BrowserPool mengelola pool browser
type BrowserPool struct {
	jobs    chan Job
	results chan Result
	wg      sync.WaitGroup
	ctx     context.Context
	browser *rod.Browser
	l       *launcher.Launcher
}

// NewBrowserPool menginisialisasi browser
func NewBrowserPool(ctx context.Context, numWorkers int) (*BrowserPool, error) {
	log.Println("🚀 Meluncurkan Headless Browser...")

	// Setup Launcher
	l := launcher.New().
		Headless(true).
		NoSandbox(true).
		Set("disable-gpu", "true").
		Set("disable-dev-shm-usage", "true").
		Set("user-agent", userAgent).
		// Blokir gambar di level engine agar cepat
		Set("blink-settings", "imagesEnabled=false").
		Set("disable-features", "IsolateOrigins,site-per-process")

	// Channel untuk hasil launch
	type launchResult struct {
		url string
		err error
	}
	ch := make(chan launchResult, 1)

	// Launch di goroutine agar bisa di-timeout
	go func() {
		u, e := l.Launch()
		ch <- launchResult{url: u, err: e}
	}()

	var browserURL string
	select {
	case res := <-ch:
		if res.err != nil {
			return nil, fmt.Errorf("launch failed: %w", res.err)
		}
		browserURL = res.url
	case <-time.After(launchTimeout):
		l.Kill()
		return nil, fmt.Errorf("launch timeout after %v", launchTimeout)
	case <-ctx.Done():
		l.Kill()
		return nil, ctx.Err()
	}

	// Connect ke browser
	browser := rod.New().ControlURL(browserURL).MustConnect()

	bp := &BrowserPool{
		jobs:    make(chan Job, 100),
		results: make(chan Result, 100),
		ctx:     ctx,
		browser: browser,
		l:       l,
	}

	// Start Workers
	for i := 0; i < numWorkers; i++ {
		bp.wg.Add(1)
		go bp.worker(i)
	}

	return bp, nil
}

// worker memproses job
func (bp *BrowserPool) worker(id int) {
	defer bp.wg.Done()
	for {
		select {
		case <-bp.ctx.Done():
			return
		case job, ok := <-bp.jobs:
			if !ok {
				return
			}
			res := bp.processPage(job.URL)
			bp.results <- res
		}
	}
}

// processPage melakukan crawling pada satu URL
func (bp *BrowserPool) processPage(rawURL string) Result {
	log.Printf("⏳ Crawling: %s", rawURL)

	// Buat Page Baru
	page := bp.browser.MustPage()

	// Set Timeout
	page = page.Timeout(pageLoadTimeout)

	// Navigate ke URL
	err := page.Navigate(rawURL)
	if err != nil {
		return Result{URL: rawURL, Status: "FAILED", Message: fmt.Sprintf("Navigate error: %v", err)}
	}

	// Tunggu halaman load
	err = page.WaitLoad()
	if err != nil {
		log.Printf("⚠️ WaitLoad timeout for %s, trying fallback...", rawURL)
		time.Sleep(2 * time.Second)
	}

	// Ambil HTML
	html, err := page.HTML()
	if err != nil {
		return Result{URL: rawURL, Status: "FAILED", Message: fmt.Sprintf("HTML fetch error: %v", err)}
	}

	// Tutup page
	page.Close()

	// Validasi Konten
	if len(strings.TrimSpace(html)) < 100 {
		return Result{URL: rawURL, Status: "FAILED", Message: "Content too short"}
	}

	// Ekstraksi Metadata
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	title := ""
	if err == nil {
		title = doc.Find("title").First().Text()
	}

	// Simpan File
	filePath, err := saveToDisk(rawURL, []byte(html))
	if err != nil {
		return Result{URL: rawURL, Status: "FAILED", Message: fmt.Sprintf("Save error: %v", err)}
	}

	return Result{
		URL:     rawURL,
		Status:  "SUCCESS",
		Message: filePath,
		Data: map[string]interface{}{
			"title": strings.TrimSpace(title),
			"size":  len(html),
		},
	}
}

// saveToDisk menyimpan HTML ke file
func saveToDisk(rawURL string, content []byte) (string, error) {
	hash := sha256.Sum256([]byte(rawURL))
	hashStr := hex.EncodeToString(hash[:])[:16]

	host := "unknown"
	parts := strings.Split(rawURL, "/")
	if len(parts) > 2 {
		host = strings.ReplaceAll(parts[2], ".", "_")
	}

	filename := fmt.Sprintf("%s_%s.html", host, hashStr)
	fullPath := filepath.Join(outputDir, filename)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", err
	}

	return fullPath, os.WriteFile(fullPath, content, 0644)
}

// Submit menambahkan job ke antrian
func (bp *BrowserPool) Submit(url string) {
	bp.jobs <- Job{URL: url}
}

// Close menutup pool
func (bp *BrowserPool) Close() {
	close(bp.jobs)
	bp.wg.Wait()
	close(bp.results)

	if bp.browser != nil {
		bp.browser.Close()
	}
	if bp.l != nil {
		bp.l.Kill()
	}
	log.Println("🛑 Browser pool closed.")
}

// Main function
func main() {
	targetURLs := []string{
		"https://sequence.day",
		"https://cmlabs.co",
		"https://react.dev",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := NewBrowserPool(ctx, maxWorkers)
	if err != nil {
		log.Fatalf("Fatal Error: %v", err)
	}

	// Monitor Results
	go func() {
		success, failed := 0, 0
		for res := range pool.results {
			if res.Status == "SUCCESS" {
				success++
				log.Printf("✅ [%s] %s", res.Data["title"], res.Message)
			} else {
				failed++
				log.Printf("❌ [ERR] %s -> %s", res.URL, res.Message)
			}
		}
		log.Printf("🏁 Done. Success: %d, Failed: %d", success, failed)
	}()

	// Submit Jobs
	for _, u := range targetURLs {
		select {
		case <-ctx.Done():
			goto cleanup
		default:
			pool.Submit(u)
			time.Sleep(500 * time.Millisecond)
		}
	}

cleanup:
	pool.Close()
}
