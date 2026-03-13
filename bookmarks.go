package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// Bookmark represents a bookmark entry.
type Bookmark struct {
	Title        string
	Link         string
	AddDate      string
	LastModified string
	// Fields to be appended after content retrieval:
	LastChecked     string
	Active          string
	HeadersContent  string
	Paragraphs      string
	MetaDescription string
}

// HTTPClient is a global variable with retries using custom transport.
var HTTPClient = &http.Client{
	Timeout: 4 * time.Second,
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	},
}

// cleanText removes unwanted characters and extra spaces.
func cleanText(text string) string {
	if text == "" {
		return ""
	}
	// Replace newline, tab, carriage returns with a space.
	replacer := strings.NewReplacer("\n", " ", "\t", " ", "\r", " ", "'", " ")
	cleaned := replacer.Replace(text)
	// Collapse multiple spaces into one.
	return strings.Join(strings.Fields(cleaned), " ")
}

// parseHTMLFile parses an HTML file and extracts bookmark data.
func parseHTMLFile(filePath string) ([]Bookmark, error) {
	data := []Bookmark{}
	// Read the file contents.
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return data, err
	}
	// Parse HTML with goquery.
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(bytes)))
	if err != nil {
		return data, err
	}
	log.Printf("Parsing HTML: %s\n", filePath)
	// Find all  elements.
	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		title := strings.TrimSpace(s.Text())
		link, _ := s.Attr("href")
		addDate, _ := s.Attr("add_date")
		lastModified, _ := s.Attr("last_modified")
		data = append(data, Bookmark{
			Title:        title,
			Link:         link,
			AddDate:      addDate,
			LastModified: lastModified,
		})
	})
	return data, nil
}

// parseJSONFile parses a JSON file recursively to extract bookmark data.
func parseJSONFile(filePath string) ([]Bookmark, error) {
	data := []Bookmark{}
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return data, err
	}
	// Unmarshal into an interface.
	var jsonData interface{}
	if err = json.Unmarshal(bytes, &jsonData); err != nil {
		return data, err
	}

	log.Printf("Parsing JSON: %s\n", filePath)

	// Recursive extraction function.
	var extractBookmarks func(node interface{})
	extractBookmarks = func(node interface{}) {
		switch v := node.(type) {
		case map[string]interface{}:
			if uri, exists := v["uri"]; exists {
				var title, addDate, lastModified string
				if tt, ok := v["title"].(string); ok {
					title = tt
				}
				link := fmt.Sprintf("%v", uri)
				if dt, ok := v["dateAdded"].(string); ok {
					addDate = dt
				}
				if lm, ok := v["lastModified"].(string); ok {
					lastModified = lm
				}
				data = append(data, Bookmark{
					Title:        title,
					Link:         link,
					AddDate:      addDate,
					LastModified: lastModified,
				})
			}
			// Process children if present.
			if children, exists := v["children"]; exists {
				extractBookmarks(children)
			}
		case []interface{}:
			for _, item := range v {
				extractBookmarks(item)
			}
		}
	}
	extractBookmarks(jsonData)
	return data, nil
}

// removeDuplicates filters out bookmarks having duplicate links.
func removeDuplicates(data []Bookmark) []Bookmark {
	seen := make(map[string]bool)
	unique := []Bookmark{}
	for _, bm := range data {
		if bm.Link == "" {
			continue
		}
		if _, found := seen[bm.Link]; !found {
			seen[bm.Link] = true
			unique = append(unique, bm)
		}
	}
	return unique
}

// extractContent fetches content from a bookmark URL and populates the bookmark fields.
func extractContent(bm Bookmark) (Bookmark, bool, bool, bool) {
	today := time.Now().Format("2006-01-02")
	// Check if URL indicates a media resource.
	mediaSuffixes := []string{".mp3", ".m3u", ".pls", ".aac", ".wav", ".ogg"}
	lowerLink := strings.ToLower(bm.Link)
	for _, suffix := range mediaSuffixes {
		if strings.HasSuffix(lowerLink, suffix) {
			// Mark as inactive for media.
			bm.LastChecked = today
			bm.Active = "no"
			return bm, false, true, false
		}
	}

	// Set up custom request.
	req, err := http.NewRequest("GET", bm.Link, nil)
	if err != nil {
		bm.LastChecked = today
		bm.Active = "no"
		return bm, false, true, false
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0")

	// Perform request.
	resp, err := HTTPClient.Do(req)
	if err != nil {
		bm.LastChecked = today
		bm.Active = "no"
		return bm, false, true, false
	}
	defer resp.Body.Close()

	// Consider status codes up to 403 as active.
	if resp.StatusCode > 403 {
		bm.LastChecked = today
		bm.Active = "no"
		return bm, false, true, false
	}

	// Load response body into goquery document.
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		bm.LastChecked = today
		bm.Active = "no"
		return bm, false, true, false
	}

	// Gather header tags h1-h6.
	var hdrTexts []string
	doc.Find("h1, h2, h3, h4, h5, h6").Each(func(i int, s *goquery.Selection) {
		hdrTexts = append(hdrTexts, s.Text())
	})
	hContent := cleanText(strings.Join(hdrTexts, " "))

	// Gather <p> paragraphs.
	var pTexts []string
	doc.Find("p").Each(func(i int, s *goquery.Selection) {
		pTexts = append(pTexts, s.Text())
	})
	pContent := cleanText(strings.Join(pTexts, " "))

	// Get meta description.
	metaDesc, exists := doc.Find("meta[name='description']").Attr("content")
	if !exists {
		metaDesc = ""
	}
	metaContent := cleanText(metaDesc)

	bm.LastChecked = today
	bm.Active = "yes"
	bm.HeadersContent = hContent
	bm.Paragraphs = pContent
	bm.MetaDescription = metaContent

	return bm, true, false, false
}

func main() {
	// os.Setenv("LC_ALL", "en_US.UTF-8")
	
	// Determine paths.
	currentDir, err := os.Getwd()
	if err != nil {
		log.Printf("Error getting working directory: %v\n", err)
		return
	}
	folderPath := filepath.Join(currentDir, "bookmarks")
	csvFilePath := filepath.Join(currentDir, "data1.csv")

	var allData []Bookmark
	htmlCount := 0
	jsonCount := 0

	// Walk the bookmarks folder and process files.
	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		lowerName := strings.ToLower(info.Name())
		if strings.HasSuffix(lowerName, ".html") {
			htmlCount++
			bms, err := parseHTMLFile(path)
			if err != nil {
				log.Printf("Error parsing HTML file %s: %v\n", path, err)
			} else {
				allData = append(allData, bms...)
			}
		} else if strings.HasSuffix(lowerName, ".json") {
			jsonCount++
			bms, err := parseJSONFile(path)
			if err != nil {
				log.Printf("Error parsing JSON file %s: %v\n", path, err)
			} else {
				allData = append(allData, bms...)
			}
		}

		return nil
	})
	if err != nil {
		log.Printf("Error walking bookmarks folder: %v\n", err)
		return
	}

	totalFiles := htmlCount + jsonCount
	log.Printf("Found %d files in %s\n\t- %d HTML files\n\t- %d JSON files.\n", totalFiles, folderPath, htmlCount, jsonCount)
	log.Printf("Found %d bookmark entries. Checking for duplicates...\n", len(allData))

	uniqueData := removeDuplicates(allData)
	duplicates := len(allData) - len(uniqueData)
	log.Printf("Removed %d duplicate entries.\nGathering content data...\n", duplicates)

	// Use WaitGroup to process bookmarks concurrently.
	var wg sync.WaitGroup
	// Use a buffered channel to collect results.
	resultCh := make(chan Bookmark, len(uniqueData))
	// Use channels to count errors.
	var mu sync.Mutex
	activeCount, httpErrorCount, timeoutCount := 0, 0, 0

	// Limit maximum concurrency using a semaphore channel.
	maxWorkers := 20
	sem := make(chan struct{}, maxWorkers)

	startTime := time.Now()
	for _, bm := range uniqueData {
		wg.Add(1)
		sem <- struct{}{}
		// Process each bookmark in a goroutine.
		go func(b Bookmark) {
			defer wg.Done()
			// Create a context with 10s timeout to mimic Python future timeout.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			type result struct {
				bm          Bookmark
				active      bool
				httpErr, tm bool
			}
			resCh := make(chan result, 1)
			go func() {
				updated, active, httpErr, tm := extractContent(b)
				resCh <- result{updated, active, httpErr, tm}
			}()

			var res result
			select {
			case res = <-resCh:
				// nothing to do
			case <-ctx.Done():
				// Timeout case:
				b.LastChecked = time.Now().Format("2006-01-02")
				b.Active = "no"
				res = result{b, false, false, true}
			}

			mu.Lock()
			if res.active {
				activeCount++
			}
			if res.httpErr {
				httpErrorCount++
			}
			if res.tm {
				timeoutCount++
			}
			mu.Unlock()

			resultCh <- res.bm

			// Report progress (optional logging)
			elapsed := time.Since(startTime)
			log.Printf("\rProcessed bookmark: %s | Elapsed: %v", b.Link, elapsed)
			<-sem
		}(bm)
	}

	wg.Wait()
	close(resultCh)

	// Gather processed bookmarks.
	var results []Bookmark
	for bm := range resultCh {
		results = append(results, bm)
	}

	log.Println("\nContent extraction complete.")
	log.Printf("Valid entries: %d\nHTTP Errors: %d\nTimeout errors: %d\n", activeCount, httpErrorCount, timeoutCount)
	log.Println("Writing data to CSV file...")

	// Write CSV output.
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		log.Printf("Error creating CSV file: %v\n", err)
		return
	}
	defer csvFile.Close()

	// Set the encoding to UTF-8
	// csvFile.WriteString("\xEF\xBB\xBF") // BOM (Byte Order Mark) for UTF-8

	writer := csv.NewWriter(csvFile)
	// Optionally, you can set custom quoting options on csv.Writer.
	header := []string{"Title", "Link", "Add Date", "Last Modified", "last_checked", "Active", "h1", "p", "meta_description"}
	if err := writer.Write(header); err != nil {
		log.Printf("Error writing header: %v\n", err)
		return
	}

	for _, bm := range results {
		row := []string{
			bm.Title,
			bm.Link,
			bm.AddDate,
			bm.LastModified,
			bm.LastChecked,
			bm.Active,
			bm.HeadersContent,
			bm.Paragraphs,
			bm.MetaDescription,
		}
		if err := writer.Write(row); err != nil {
			log.Printf("Error writing row: %v\n", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Printf("Error flushing CSV: %v\n", err)
	}
	log.Println("Done.")
}
