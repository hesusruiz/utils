// Package filecache implements a simple file cache for local and remote files.
// It supports caching of file contents in memory, with configurable freshness
// policies for both disk-based and server-based files. It handles HTTP ETag
// and Expires headers to optimize network usage and ensure data consistency.
package filecache

import (
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hesusruiz/utils/errl"
	"gitlab.com/greyxor/slogor"
)

// FileEntry implements a file cache to improve performance while being able to pick newer
// versions of the configuration and rules in a reasonable timeframe.
type FileEntry struct {
	Name         string
	EntryUpdated time.Time
	FileModTime  time.Time
	Content      []byte
	Etag         string
	Expires      time.Time
	FileHash     uint64
}

const maxFileSize = 1024 * 1024
const freshnessForDiskFiles = 20 * time.Second
const freshnessForServerFiles = 5 * time.Minute

var ErrorRedirectsNotAllowed = errors.New("redirects not allowed")
var seed = maphash.MakeSeed()

type SimpleFileCache struct {
	fileCache  *sync.Map
	httpClient *http.Client
	options    *FileCacheOptions
}

type FileCacheOptions struct {
	// MaxFileSize is the maximum size of the file to be cached. Default is 1MB.
	MaxFileSize int
	// FreshnessForDiskFiles is the freshness time for disk files. Default is 20 seconds.
	FreshnessForDiskFiles time.Duration
	// FreshnessForServerFiles is the freshness time for server files. Default is 5 minutes.
	FreshnessForServerFiles time.Duration
	// HTTPClient is the HTTP client to be used. Default is a new client with a timeout of 10 seconds.
	HTTPClient *http.Client
}

// NewSimpleFileCache creates a new SimpleFileCache with the given options.
// If options is nil, default values are used. The default values are:
// - MaxFileSize: 1MB
// - FreshnessForDiskFiles: 20 seconds
// - FreshnessForServerFiles: 5 minutes
// - HTTPClient: a new client with a timeout of 10 seconds
func NewSimpleFileCache(options *FileCacheOptions) *SimpleFileCache {
	if options == nil {
		options = &FileCacheOptions{}
	}
	if options.MaxFileSize == 0 {
		options.MaxFileSize = maxFileSize
	}
	if options.FreshnessForDiskFiles == 0 {
		options.FreshnessForDiskFiles = freshnessForDiskFiles
	}
	if options.FreshnessForServerFiles == 0 {
		options.FreshnessForServerFiles = freshnessForServerFiles
	}

	var client *http.Client

	if options.HTTPClient == nil {
		client = &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return ErrorRedirectsNotAllowed
			},
		}
	} else {
		client = options.HTTPClient
	}

	return &SimpleFileCache{
		fileCache:  &sync.Map{},
		httpClient: client,
		options:    options,
	}
}

func (m *SimpleFileCache) Get(fileName string) (*FileEntry, error) {

	if fileName == "" {
		return nil, errl.Errorf("file name is empty")
	}

	// We only support https or local files
	if strings.HasPrefix(fileName, "https://") {
		return m.GetURL(fileName)
	} else {
		return m.GetFile(fileName)
	}
}

func (m *SimpleFileCache) GetURL(fileName string) (*FileEntry, error) {

	now := time.Now()

	// Try to get the file from the cache
	fe, found := m.fileCache.Load(fileName)
	if found {

		// Return the entry if it is fresh enough, or read it again if it is not.

		entry := fe.(*FileEntry)
		if now.Before(entry.Expires) {

			// We return directly the entry in the cache if it is fresh enough.
			slog.Debug("readFileIfNew", "file", fileName, "msg", "found and cache entry is fresh")
			return entry, nil

		} else {

			// The entry in the cache is old, so we read again the file.
			slog.Debug("readFileIfNew", "file", fileName, "msg", "found but entry is NOT fresh")
			req, err := http.NewRequest("GET", fileName, nil)
			if err != nil {
				// An error creating the request is strange, but we return the entry in the cache
				// and log the error, so the system can continue working, even with stale data.
				slog.Error("readFileIfNew", "file", fileName, "msg", "error creating request", slogor.Err(err))
				return entry, nil
			}

			// Add to the request the If-None-Match header if Etag was present in the previous response from the server
			if entry.Etag != "" {
				req.Header.Add("If-None-Match", entry.Etag)
			}

			resp, err := m.httpClient.Do(req)
			if err != nil {
				// Log the error and return the entry in the cache, so the system can continue working, even with stale data.
				slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slogor.Err(err))
				return entry, nil
			}
			defer resp.Body.Close()

			// If the server returns a 304 Not Modified, we return the existing entry in the cache,
			// but we update the Etag and Expires headers if present in the response from the server.
			// This is useful to refresh the cache without downloading the file again.
			if resp.StatusCode == http.StatusNotModified {
				slog.Debug("readFileIfNew", "file", fileName, "msg", "file not modified")

				// Set the Etag header if present in the response from the server.
				// In principle, given the status 304, the Etag should not change, but we update it just in case.
				if etag := resp.Header.Get("Etag"); etag != "" {
					entry.Etag = etag
				}

				// Refresh the expires header if present in the response from the server
				// Again, given the status 304, the expires should not change, but we update it just in case.
				if expires := resp.Header.Get("Expires"); expires != "" {
					entry.Expires, err = time.Parse(time.RFC1123, expires)
					if err != nil {
						// If we cannot parse the Expires header, we just log the error,
						// so the system can continue working, even with stale data.
						slog.Error("readFileIfNew", "file", fileName, "msg", "error parsing Expires header", slogor.Err(err))
					}
				}

				// Update Last-Modified if present
				if lastMod := resp.Header.Get("Last-Modified"); lastMod != "" {
					if t, err := time.Parse(time.RFC1123, lastMod); err == nil {
						entry.FileModTime = t
					}
				}

				// Update the entry in the cache with the new Etag and Expires headers
				// We have not updated the content of the file, as it was not modified.
				// For the caller, the content is the same as before so we return found = true
				m.fileCache.Store(fileName, entry)
				return entry, nil
			}

			// Other status codes apart from 200 are errors, so we return the existing entry in the cache, logging the error
			// and the status code.
			if resp.StatusCode != http.StatusOK {
				slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slog.Int("status", resp.StatusCode))
				return entry, nil
			}

			content, err := io.ReadAll(resp.Body)
			if err != nil {
				// If we cannot read the file, we return the existing entry in the cache, logging the error
				// so the system can continue working, even with stale data.
				slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slogor.Err(err))
				return entry, nil
			}

			// Check the size of the file
			if len(content) > maxFileSize {
				// If the file is too big, we return the entry in the cache, logging the error
				// so the system can continue working, even with stale data.
				slog.Error("readFileIfNew", "file", fileName, "msg", "file is too big", slog.Int("size", len(content)))
				// We do not return the content of the file, as it is too big.
				return entry, nil
			}

			// Store the new entry with the content of the file
			entry := &FileEntry{
				Name:         fileName,
				EntryUpdated: now,
				Content:      content,
				FileHash:     maphash.Bytes(seed, content),
			}

			// Add the Etag header if present in the response from the server
			if etag := resp.Header.Get("Etag"); etag != "" {
				entry.Etag = etag
			}

			// Set the FileModTime if Last-Modified header is present
			if lastMod := resp.Header.Get("Last-Modified"); lastMod != "" {
				if t, err := time.Parse(time.RFC1123, lastMod); err == nil {
					entry.FileModTime = t
				}
			}

			// Set the expires header if present in the response from the server
			if expires := resp.Header.Get("Expires"); expires != "" {
				entry.Expires, err = time.Parse(time.RFC1123, expires)
				if err != nil {
					// If we cannot parse the Expires header, set the default one
					entry.Expires = now.Add(freshnessForServerFiles)
				}
			} else {
				// If the Expires header is not present, set the default one
				entry.Expires = now.Add(freshnessForServerFiles)
			}

			slog.Debug("readFileIfNew", "file", fileName, "msg", "file refreshed from the server")

			// Store the entry in the cache and return the content, with found = false because the contents changed
			m.fileCache.Store(fileName, entry)
			return entry, nil

		}

	} else {

		// The entry was not found, read the file from the server, set in the cache and return the file.
		// In we found any error, we can not do anything except log it and return an error.
		slog.Debug("readFileIfNew", "file", fileName, "msg", "entry not found in cache")

		// Request the file from the server.
		req, err := http.NewRequest("GET", fileName, nil)
		if err != nil {
			slog.Error("readFileIfNew", "file", fileName, "msg", "error creating request", slogor.Err(err))
			return nil, err
		}

		resp, err := m.httpClient.Do(req)
		if err != nil {
			// If we cannot read the file, we return an error after logging it
			slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slogor.Err(err))
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			// Other status codes are errors, and we can not do anything except log an error
			// and return the error.
			slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slog.Int("status", resp.StatusCode))
			return nil, fmt.Errorf("readFileIfNew: error reading file %s: %w", fileName, err)
		}

		content, err := io.ReadAll(resp.Body)
		if err != nil {
			// If we cannot read the file, we return an error after logging it
			slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slogor.Err(err))
			return nil, err
		}

		// Check the size of the file
		if len(content) > maxFileSize {
			// If the file is too big, we return an error after logging it
			slog.Error("readFileIfNew", "file", fileName, "msg", "file is too big", slog.Int("size", len(content)))
			return nil, fmt.Errorf("readFileIfNew: file %s is too big", fileName)
		}

		// Store the entry with the content of the file
		entry := &FileEntry{
			Name:         fileName,
			EntryUpdated: now,
			Content:      content,
			FileHash:     maphash.Bytes(seed, content),
		}

		// Add the Etag header if present in the response from the server
		if etag := resp.Header.Get("Etag"); etag != "" {
			entry.Etag = etag
		}

		// Set the FileModTime if Last-Modified header is present
		if lastMod := resp.Header.Get("Last-Modified"); lastMod != "" {
			if t, err := time.Parse(time.RFC1123, lastMod); err == nil {
				entry.FileModTime = t
			}
		}

		// Set the expires header if present in the response from the server
		if expires := resp.Header.Get("Expires"); expires != "" {
			entry.Expires, err = time.Parse(time.RFC1123, expires)
			if err != nil {
				// If we cannot parse the Expires header, set the default one
				entry.Expires = now.Add(freshnessForServerFiles)
			}
		} else {
			// If the Expires header is not present, set the default one
			entry.Expires = now.Add(freshnessForServerFiles)
		}

		slog.Debug("readFileIfNew", "file", fileName, "msg", "file read from the server")
		// Store the entry in the cache and return the content
		m.fileCache.Store(fileName, entry)
		return entry, nil

	}

}

func (m *SimpleFileCache) GetFile(fileName string) (*FileEntry, error) {

	now := time.Now()

	// Try to get the file from the cache
	fe, found := m.fileCache.Load(fileName)
	if found {
		entry := fe.(*FileEntry)

		// Return the entry if it is fresh enough.
		if now.Before(entry.Expires) {
			slog.Debug("readFileIfNew", "file", fileName, "msg", "found and cache entry is fresh")
			return entry, nil
		}
		slog.Debug("readFileIfNew", "file", fileName, "msg", "found but entry is NOT fresh")
	}

	// We are here because either the entry was not found or is not fresh.
	// We get the file info, to check if it was modified.
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return nil, errl.Errorf("readFileIfNew: error checking file %s: %w", fileName, err)
	} else if fileInfo.Mode().IsDir() {
		// We cannot read a directory
		return nil, errl.Errorf("readFileIfNew: file %s is a directory, not a file", fileName)
	}

	// Check if the size is "reasonable" to be loaded in the cache. Default is 1MB, enogh for many policies.
	if fileInfo.Size() > maxFileSize {
		return nil, errl.Errorf("readFileIfNew: file %s is too big", fileName)
	}

	modifiedAt := fileInfo.ModTime()

	// If not found, read the file, set in the cache and return the file.
	if !found {
		slog.Info("readFileIfNew", "file", fileName, "msg", "entry not found in cache, reading")
		content, err := os.ReadFile(fileName)
		if err != nil {
			return nil, errl.Errorf("readFileIfNew: error reading file %s: %w", fileName, err)
		}

		// Add or replace the content of the file cache
		entry := &FileEntry{
			Name:         fileName,
			EntryUpdated: now,
			FileModTime:  modifiedAt,
			Content:      content,
			FileHash:     maphash.Bytes(seed, content),
			Expires:      now.Add(m.options.FreshnessForDiskFiles),
		}

		m.fileCache.Store(fileName, entry)
		slog.Debug("readFileIfNew", "file", fileName, "msg", "file read from disk")

		return entry, nil

	}

	// The entry was found in the cache, but it may be stale.
	entry := fe.(*FileEntry)

	if entry.FileModTime.Before(modifiedAt) {

		// The entry in the cache is old, so we read again the file.
		content, err := os.ReadFile(fileName)
		if err != nil {
			return nil, errl.Errorf("readFileIfNew: error reading file %s: %w", fileName, err)
		}

		// Add to the cache. There is only one instance of each file in the cache.
		entry := &FileEntry{
			Name:         fileName,
			EntryUpdated: now,
			FileModTime:  modifiedAt,
			Content:      content,
			FileHash:     maphash.Bytes(seed, content),
			Expires:      now.Add(m.options.FreshnessForDiskFiles),
		}

		slog.Debug("readFileIfNew", "file", fileName, "msg", "file modification is later than in entry")
		m.fileCache.Store(fileName, entry)
		return entry, nil

	} else {

		// The entry in the cache is still valid, update the timestamp and return the file.
		// Updating the timestamp extends the TTL of the entry.
		slog.Debug("readFileIfNew", "file", fileName, "msg", "entry was not fresh but still valid")
		entry.EntryUpdated = now
		entry.Expires = now.Add(m.options.FreshnessForDiskFiles)

		// And return contents
		return entry, nil
	}

}

// Set stores the contents in the cache with the given name and TTL, maybe overriding any existing entry.
// The TTL is the time to live of the entry in the cache. If it is 0, the entry will never expire (actually in 100 years).
// The content is the contents of the file to be cached. The name is the key of the entry in the cache.
func (m *SimpleFileCache) Set(fileName string, content []byte, ttl time.Duration) error {
	if fileName == "" {
		return errl.Errorf("file name is empty")
	}

	// If the user did not specify a TTL, we set it to 100 years (no expiration of the cache entry)
	now := time.Now()
	if ttl == 0 {
		ttl = time.Hour * 24 * 365 * 100 // 100 years
	}

	entry := &FileEntry{
		Name:         fileName,
		EntryUpdated: now,
		FileModTime:  now,
		Content:      content,
		FileHash:     maphash.Bytes(seed, content),
		Expires:      time.Now().Add(ttl),
	}

	m.fileCache.Store(fileName, entry)

	return nil

}

// MustExist checks if the file exists in the cache and returns it.
// If the file does not exist, it returns an error.
func (m *SimpleFileCache) MustExist(fileName string) (*FileEntry, error) {

	// Try to get the file from the cache
	fe, found := m.fileCache.Load(fileName)
	if found {
		entry := fe.(*FileEntry)
		return entry, nil
	} else {
		return nil, errl.Errorf("file %s not found in cache", fileName)
	}

}
