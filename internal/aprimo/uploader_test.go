package aprimo

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/timw255/uplink/internal/connector"
)

// uploadStub captures multipart segment bodies so the test can assert
// the protocol was followed correctly.
type uploadStub struct {
	mu           sync.Mutex
	segmentBytes map[int][]byte
	setupHit     int
	commitHit    int
	deleteHit    int
	uri          string
}

func (s *uploadStub) handler(t *testing.T) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) {
		// single-shot small-file upload
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, "parse", http.StatusBadRequest)
			return
		}
		if _, ok := r.MultipartForm.File["file1"]; !ok {
			http.Error(w, "no file1 part", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"token":"small-tok"}`))
	})
	mux.HandleFunc("/uploads/segments", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.setupHit++
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"uri":"` + s.uri + `"}`))
	})
	// /upload/<id>{?index=N}  and  /upload/<id>/commit
	mux.HandleFunc("/upload/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete:
			s.mu.Lock()
			s.deleteHit++
			s.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		case strings.HasSuffix(r.URL.Path, "/commit"):
			body, _ := io.ReadAll(r.Body)
			var c struct {
				Filename     string `json:"filename"`
				SegmentCount int    `json:"segmentcount"`
			}
			_ = json.Unmarshal(body, &c)
			if c.SegmentCount == 0 {
				http.Error(w, "no segmentcount", http.StatusBadRequest)
				return
			}
			s.mu.Lock()
			s.commitHit++
			s.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"token":"final-tok"}`))
			return
		default:
			index := r.URL.Query().Get("index")
			if index == "" {
				http.Error(w, "missing index", http.StatusBadRequest)
				return
			}
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				http.Error(w, "parse", http.StatusBadRequest)
				return
			}
			fhs := r.MultipartForm.File["segment"+index]
			if len(fhs) == 0 {
				http.Error(w, "no segment"+index+" part", http.StatusBadRequest)
				return
			}
			f, err := fhs[0].Open()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(f)
			_ = f.Close()
			s.mu.Lock()
			if s.segmentBytes == nil {
				s.segmentBytes = make(map[int][]byte)
			}
			// parse index back into an int
			idxInt := 0
			for i, c := range index {
				if i == 0 && c == '-' {
					continue
				}
				idxInt = idxInt*10 + int(c-'0')
			}
			s.segmentBytes[idxInt] = body
			s.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}
	})
	return mux
}

func TestCreateDirectUpload(t *testing.T) {
	var gotFileName string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/uploads" || r.Method != http.MethodPost {
			http.Error(w, "unexpected", http.StatusNotFound)
			return
		}
		var body struct {
			FileName string `json:"fileName"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotFileName = body.FileName
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"token":"tok-123","sasUrl":"https://acct.blob.core.windows.net/c/abc/bigfile.psd?sig=x"}`))
	}))
	defer srv.Close()

	c, err := New(Config{Environment: "env", TokenProvider: stubAuth("tok"), HTTPClient: srv.Client()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.mo.baseURL = srv.URL

	du, err := c.Uploader.CreateDirectUpload(context.Background(), "bigfile.psd")
	if err != nil {
		t.Fatalf("CreateDirectUpload: %v", err)
	}
	if gotFileName != "bigfile.psd" {
		t.Errorf("fileName sent = %q, want bigfile.psd", gotFileName)
	}
	if du.Token != "tok-123" || du.SASURL == "" {
		t.Fatalf("DirectUpload = %+v", du)
	}
}

func TestCreateDirectUploadMissingSAS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"token":"t"}`)) // no sasUrl — should error
	}))
	defer srv.Close()

	c, err := New(Config{Environment: "env", TokenProvider: stubAuth("tok"), HTTPClient: srv.Client()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.mo.baseURL = srv.URL

	if _, err := c.Uploader.CreateDirectUpload(context.Background(), "f.bin"); err == nil {
		t.Fatal("expected an error when sasUrl is missing")
	}
}

func TestUploaderSingleShot(t *testing.T) {
	s := &uploadStub{}
	srv := httptest.NewServer(s.handler(t))
	defer srv.Close()
	s.uri = srv.URL + "/upload/abc"

	c, err := New(Config{
		Environment:   "env",
		TokenProvider: stubAuth("tok"),
		HTTPClient:    srv.Client(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Override the MO URL to point at our test server.
	c.mo.baseURL = srv.URL

	src := &connector.ReaderSource{Data: []byte("hello")}
	res, err := c.Uploader.UploadFromSource(context.Background(), src, "hello.txt", nil)
	if err != nil {
		t.Fatalf("UploadFromSource: %v", err)
	}
	if res.Token != "small-tok" {
		t.Fatalf("token = %q", res.Token)
	}
}

func TestUploaderSegmented(t *testing.T) {
	s := &uploadStub{}
	srv := httptest.NewServer(s.handler(t))
	defer srv.Close()
	s.uri = srv.URL + "/upload/abc"

	c, err := New(Config{
		Environment:   "env",
		TokenProvider: stubAuth("tok"),
		HTTPClient:    srv.Client(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.mo.baseURL = srv.URL

	// 1KiB segments * 3.5 segments = 4 segments total (last one partial)
	src := bytes.Repeat([]byte("A"), 3500)
	var progressMu sync.Mutex
	var progress []int
	var totals []int
	opts := &UploadOptions{
		SegmentSize:      1024,
		ParallelSegments: 2,
		OnProgress: func(done, total int) {
			progressMu.Lock()
			progress = append(progress, done)
			totals = append(totals, total)
			progressMu.Unlock()
		},
	}
	res, err := c.Uploader.UploadFromSource(context.Background(),
		&connector.ReaderSource{Data: src}, "big.bin", opts)
	if err != nil {
		t.Fatalf("UploadFromSource: %v", err)
	}
	if res.Token != "final-tok" {
		t.Fatalf("token = %q", res.Token)
	}

	if s.setupHit != 1 {
		t.Errorf("setupHit = %d", s.setupHit)
	}
	if s.commitHit != 1 {
		t.Errorf("commitHit = %d", s.commitHit)
	}
	if len(s.segmentBytes) != 4 {
		t.Errorf("got %d segments, want 4", len(s.segmentBytes))
	}

	// All bytes round-tripped.
	var rebuilt bytes.Buffer
	for i := range 4 {
		rebuilt.Write(s.segmentBytes[i])
	}
	if !bytes.Equal(rebuilt.Bytes(), src) {
		t.Errorf("reassembled bytes do not match source (got %d bytes, want %d)",
			rebuilt.Len(), len(src))
	}

	progressMu.Lock()
	got := len(progress)
	gotTotals := append([]int(nil), totals...)
	progressMu.Unlock()
	if got != 4 {
		t.Errorf("progress called %d times, want 4", got)
	}
	for i, total := range gotTotals {
		if total != 4 {
			t.Errorf("progress call %d reported total=%d, want 4", i, total)
		}
	}
}
