// frostrelay — минимальный сервер интернет-радио, совместимый с Icecast
// source-протоколом (BUTT, libshout и т.п.). Принимает один входящий поток,
// раздаёт его слушателям по HTTP и пишет резервную копию на диск.
package main

import (
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var indexHTML []byte

const (
	chunkSize    = 8 * 1024
	burstMax     = 128 * 1024 // отдаётся новому слушателю для быстрого старта
	listenerBuf  = 64         // ёмкость канала слушателя (чанков)
	sourceIdle   = 60 * time.Second
)

type listener struct {
	ch   chan []byte
	gone bool // под hub.mu; канал уже закрыт или слушатель снят с учёта
}

type hub struct {
	mu          sync.Mutex
	live        bool
	contentType string
	song        string
	listeners   map[*listener]struct{}
	burst       []byte
}

func newHub() *hub {
	return &hub{listeners: map[*listener]struct{}{}}
}

// start резервирует хаб под единственный источник.
func (h *hub) start(contentType string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.live {
		return false
	}
	h.live = true
	h.contentType = contentType
	h.burst = nil
	return true
}

func (h *hub) stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.live = false
	h.song = ""
	h.burst = nil
	for l := range h.listeners {
		if !l.gone {
			l.gone = true
			close(l.ch)
		}
		delete(h.listeners, l)
	}
}

func (h *hub) broadcast(p []byte) {
	b := make([]byte, len(p))
	copy(b, p)

	h.mu.Lock()
	defer h.mu.Unlock()

	h.burst = append(h.burst, b...)
	if len(h.burst) > burstMax {
		nb := make([]byte, burstMax)
		copy(nb, h.burst[len(h.burst)-burstMax:])
		h.burst = nb
	}

	for l := range h.listeners {
		select {
		case l.ch <- b:
		default: // слушатель не успевает читать — отключаем
			l.gone = true
			close(l.ch)
			delete(h.listeners, l)
		}
	}
}

func (h *hub) addListener() (l *listener, burst []byte, contentType string, live bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.live {
		return nil, nil, "", false
	}
	l = &listener{ch: make(chan []byte, listenerBuf)}
	h.listeners[l] = struct{}{}
	burst = make([]byte, len(h.burst))
	copy(burst, h.burst)
	return l, burst, h.contentType, true
}

func (h *hub) removeListener(l *listener) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !l.gone {
		l.gone = true
		delete(h.listeners, l)
	}
}

func (h *hub) status() (live bool, listeners int, song, contentType string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.live, len(h.listeners), h.song, h.contentType
}

func (h *hub) setSong(s string) {
	h.mu.Lock()
	h.song = s
	h.mu.Unlock()
}

type server struct {
	hub        *hub
	mount      string
	password   string
	archiveDir string
	name       string
}

func (s *server) checkSourceAuth(r *http.Request) bool {
	_, pass, ok := r.BasicAuth()
	return ok && subtle.ConstantTimeCompare([]byte(pass), []byte(s.password)) == 1
}

func extForContentType(ct string) string {
	switch {
	case strings.Contains(ct, "mpeg"):
		return ".mp3"
	case strings.Contains(ct, "ogg"):
		return ".ogg"
	case strings.Contains(ct, "aac"):
		return ".aac"
	case strings.Contains(ct, "webm"):
		return ".webm"
	case strings.Contains(ct, "flac"):
		return ".flac"
	}
	return ".dat"
}

func (s *server) handleMount(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "SOURCE", http.MethodPut:
		s.handleSource(w, r)
	case http.MethodGet, http.MethodHead:
		s.handleListen(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) handleSource(w http.ResponseWriter, r *http.Request) {
	if !s.checkSourceAuth(r) {
		w.Header().Set("WWW-Authenticate", `Basic realm="frostrelay"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "audio/mpeg"
	}
	if !s.hub.start(contentType) {
		http.Error(w, "source already connected", http.StatusForbidden)
		return
	}
	defer s.hub.stop()

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		log.Printf("source: hijack: %v", err)
		return
	}
	defer conn.Close()

	// libshout (BUTT) шлёт PUT с Expect: 100-continue и ждёт ответа,
	// прежде чем начать отдавать поток; старый SOURCE ждёт сразу 200.
	expectContinue := strings.Contains(strings.ToLower(r.Header.Get("Expect")), "100-continue")
	if expectContinue {
		_, err = rw.WriteString("HTTP/1.1 100 Continue\r\n\r\n")
	} else {
		_, err = rw.WriteString("HTTP/1.0 200 OK\r\n\r\n")
	}
	if err == nil {
		err = rw.Flush()
	}
	if err != nil {
		log.Printf("source: handshake: %v", err)
		return
	}

	// Тело читаем в обход net/http: поток идёт без Content-Length.
	// Transfer-Encoding сервер Go при разборе переносит из заголовков
	// в r.TransferEncoding.
	chunked := strings.EqualFold(r.Header.Get("Transfer-Encoding"), "chunked")
	for _, te := range r.TransferEncoding {
		chunked = chunked || strings.EqualFold(te, "chunked")
	}
	var src io.Reader = rw.Reader
	if chunked {
		src = httputil.NewChunkedReader(rw.Reader)
	} else if r.ContentLength > 0 {
		src = io.LimitReader(rw.Reader, r.ContentLength)
	}

	archive, archivePath := s.openArchive(contentType)
	if archive != nil {
		defer archive.Close()
	}

	log.Printf("source: подключён %s (%s), архив: %s", r.RemoteAddr, contentType, archivePath)
	start := time.Now()
	var total int64
	buf := make([]byte, chunkSize)
	for {
		conn.SetReadDeadline(time.Now().Add(sourceIdle))
		n, rerr := src.Read(buf)
		if n > 0 {
			total += int64(n)
			if archive != nil {
				if _, werr := archive.Write(buf[:n]); werr != nil {
					log.Printf("archive: запись прервана: %v", werr)
					archive.Close()
					archive = nil
				}
			}
			s.hub.broadcast(buf[:n])
		}
		if rerr != nil {
			if rerr != io.EOF {
				log.Printf("source: чтение: %v", rerr)
			}
			break
		}
	}
	if expectContinue {
		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		io.WriteString(conn, "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
	}
	log.Printf("source: отключён %s (%.1f МБ за %s)", r.RemoteAddr,
		float64(total)/1024/1024, time.Since(start).Round(time.Second))
}

func (s *server) openArchive(contentType string) (*os.File, string) {
	name := time.Now().Format("2006-01-02_15-04-05") + extForContentType(contentType)
	path := filepath.Join(s.archiveDir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		log.Printf("archive: не удалось открыть %s: %v (вещание продолжится без записи)", path, err)
		return nil, "—"
	}
	return f, path
}

func (s *server) handleListen(w http.ResponseWriter, r *http.Request) {
	l, burst, contentType, live := s.hub.addListener()
	if !live {
		http.Error(w, "stream offline", http.StatusServiceUnavailable)
		return
	}

	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Cache-Control", "no-cache, no-store")
	h.Set("Pragma", "no-cache")
	h.Set("icy-name", s.name)
	h.Set("Access-Control-Allow-Origin", "*")

	if r.Method == http.MethodHead {
		s.hub.removeListener(l)
		w.WriteHeader(http.StatusOK)
		return
	}
	defer s.hub.removeListener(l)

	flusher, _ := w.(http.Flusher)
	write := func(b []byte) bool {
		if _, err := w.Write(b); err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}

	if len(burst) > 0 && !write(burst) {
		return
	}
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-l.ch:
			if !ok || !write(b) {
				return
			}
		}
	}
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	live, listeners, song, _ := s.hub.status()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(map[string]any{
		"name":      s.name,
		"mount":     s.mount,
		"live":      live,
		"listeners": listeners,
		"song":      song,
	})
}

// handleIcecastStatus отдаёт статистику в формате Icecast 2.4
// (/status-json.xsl) — по ней BUTT показывает число слушателей.
func (s *server) handleIcecastStatus(w http.ResponseWriter, r *http.Request) {
	live, listeners, song, contentType := s.hub.status()
	icestats := map[string]any{
		"server_id": "frostrelay",
		"host":      r.Host,
	}
	if live {
		icestats["source"] = map[string]any{
			"listenurl":   "http://" + r.Host + s.mount,
			"listeners":   listeners,
			"server_name": s.name,
			"server_type": contentType,
			"title":       song,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]any{"icestats": icestats})
}

// handleMetadata принимает обновления "сейчас играет" от BUTT
// (GET /admin/metadata?mode=updinfo&song=...).
func (s *server) handleMetadata(w http.ResponseWriter, r *http.Request) {
	if !s.checkSourceAuth(r) {
		w.Header().Set("WWW-Authenticate", `Basic realm="frostrelay"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.URL.Query().Get("mode") != "updinfo" {
		http.Error(w, "unsupported mode", http.StatusBadRequest)
		return
	}
	s.hub.setSong(r.URL.Query().Get("song"))
	w.Header().Set("Content-Type", "text/xml")
	io.WriteString(w, `<?xml version="1.0"?><iceresponse><message>Metadata update successful</message><return>1</return></iceresponse>`)
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

func main() {
	listen := flag.String("listen", ":8000", "адрес и порт HTTP-сервера")
	mount := flag.String("mount", "/stream", "точка монтирования (как в настройках BUTT)")
	password := flag.String("password", "", "пароль источника (или переменная окружения FROSTRELAY_PASSWORD)")
	archiveDir := flag.String("archive", "archive", "каталог для резервных копий эфиров")
	name := flag.String("name", "frostrelay", "название станции")
	flag.Parse()

	if *password == "" {
		*password = os.Getenv("FROSTRELAY_PASSWORD")
	}
	if *password == "" {
		log.Fatal("задайте пароль источника: -password или FROSTRELAY_PASSWORD")
	}
	if !strings.HasPrefix(*mount, "/") {
		*mount = "/" + *mount
	}
	if err := os.MkdirAll(*archiveDir, 0o755); err != nil {
		log.Fatalf("не удалось создать каталог архива: %v", err)
	}

	s := &server{
		hub:        newHub(),
		mount:      *mount,
		password:   *password,
		archiveDir: *archiveDir,
		name:       *name,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/status.json", s.handleStatus)
	mux.HandleFunc("/status-json.xsl", s.handleIcecastStatus)
	mux.HandleFunc("/admin/metadata", s.handleMetadata)
	mux.HandleFunc(*mount, s.handleMount)

	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// Write/Read timeouts не задаём: и источник, и слушатели —
		// долгоживущие потоковые соединения.
	}

	port := *listen
	if i := strings.LastIndexByte(port, ':'); i >= 0 {
		port = port[i+1:]
	}
	if _, err := strconv.Atoi(port); err != nil {
		port = "?"
	}
	fmt.Printf("frostrelay: страница http://localhost:%s/  ·  приём потока на %s  ·  архив в %s\n",
		port, *mount, *archiveDir)
	log.Fatal(srv.ListenAndServe())
}
