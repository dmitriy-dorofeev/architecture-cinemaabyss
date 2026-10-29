package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type service struct {
	monolith *httputil.ReverseProxy
	movies   *httputil.ReverseProxy
	events   *httputil.ReverseProxy
	gradual  bool
	percent  int
}

func main() {
	monolithURL := parseURL(getenv("MONOLITH_URL", "http://localhost:8080"))
	moviesURL := parseURL(getenv("MOVIES_SERVICE_URL", "http://localhost:8081"))
	eventsURL := parseURL(getenv("EVENTS_SERVICE_URL", "http://localhost:8082"))

	gradual, err := strconv.ParseBool(getenv("GRADUAL_MIGRATION", "false"))
	if err != nil {
		log.Fatalf("invalid GRADUAL_MIGRATION: %v", err)
	}

	percent, err := strconv.Atoi(getenv("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil || percent < 0 || percent > 100 {
		log.Fatal("MOVIES_MIGRATION_PERCENT must be an integer from 0 to 100")
	}

	s := &service{
		monolith: newProxy(monolithURL),
		movies:   newProxy(moviesURL),
		events:   newProxy(eventsURL),
		gradual:  gradual,
		percent:  percent,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.Handle("/", s)

	server := &http.Server{
		Addr:              ":" + getenv("PORT", "8000"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("Starting proxy on %s", server.Addr)
	log.Fatal(server.ListenAndServe())
}

func (s *service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/api/movies"):
		if s.gradual && rand.Intn(100) < s.percent {
			log.Printf("%s %s -> movies-service", r.Method, r.URL.RequestURI())
			s.movies.ServeHTTP(w, r)
			return
		}
		log.Printf("%s %s -> monolith", r.Method, r.URL.RequestURI())
		s.monolith.ServeHTTP(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/events"):
		s.events.ServeHTTP(w, r)
	default:
		s.monolith.ServeHTTP(w, r)
	}
}

func newProxy(target *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error for %s %s: %v", r.Method, r.URL.RequestURI(), err)
		http.Error(w, "Upstream service is unavailable", http.StatusBadGateway)
	}
	return proxy
}

func parseURL(value string) *url.URL {
	u, err := url.Parse(value)
	if err != nil || u.Scheme == "" || u.Host == "" {
		log.Fatalf("invalid service URL %q", value)
	}
	return u
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("Strangler Fig Proxy is healthy"))
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
