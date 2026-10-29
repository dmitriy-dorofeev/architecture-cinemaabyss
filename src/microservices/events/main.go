package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

var topics = []string{"movie-events", "user-events", "payment-events"}

type eventService struct {
	writers map[string]*kafka.Writer
}

func main() {
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := waitForKafka(ctx, brokers[0]); err != nil {
		log.Fatalf("Kafka is unavailable: %v", err)
	}

	s := &eventService{writers: make(map[string]*kafka.Writer, len(topics))}
	for _, topic := range topics {
		s.writers[topic] = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireOne,
			WriteTimeout: 10 * time.Second,
		}
	}

	var consumers sync.WaitGroup
	readers := make([]*kafka.Reader, 0, len(topics))
	for _, topic := range topics {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  "events-service",
			MinBytes: 1,
			MaxBytes: 10 << 20,
			MaxWait:  time.Second,
		})
		readers = append(readers, reader)
		consumers.Add(1)
		go consume(ctx, &consumers, reader)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", healthHandler)
	mux.HandleFunc("/api/events/movie", s.publishHandler("movie-events"))
	mux.HandleFunc("/api/events/user", s.publishHandler("user-events"))
	mux.HandleFunc("/api/events/payment", s.publishHandler("payment-events"))

	server := &http.Server{
		Addr:              ":" + getenv("PORT", "8082"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("Starting events service on %s", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	for _, reader := range readers {
		if err := reader.Close(); err != nil {
			log.Printf("Kafka reader close error: %v", err)
		}
	}
	consumers.Wait()
	for _, writer := range s.writers {
		if err := writer.Close(); err != nil {
			log.Printf("Kafka writer close error: %v", err)
		}
	}
}

func (s *eventService) publishHandler(topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "Cannot read request body", http.StatusBadRequest)
			return
		}
		if !json.Valid(body) {
			http.Error(w, "Request body must contain valid JSON", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := s.writers[topic].WriteMessages(ctx, kafka.Message{Value: body}); err != nil {
			log.Printf("Cannot publish event to %s: %v", topic, err)
			http.Error(w, "Cannot publish event", http.StatusInternalServerError)
			return
		}

		log.Printf("Published event to %s", topic)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	}
}

func consume(ctx context.Context, wg *sync.WaitGroup, reader *kafka.Reader) {
	defer wg.Done()
	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Cannot read from %s: %v", reader.Config().Topic, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		log.Printf("Consumed event topic=%s partition=%d offset=%d payload=%s", message.Topic, message.Partition, message.Offset, message.Value)
	}
}

func waitForKafka(ctx context.Context, broker string) error {
	for attempt := 1; attempt <= 30; attempt++ {
		dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := kafka.DialContext(dialCtx, "tcp", broker)
		cancel()
		if err == nil {
			return conn.Close()
		}
		log.Printf("Waiting for Kafka, attempt %d/30", attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return errors.New("connection attempts exceeded")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
