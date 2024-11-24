package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func setupTestStorage(t *testing.T) (*Storage, *http.ServeMux) {
	mux := &http.ServeMux{}
	storage := NewStorage(mux, "test", []string{}, true)
	storage.Run("test_snapshots/", "test.log")

	router := NewRouter(mux, [][]string{{"node1"}, {"node2"}})
	router.Run()

	return storage, mux
}

// Helper function to send transaction and wait for response
func sendTransaction(storage *Storage, transaction Transaction) error {
	transaction.LSN = 1
	storage.transactions <- transaction

	select {
	case response := <-storage.messages:
		return response.err
	case <-time.After(5 * time.Second):
		return errors.New("transaction processing timeout")
	}
}

// Test request counting and redirection
func TestRequestCounting(t *testing.T) {
	storage, mux := setupTestStorage(t)
	defer storage.Stop()

	server := httptest.NewServer(mux)
	defer server.Close()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(server.URL + "/test/select")
			if err != nil {
				t.Errorf("Failed to make request: %v", err)
				return
			}
			defer resp.Body.Close()

			// Check if we get redirected after max requests
			if storage.engine.requests["/select"] > storage.engine.maxReqs {
				if resp.StatusCode != http.StatusTemporaryRedirect {
					t.Errorf("Expected redirect status, got %d", resp.StatusCode)
				}
			}
		}()
	}
	wg.Wait()
}

// Test vector clock functionality
func TestVectorClock(t *testing.T) {
	storage, _ := setupTestStorage(t)
	defer storage.Stop()

	t.Logf("Initial vclock state: %v", storage.engine.vclock)

	transaction := Transaction{
		Name:   "test",
		Action: "/insert",
		LSN:    1,
		Feature: geojson.Feature{
			ID:       "test-feature",
			Geometry: orb.Point{0, 0},
		},
	}

	if err := sendTransaction(storage, transaction); err != nil {
		t.Fatalf("Failed to process transaction: %v", err)
	}

	t.Logf("Final vclock state: %v", storage.engine.vclock)

	if storage.engine.vclock["test"] != 1 {
		t.Errorf("Expected vclock[test] to be 1, got %d", storage.engine.vclock["test"])
	}
}

// Test leader-only write operations
func TestLeaderOnlyWrites(t *testing.T) {
	mux := &http.ServeMux{}

	router := NewRouter(mux, [][]string{{"leader"}, {"follower"}})
	router.Run()

	leader := NewStorage(mux, "leader", []string{}, true)
	leader.Run("test_snapshots/", "leader.log")
	defer leader.Stop()

	follower := NewStorage(mux, "follower", []string{}, false)
	follower.Run("test_snapshots/", "follower.log")
	defer follower.Stop()

	server := httptest.NewServer(mux)
	defer server.Close()

	feature := geojson.Feature{
		ID:         "test-feature-1",
		Geometry:   orb.Point{0, 0},
		Properties: nil,
	}
	body, _ := json.Marshal(feature)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	url := server.URL + "/insert"
	t.Logf("Making request to: %s", url)

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	t.Logf("Response status: %d", resp.StatusCode)
	t.Logf("Response headers: %v", resp.Header)

	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf("Expected redirect status (307), got %d", resp.StatusCode)
	}
}

// Test websocket replication
func TestWebsocketReplication(t *testing.T) {
	storage, mux := setupTestStorage(t)
	defer storage.Stop()

	select {
	case <-storage.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for engine to be ready")
	}

	server := httptest.NewServer(mux)
	defer server.Close()

	wsURL := "ws" + server.URL[4:] + "/test/replication"

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal("dial:", err)
	}
	defer c.Close()

	time.Sleep(200 * time.Millisecond) // Increased sleep time

	storage.engine.replicaMu.RLock()
	count := len(storage.engine.replicas)
	storage.engine.replicaMu.RUnlock()

	if count != 1 {
		t.Errorf("Expected 1 replica connection, got %d", count)
	}
}

// Test infinite redirect protection
func TestRedirectProtection(t *testing.T) {
	storage, mux := setupTestStorage(t)
	defer storage.Stop()

	server := httptest.NewServer(mux)
	defer server.Close()

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, _ := http.NewRequest("GET", server.URL+"/select", nil)
	req.Header.Set("X-Redirect-Count", "3") // Max redirects

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	t.Logf("Response status: %d", resp.StatusCode)
	if resp.StatusCode != http.StatusLoopDetected {
		t.Errorf("Expected status loop detected, got %d", resp.StatusCode)
	}
}
