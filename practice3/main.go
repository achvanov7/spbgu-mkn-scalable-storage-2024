package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"sync"

	"github.com/gorilla/websocket"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

type Router struct {
	nodes            [][]string
	currentNodeIndex map[string]int // Track which node to redirect to for each path
	maxRedirects     int            // Prevent infinite redirects
}

func NewRouter(r *http.ServeMux, nodes [][]string) *Router {
	result := Router{
		nodes:            nodes,
		currentNodeIndex: make(map[string]int),
		maxRedirects:     3,
	}

	handleRedirect := func(w http.ResponseWriter, r *http.Request, target string) {
		slog.Info("Handling redirect", "from", r.URL.Path, "to", target)
		// Check for redirect loop
		redirectCount := 0
		if count := r.Header.Get("X-Redirect-Count"); count != "" {
			redirectCount, _ = strconv.Atoi(count)
			if redirectCount >= result.maxRedirects {
				w.WriteHeader(http.StatusLoopDetected)
				return
			}
		}

		redirectCount++
		w.Header().Set("Location", target)
		w.Header().Set("X-Redirect-Count", strconv.Itoa(redirectCount))
		w.WriteHeader(http.StatusTemporaryRedirect)
	}

	leaderNode := nodes[0][0]
	slog.Info("Router setup", "leaderNode", leaderNode)

	// Write operations should always go to leader
	writeOps := []string{"/insert", "/replace", "/delete", "/checkpoint"}
	for _, op := range writeOps {
		pathOp := op
		slog.Info("Registering write operation", "path", pathOp)
		// Handle both patterns
		r.HandleFunc(pathOp, func(w http.ResponseWriter, r *http.Request) {
			slog.Info("Handling write operation redirect", "path", r.URL.Path)
			handleRedirect(w, r, "/"+leaderNode+pathOp)
		})
	}

	// Read operations can be load balanced
	readOps := []string{"/select"}
	for _, op := range readOps {
		pathOp := op
		r.HandleFunc(pathOp, func(w http.ResponseWriter, r *http.Request) {
			slog.Info("Handling read operation", "path", r.URL.Path)
			if count := r.Header.Get("X-Redirect-Count"); count != "" {
				redirectCount, _ := strconv.Atoi(count)
				if redirectCount >= result.maxRedirects {
					w.WriteHeader(http.StatusLoopDetected)
					return
				}
			}
			currentIndex := result.currentNodeIndex[pathOp]
			targetNode := nodes[currentIndex][0]
			result.currentNodeIndex[pathOp] = (currentIndex + 1) % len(nodes)
			handleRedirect(w, r, "/"+targetNode+pathOp)
		})
	}

	return &result
}

func (r *Router) Run() {

}

func (r *Router) Stop() {

}

type Transaction struct {
	Action  string
	Name    string
	LSN     uint64
	Feature geojson.Feature
}

func (t Transaction) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Action  string          `json:"action"`
		Name    string          `json:"name"`
		LSN     uint64          `json:"lsn"`
		Feature geojson.Feature `json:"feature"`
	}{
		Action:  t.Action,
		Name:    t.Name,
		LSN:     t.LSN,
		Feature: t.Feature,
	})
}

type Message struct {
	data []byte
	err  error
}

type Replica struct {
	conn   *websocket.Conn
	vclock map[string]uint64
}

type Storage struct {
	name         string
	ctx          context.Context
	cancel       context.CancelFunc
	messages     chan Message
	transactions chan Transaction
	leader       bool
	replicas     []string
	engine       *Engine
	ready        chan struct{}
}

type Engine struct {
	lsn       uint64
	log       string
	path      string
	rTree     rtree.RTreeG[*geojson.Feature]
	data      map[string]*geojson.Feature
	vclock    map[string]uint64
	replicas  map[string]*Replica
	replicaMu sync.RWMutex
	upgrader  websocket.Upgrader
	requests  map[string]int
	requestMu sync.RWMutex
	maxReqs   int
}

func WriteError(w http.ResponseWriter, err error) {
	slog.Error(err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

func handlePost(mux *http.ServeMux, method string, name string, messages chan Message, transactions chan Transaction) {
	slog.Info("POST " + method)
	mux.HandleFunc("/"+name+method, func(w http.ResponseWriter, r *http.Request) {
		var feat geojson.Feature
		err := json.NewDecoder(r.Body).Decode(&feat)
		if err != nil {
			WriteError(w, err)
			return
		}
		transactions <- Transaction{Name: name, Action: method, Feature: feat}
		response := <-messages
		if response.err != nil {
			WriteError(w, response.err)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

func handleGet(mux *http.ServeMux, method string, name string, messages chan Message, transactions chan Transaction, engine *Engine) {
	slog.Info("GET " + method)
	mux.HandleFunc("/"+name+method, func(w http.ResponseWriter, r *http.Request) {
		// Track concurrent requests
		engine.requestMu.Lock()
		engine.requests[method]++
		engine.requestMu.Unlock()

		defer func() {
			engine.requestMu.Lock()
			engine.requests[method]--
			engine.requestMu.Unlock()
		}()

		transactions <- Transaction{Name: name, Action: method, Feature: geojson.Feature{}}
		response := <-messages
		if response.err != nil {
			WriteError(w, response.err)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(response.data)
		if err != nil {
			WriteError(w, err)
			return
		}
	})
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize the engine first
	engine := &Engine{
		data:     make(map[string]*geojson.Feature),
		rTree:    rtree.RTreeG[*geojson.Feature]{},
		vclock:   make(map[string]uint64),
		replicas: make(map[string]*Replica),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		requests:  make(map[string]int),
		requestMu: sync.RWMutex{},
		maxReqs:   10,
	}

	result := Storage{
		name:         name,
		ctx:          ctx,
		cancel:       cancel,
		messages:     make(chan Message),
		transactions: make(chan Transaction),
		leader:       leader,
		replicas:     replicas,
		engine:       engine,
		ready:        make(chan struct{}),
	}

	slog.Info("New Storage")

	// Only register handlers if this is the leader node or if it's a read operation
	if leader {
		handlePost(mux, "/insert", name, result.messages, result.transactions)
		handlePost(mux, "/delete", name, result.messages, result.transactions)
		handlePost(mux, "/replace", name, result.messages, result.transactions)
		handlePost(mux, "/checkpoint", name, result.messages, result.transactions)
	}
	// Read operations can be handled by any node
	handleGet(mux, "/select", name, result.messages, result.transactions, engine)

	mux.HandleFunc("/"+name+"/replication", result.handleWebsocket)

	return &result
}

func (r *Storage) Run(path string, log string) {
	slog.Info("Storage running on " + path)
	RunEngine(r, path, log)
}

func (r *Storage) Stop() {
	slog.Info("Stop Storage")
	r.cancel()
}

func SaveSnapshot(engine *Engine) (string, error) {
	filePath := engine.path + "snapshot-" + time.Now().Format("2006-01-02_15-04-05") + ".bin"

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return "", err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			slog.Error("Error closing file", "error", closeErr)
		}
	}()

	for _, feature := range engine.data {
		if err := LogTransaction(file, Transaction{Action: "/insert", Feature: *feature}); err != nil {
			return "", err
		}
	}

	if err := os.Truncate(engine.log, 0); err != nil {
		return "", err
	}

	return filePath, nil
}

func ProcessTransaction(engine *Engine, transaction Transaction) ([]byte, error) {
	switch transaction.Action {
	case "/insert":
		if transaction.Feature.ID == nil {
			return nil, errors.New("feature ID cannot be nil")
		}
		engine.data[transaction.Feature.ID.(string)] = &transaction.Feature
		engine.rTree.Insert(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, &transaction.Feature)

	case "/replace":
		engine.rTree.Replace(
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max,
			engine.data[transaction.Feature.ID.(string)],
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max,
			&transaction.Feature,
		)
		engine.data[transaction.Feature.ID.(string)] = &transaction.Feature

	case "/delete":
		engine.rTree.Delete(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, &transaction.Feature)
		delete(engine.data, transaction.Feature.ID.(string))

	case "/checkpoint":
		fileName, err := SaveSnapshot(engine)
		if err != nil {
			return nil, err
		}
		return []byte("Saved: " + fileName), nil

	case "/select":
		featureCollection := geojson.NewFeatureCollection()
		engine.rTree.Scan(func(min, max [2]float64, data *geojson.Feature) bool {
			featureCollection.Features = append(featureCollection.Features, data)
			return true
		})
		marshal, err := json.Marshal(featureCollection)
		if err != nil {
			return nil, err
		}
		return marshal, nil

	default:
		return nil, errors.New("unknown transaction action: " + transaction.Action)
	}

	return nil, nil
}

func LogTransaction(file *os.File, transaction Transaction) error {
	marshal, err := json.Marshal(transaction)
	if err != nil {
		return err
	}
	_, err = file.WriteString(string(marshal) + "\n")
	return err
}

func NewEngine(path string, log string) (*Engine, error) {
	engine := Engine{
		data:      make(map[string]*geojson.Feature),
		rTree:     rtree.RTreeG[*geojson.Feature]{},
		log:       log,
		lsn:       0,
		path:      path,
		vclock:    make(map[string]uint64),
		replicas:  make(map[string]*Replica),
		upgrader:  websocket.Upgrader{},
		requests:  make(map[string]int),
		requestMu: sync.RWMutex{},
		maxReqs:   10,
	}

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(log, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var lastFile string
	var lastTime int64

	for _, snapshot := range files {
		stat, err := os.Stat(path + snapshot.Name())
		if err != nil {
			return nil, err
		}
		currentTime := stat.ModTime().Unix()
		if lastTime < currentTime {
			lastFile = snapshot.Name()
			lastTime = currentTime
		}
	}

	if err := LoadTransaction(&engine, engine.path+lastFile); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if err := LoadTransaction(&engine, engine.log); err != nil {
		return nil, err
	}

	if err := file.Close(); err != nil {
		return nil, err
	}

	return &engine, nil
}

func LoadTransaction(engine *Engine, fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var transaction Transaction
		err := json.Unmarshal(scanner.Bytes(), &transaction)
		slog.Info(transaction.Action)
		if err != nil {
			return err
		}
		_, err = ProcessTransaction(engine, transaction)
		if err != nil {
			return err
		}
	}

	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			return
		}
	}(file)
	return nil
}

func RunEngine(r *Storage, path string, log string) {
	go func() {
		engine, err := NewEngine(path, log)
		if err != nil {
			panic(err.Error())
		}

		r.engine = engine
		close(r.ready)

		for _, replicaAddr := range r.replicas {
			go connectToReplica(engine, replicaAddr)
		}

		for {
			select {
			case <-r.ctx.Done():
				return
			case transaction := <-r.transactions:
				if r.leader && transaction.Name == r.name {
					engine.lsn++
					transaction.LSN = engine.lsn
				}

				message, err := ProcessTransaction(engine, transaction)

				if transaction.LSN > engine.vclock[transaction.Name] {
					engine.vclock[transaction.Name] = transaction.LSN
				}

				if transaction.Name == r.name {
					engine.replicaMu.RLock()
					for _, replica := range engine.replicas {
						replica.conn.WriteJSON(transaction)
					}
					engine.replicaMu.RUnlock()
				}

				var logErr error
				if err == nil {
					file, fileErr := os.OpenFile(engine.log, os.O_WRONLY|os.O_APPEND, 0666)
					if fileErr != nil {
						logErr = fileErr
					} else {
						logErr = LogTransaction(file, transaction)
						file.Close()
					}
				}

				r.messages <- Message{
					err:  errors.Join(err, logErr),
					data: message,
				}
			}
		}
	}()
}

func connectToReplica(engine *Engine, addr string) {
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial("ws://"+addr+"/replication", nil)
	if err != nil {
		slog.Error("failed to connect to replica", "addr", addr, "error", err)
		return
	}

	replica := &Replica{
		conn:   conn,
		vclock: make(map[string]uint64),
	}

	engine.replicaMu.Lock()
	engine.replicas[addr] = replica
	engine.replicaMu.Unlock()
}

func (s *Storage) handleWebsocket(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("websocket upgrade failed", "error", err)
		return
	}

	replica := &Replica{
		conn:   conn,
		vclock: make(map[string]uint64),
	}

	s.engine.replicaMu.Lock()
	s.engine.replicas[conn.RemoteAddr().String()] = replica
	s.engine.replicaMu.Unlock()

	go func() {
		defer conn.Close()
		for {
			var transaction Transaction
			err := conn.ReadJSON(&transaction)
			if err != nil {
				break
			}

			if s.engine.vclock[transaction.Name] < transaction.LSN {
				s.transactions <- transaction
			}
		}

		s.engine.replicaMu.Lock()
		delete(s.engine.replicas, conn.RemoteAddr().String())
		s.engine.replicaMu.Unlock()
	}()
}

func main() {
	r := http.ServeMux{}

	nodes := []struct {
		name     string
		addr     string
		replicas []string
		leader   bool
	}{
		{
			name:     "node1",
			addr:     "localhost:8080",
			replicas: []string{"localhost:8081", "localhost:8082"},
			leader:   true,
		},
		{
			name:     "node2",
			addr:     "localhost:8081",
			replicas: []string{"localhost:8080", "localhost:8082"},
			leader:   false,
		},
		{
			name:     "node3",
			addr:     "localhost:8082",
			replicas: []string{"localhost:8080", "localhost:8081"},
			leader:   false,
		},
	}

	for _, node := range nodes {
		storage := NewStorage(&r, node.name, node.replicas, node.leader)
		storage.Run("snapshots/", storage.name+".log")

		router := NewRouter(&r, [][]string{{"test"}})
		router.Run()

		l := http.Server{}
		l.Addr = "127.0.0.1:8080"
		l.Handler = &r

		go func() {
			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
			sig := <-sigs
			slog.Info("got", "signal", sig)
			for range sigs {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				l.Shutdown(ctx)
			}
		}()

		defer slog.Info("we are going down")
		slog.Info("listen http://" + l.Addr)
		err := l.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Info("err", "err", err)
		}

		router.Stop()

		storage.Stop()
	}
}
