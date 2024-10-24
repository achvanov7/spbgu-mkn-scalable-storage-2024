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
	"syscall"
	"time"

	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

type Router struct {
}

func NewRouter(r *http.ServeMux, nodes [][]string) *Router {
	result := Router{}

	r.Handle("/", http.FileServer(http.Dir("../front/dist")))

	node := nodes[0][0]

	r.Handle("/insert", http.RedirectHandler("/"+node+"/insert", http.StatusTemporaryRedirect))
	r.Handle("/select", http.RedirectHandler("/"+node+"/select", http.StatusTemporaryRedirect))
	r.Handle("/replace", http.RedirectHandler("/"+node+"/replace", http.StatusTemporaryRedirect))
	r.Handle("/delete", http.RedirectHandler("/"+node+"/delete", http.StatusTemporaryRedirect))
	r.Handle("/checkpoint", http.RedirectHandler("/"+node+"/checkpoint", http.StatusTemporaryRedirect))

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

type Storage struct {
	name         string
	ctx          context.Context
	cancel       context.CancelFunc
	messages     chan Message
	transactions chan Transaction
}

type Engine struct {
	lsn   uint64
	log   string
	path  string
	rTree rtree.RTreeG[*geojson.Feature]
	data  map[string]*geojson.Feature
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

func handleGet(mux *http.ServeMux, method string, name string, messages chan Message, transactions chan Transaction) {
	slog.Info("GET " + method)
	mux.HandleFunc("/"+name+method, func(w http.ResponseWriter, r *http.Request) {
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
	result := Storage{name, ctx, cancel, make(chan Message), make(chan Transaction)}
	slog.Info("New Storage")

	handlePost(mux, "/insert", name, result.messages, result.transactions)
	handlePost(mux, "/delete", name, result.messages, result.transactions)
	handlePost(mux, "/replace", name, result.messages, result.transactions)
	handlePost(mux, "/checkpoint", name, result.messages, result.transactions)

	handleGet(mux, "/select", name, result.messages, result.transactions)

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
		data:  make(map[string]*geojson.Feature),
		rTree: rtree.RTreeG[*geojson.Feature]{},
		log:   log,
		lsn:   0,
		path:  path,
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

		_, err = SaveSnapshot(engine)
		if err != nil {
			panic(err.Error())
		}

		for {
			select {
			case <-r.ctx.Done():
				return
			case transaction := <-r.transactions:
				transaction.LSN = engine.lsn
				message, err := ProcessTransaction(engine, transaction)
				if err != nil {
					r.messages <- Message{err: err, data: nil}
				} else {
					file, err := os.OpenFile(engine.log, os.O_WRONLY|os.O_APPEND, 0666)
					if err != nil {
						panic(err.Error())
					}
					r.messages <- Message{err: LogTransaction(file, transaction), data: message}
					err = file.Close()
					if err != nil {
						panic(err.Error())
					}
				}
				engine.lsn++
			}
		}
	}()
}

func main() {
	r := http.ServeMux{}

	storage := NewStorage(&r, "test", []string{}, true)
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
	err := l.ListenAndServe() // http event loop
	if !errors.Is(err, http.ErrServerClosed) {
		slog.Info("err", "err", err)
	}

	router.Stop()

	storage.Stop()
}
