package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/paulmach/orb/geojson"
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
	return &result
}

func (r *Router) Run() {

}

func (r *Router) Stop() {

}

type Storage struct {
	name string
}

func GetFeature(r *http.Request) (*geojson.Feature, string, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, "", err
	}
	feat, err := geojson.UnmarshalFeature(body)
	if err != nil {
		return nil, "", err
	}
	id, ok := feat.ID.(string)
	if !ok {
		return nil, "", errors.New("Couldn't get id")
	}
	return feat, id, nil
}

func WriteError(w http.ResponseWriter, err error) {
	slog.Error(err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	result := Storage{name}
	slog.Info("New Storage")

	wd, err := os.Getwd()
	if err != nil {
		panic("Can't get working directory")
	}
	dir := wd + "/" + name

	err = os.MkdirAll(dir, 0777)
	if err != nil {
		panic("Failed to create " + dir)
	}

	mux.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Insert")
		feat, id, err := GetFeature(r)

		if err != nil {
			WriteError(w, err)
			return
		}

		json, err := feat.MarshalBSON()

		if err != nil {
			WriteError(w, err)
			return
		}

		err = os.WriteFile(dir+"/"+id+".json", json, 0666)
		if err != nil {
			WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Replace")
		feat, id, err := GetFeature(r)

		if err != nil {
			WriteError(w, err)
			return
		}

		json, err := feat.MarshalBSON()

		if err != nil {
			WriteError(w, err)
			return
		}

		err = os.WriteFile(dir+"/"+id+".json", json, 0666)
		if err != nil {
			WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Delete")
		_, id, err := GetFeature(r)

		if err != nil {
			WriteError(w, err)
			return
		}

		err = os.Remove(dir + "/" + id + ".json")
		if err != nil {
			WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Select")
		elems, err := os.ReadDir(dir)
		if err != nil {
			WriteError(w, err)
			return
		}
		collection := geojson.NewFeatureCollection()

		for _, elem := range elems {
			if elem.IsDir() {
				continue
			}

			bytes, err := os.ReadFile(dir + "/" + elem.Name())
			if err != nil {
				WriteError(w, err)
				return
			}

			slog.Info(elem.Type().String())

			feat := geojson.Feature{}
			err = feat.UnmarshalBSON(bytes)
			if err != nil {
				WriteError(w, err)
				return
			}

			collection.Append(&feat)
		}

		data, err := collection.MarshalJSON()
		if err != nil {
			WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	})

	return &result
}

func (r *Storage) Run() {

}

func (r *Storage) Stop() {

}

func main() {
	r := http.ServeMux{}

	storage := NewStorage(&r, "test", []string{}, true)
	storage.Run()

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
