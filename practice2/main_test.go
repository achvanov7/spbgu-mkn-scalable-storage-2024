package main

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func TestBasicFunctionality(t *testing.T) {
	mux := http.NewServeMux()
	storage := NewStorage(mux, "GlobalTest", []string{}, true)
	storage.Run("testSnap/", "test.log")
	router := NewRouter(mux, [][]string{{"GlobalTest"}})
	router.Run()

	performInsertTest(t, mux, storage)
	performReplaceTest(t, mux, storage)
	performDeleteTest(t, mux, storage)

	t.Cleanup(router.Stop)
	t.Cleanup(storage.Stop)
}

func performPostTest(t *testing.T, mux *http.ServeMux, endpoint string, feature *geojson.Feature) {
	data, err := json.Marshal(feature)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, req)
	if recorder.Code == http.StatusTemporaryRedirect {
		req, err = http.NewRequest("POST", recorder.Header().Get("Location"), bytes.NewReader(data)) // Use recorder.Header() here
		if err != nil {
			t.Fatal(err)
		}
		recorder = httptest.NewRecorder()
		mux.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Errorf("expected %d, got %d", http.StatusOK, recorder.Code)
		}
	} else if recorder.Code != http.StatusOK {
		t.Errorf("expected %d, got %d", http.StatusOK, recorder.Code)
	}
}

func performInsertTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	pointFeature := geojson.NewFeature(orb.Point{rand.NormFloat64(), rand.NormFloat64()})
	pointFeature.ID = uuid.New().String()

	performPostTest(t, mux, "/"+storage.name+"/insert", pointFeature)
}

func performReplaceTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	lineFeature := geojson.NewFeature(orb.LineString{orb.Point{rand.NormFloat64(), rand.NormFloat64()}, orb.Point{rand.NormFloat64(), rand.NormFloat64()}})
	lineFeature.ID = uuid.New().String()

	performPostTest(t, mux, "/"+storage.name+"/replace", lineFeature)
}

func performDeleteTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	pointFeature := geojson.NewFeature(orb.Point{rand.NormFloat64(), rand.NormFloat64()})
	pointFeature.ID = uuid.New().String()

	performPostTest(t, mux, "/"+storage.name+"/insert", pointFeature)

	performPostTest(t, mux, "/"+storage.name+"/delete", pointFeature)
}
