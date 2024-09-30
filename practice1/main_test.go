package main

import (
	"bytes"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func sendRequest(t *testing.T, mux *http.ServeMux, method string, url string, feature *geojson.Feature) *httptest.ResponseRecorder {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func sendRedirectedRequest(t *testing.T, mux *http.ServeMux, method string, url string, feature *geojson.Feature) *httptest.ResponseRecorder {
	rr := sendRequest(t, mux, method, url, feature)
	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}
	return sendRequest(t, mux, method, rr.Header().Get("location"), feature)
}

func createFeature(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	rr := sendRedirectedRequest(t, mux, "POST", "/insert", feature)
	if rr.Code != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func updateFeature(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	rr := sendRedirectedRequest(t, mux, "POST", "/replace", feature)
	if rr.Code != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func deleteFeature(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	rr := sendRedirectedRequest(t, mux, "POST", "/delete", feature)
	if rr.Code != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func sendGetRequest(t *testing.T, mux *http.ServeMux, url string) *httptest.ResponseRecorder {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func getFeatures(t *testing.T, mux *http.ServeMux) *geojson.FeatureCollection {
	rr := sendGetRequest(t, mux, "/select")
	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}
	rr = sendGetRequest(t, mux, rr.Header().Get("location"))
	if rr.Code != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
	return unmarshalFeatureCollection(t, rr.Body.Bytes())
}

func unmarshalFeatureCollection(t *testing.T, data []byte) *geojson.FeatureCollection {
	collection, err := geojson.UnmarshalFeatureCollection(data)
	if err != nil {
		t.Fatal(err)
	}
	return collection
}

func TestStorage(t *testing.T) {
	mux := http.NewServeMux()

	s := NewStorage(mux, "test", []string{}, true)
	if s == nil {
		t.Fatal("NewStorage returned nil")
	}
	s.Run()

	r := NewRouter(mux, [][]string{{"test"}})
	if r == nil {
		t.Fatal("NewRouter returned nil")
	}
	r.Run()

	t.Cleanup(func() {
		r.Stop()
		s.Stop()
	})

	point := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	point.ID = "1"

	createFeature(t, mux, point)
	collection := getFeatures(t, mux)
	if len(collection.Features) != 1 {
		t.Fatalf("Len is not 1")
	}
	if !orb.Equal(point.Geometry, collection.Features[0].Geometry) {
		t.Fatalf("Points are not equal")
	}

	line := geojson.NewFeature(orb.LineString{{rand.Float64(), rand.Float64()}})
	line.ID = "1"

	updateFeature(t, mux, line)
	collection = getFeatures(t, mux)
	if len(collection.Features) != 1 {
		t.Fatalf("Len is not 1")
	}
	if !orb.Equal(line.Geometry, collection.Features[0].Geometry) {
		t.Fatalf("Lines are not equal")
	}

	deleteFeature(t, mux, line)
	collection = getFeatures(t, mux)
	if len(collection.Features) != 0 {
		t.Fatalf("Len is not 0")
	}
}
