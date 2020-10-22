package main

import (
	"fmt"
	"reflect"
	"testing"
)

var expectedValue = map[string]string{
	"paperback": "5",
	"language":  "English",
	"weight":    "100 g",
}

func TestJsonStore(t *testing.T) {
	db, err := InitJsonStore()
	failOnErr(err)
	failOnErr(db.clear())

	failOnErr(db.insert("path", "key", expectedValue))
	actualValue, err := db.QueryValue("path", "key")
	failOnErr(err)
	if !reflect.DeepEqual(expectedValue, actualValue) {
		t.Fatalf("Value %v not equal to expected %v", actualValue, expectedValue)
	}
}

func TestHStore(t *testing.T) {
	db, err := InitHStore()
	failOnErr(err)
	failOnErr(db.clear())

	failOnErr(db.insert("path", "key", expectedValue))
	actualValue, err := db.QueryValue("path", "key")
	failOnErr(err)
	if !reflect.DeepEqual(expectedValue, actualValue) {
		t.Fatalf("Value  \n%v not equal to expected \n%v", actualValue, expectedValue)
	}
	actualValue, err = db.QueryValueHStoreToJson("path", "key")
	failOnErr(err)
	if !reflect.DeepEqual(expectedValue, actualValue) {
		t.Fatalf("Value  \n%v not equal to expected \n%v", actualValue, expectedValue)
	}
}

func BenchmarkJsonStoreInsert(b *testing.B) {
	db, err := InitJsonStore()
	failOnErr(err)
	failOnErr(db.clear())
	for n := 0; n < b.N; n++ {
		failOnErr(db.insert("path", fmt.Sprintf("%d", n), expectedValue))
	}
}

func BenchmarkHStoreInsert(b *testing.B) {
	db, err := InitHStore()
	failOnErr(err)
	failOnErr(db.clear())
	for n := 0; n < b.N; n++ {
		failOnErr(db.insert("path", fmt.Sprintf("%d", n), expectedValue))
	}
}

func BenchmarkJsonStoreQueryValue(b *testing.B) {
	db, err := InitJsonStore()
	failOnErr(err)
	failOnErr(db.clear())
	failOnErr(db.insert("path", "key", expectedValue))
	for n := 0; n < b.N; n++ {
		_, err := db.QueryValue("path", "key")
		failOnErr(err)
	}
}

func BenchmarkHStoreQueryValue(b *testing.B) {
	db, err := InitHStore()
	failOnErr(err)
	failOnErr(db.clear())
	failOnErr(db.insert("path", "key", expectedValue))
	for n := 0; n < b.N; n++ {
		_, err := db.QueryValue("path", "key")
		failOnErr(err)
	}
}

func BenchmarkHStoreToJsonQueryValue(b *testing.B) {
	db, err := InitHStore()
	failOnErr(err)
	failOnErr(db.clear())
	failOnErr(db.insert("path", "key", expectedValue))
	for n := 0; n < b.N; n++ {
		_, err := db.QueryValueHStoreToJson("path", "key")
		failOnErr(err)
	}
}
