package main

import (
	"pgstore/pgstore/store"
	"reflect"
	"testing"
)

var expectedValue = map[string]string{
	"paperback": "5",
	"language":  "English",
	"weight":    "100 g",
}

func TestJsonStore(t *testing.T) {
	db, err := store.InitJsonStore()
	store.FailOnErr(err)
	defer db.Close()
	store.FailOnErr(db.Clear())

	store.FailOnErr(db.Insert("path", "key", expectedValue))
	actualValue, err := db.QueryValue("path", "key")
	store.FailOnErr(err)
	if !reflect.DeepEqual(expectedValue, actualValue) {
		t.Fatalf("Value %v not equal to expected %v", actualValue, expectedValue)
	}
}

func TestHStore(t *testing.T) {
	db, err := store.InitHStore()
	store.FailOnErr(err)
	defer db.Close()
	store.FailOnErr(db.Clear())

	store.FailOnErr(db.Insert("path", "key", expectedValue))
	actualValue, err := db.QueryValue("path", "key")
	store.FailOnErr(err)
	if !reflect.DeepEqual(expectedValue, actualValue) {
		t.Fatalf("Value  \n%v not equal to expected \n%v", actualValue, expectedValue)
	}
	actualValue, err = db.QueryValueHStoreToJson("path", "key")
	store.FailOnErr(err)
	if !reflect.DeepEqual(expectedValue, actualValue) {
		t.Fatalf("Value  \n%v not equal to expected \n%v", actualValue, expectedValue)
	}
}
