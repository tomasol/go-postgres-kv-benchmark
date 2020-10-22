package main

import (
	"context"
	"os"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/joho/godotenv/autoload"
	"github.com/sirupsen/logrus"
)

func main() {
	db, err := InitJsonStore()
	failOnErr(err)
	value := map[string]string{"json": "json"}

	failOnErr(db.clear())
	failOnErr(db.insert("path", "1", value))
}

func failOnErr(err error) {
	if err != nil {
		logrus.Fatalf("Unable to connection to database: %v", err)
	}
}

type HStore struct {
	connectionPool pgxpool.Pool
}
type JsonStore struct {
	connectionPool pgxpool.Pool
}

func initConnectionPool() (*pgxpool.Pool, error) {
	dbURL, ok := os.LookupEnv("POSTGRES_DATABASE_URL")
	if !ok {
		dbURL = "host=127.0.0.1 port=5432 user=postgres password=postgres database=kvstore"
	}
	return pgxpool.Connect(context.Background(), dbURL)
}

func InitHStore() (*HStore, error) {
	connectionPool, err := initConnectionPool()
	if err != nil {
		return nil, err
	}
	return &HStore{*connectionPool}, nil
}

func InitJsonStore() (*JsonStore, error) {
	connectionPool, err := initConnectionPool()
	if err != nil {
		return nil, err
	}
	return &JsonStore{*connectionPool}, nil
}

func (db JsonStore) clear() error {
	_, err := db.connectionPool.Exec(context.Background(),
		"DELETE FROM kv_jsonb")
	return err
}

func (db HStore) clear() error {
	_, err := db.connectionPool.Exec(context.Background(),
		"DELETE FROM kv_hstore")
	return err
}

func (db JsonStore) insert(path string, key string, value map[string]string) error {
	_, err := db.connectionPool.Exec(context.Background(),
		"INSERT INTO kv_jsonb(path,key,value) VALUES ($1,$2,$3)",
		path, key, value,
	)
	return err
}

func (db HStore) insert(path string, key string, value map[string]string) error {
	// serialize map to string  `"key1" => "val1", "key2" => "val2"`
	var stringValue string
	for k, v := range value {
		if len(stringValue) > 0 {
			stringValue += ","
		}
		stringValue += `"` + k + `" => "` + v + `"`
	}
	_, err := db.connectionPool.Exec(context.Background(),
		"INSERT INTO kv_hstore(path,key,value) VALUES ($1,$2,$3)",
		path, key, stringValue,
	)
	return err
}

func (db JsonStore) QueryValue(path string, key string) (map[string]string, error) {
	rows, err := db.connectionPool.Query(context.Background(), "SELECT value FROM kv_jsonb WHERE path=$1 AND key=$2",
		path, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var value map[string]string
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, nil
}

func (db HStore) QueryValue(path string, key string) (map[string]string, error) {
	rows, err := db.connectionPool.Query(context.Background(), "SELECT value FROM kv_hstore WHERE path=$1 AND key=$2",
		path, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var stringValue string
		err = rows.Scan(&stringValue)
		if err != nil {
			return nil, err
		}
		// deserialize to map
		kvs := strings.Split(stringValue, ", ")
		logrus.Debugf("Deserializing '%s' into %d splits", stringValue, len(kvs))
		result := make(map[string]string)
		for _, kvString := range kvs {
			logrus.Debugf("Deserializing kv '%s'", kvString)
			kvSlice := strings.Split(kvString, `"=>"`)
			key := strings.TrimSuffix(strings.TrimPrefix(kvSlice[0], `"`), `"`)
			val := strings.TrimSuffix(strings.TrimPrefix(kvSlice[1], `"`), `"`)
			logrus.Debugf("Deserializing kv key '%s' val '%s'", key, val)
			result[key] = val
		}
		return result, nil
	}
	return nil, nil
}
