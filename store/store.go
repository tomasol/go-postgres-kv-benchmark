package store

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/joho/godotenv/autoload"
	"github.com/sirupsen/logrus"
)

type HStore struct {
	connectionPool pgxpool.Pool
}
type JsonStore struct {
	connectionPool pgxpool.Pool
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

func (db HStore) Close() {
	db.connectionPool.Close()
}

func (db JsonStore) Close() {
	db.connectionPool.Close()
}

func (db JsonStore) Clear() error {
	_, err := db.connectionPool.Exec(context.Background(),
		"DELETE FROM kv_json")
	return err
}

func (db HStore) Clear() error {
	_, err := db.connectionPool.Exec(context.Background(),
		"DELETE FROM kv_hstore")
	return err
}

func (db JsonStore) Insert(path string, key string, value map[string]string) error {
	_, err := db.connectionPool.Exec(context.Background(),
		"INSERT INTO kv_json(path,key,value) VALUES ($1,$2,$3)",
		path, key, value,
	)
	return err
}

func (db HStore) Insert(path string, key string, value map[string]string) error {
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

func (db JsonStore) Count() (int, error) {
	rows, err := db.connectionPool.Query(context.Background(), "SELECT COUNT(*) FROM kv_json")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if rows.Next() {
		var result int
		err = rows.Scan(&result)
		if err != nil {
			return 0, err
		}
		return result, nil
	}
	return 0, errors.New("Unreachable")
}

func (db JsonStore) QueryValue(path string, key string) (map[string]string, error) {
	rows, err := db.connectionPool.Query(context.Background(), "SELECT value FROM kv_json WHERE path=$1 AND key=$2",
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

// manual parsing
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

func (db HStore) QueryValueHStoreToJson(path string, key string) (map[string]string, error) {
	rows, err := db.connectionPool.Query(context.Background(), "SELECT hstore_to_json (value) FROM kv_hstore WHERE path=$1 AND key=$2",
		path, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var result map[string]string
		err = rows.Scan(&result)
		return result, err
	}
	return nil, nil
}
