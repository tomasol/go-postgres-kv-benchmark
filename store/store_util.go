package store

import (
	"context"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/joho/godotenv/autoload"
	"github.com/sirupsen/logrus"
)

func FailOnErr(err error) {
	if err != nil {
		logrus.Fatalf("Unable to connection to database: %v", err)
	}
}

func initConnectionPool() (*pgxpool.Pool, error) {
	dbURL, ok := os.LookupEnv("POSTGRES_DATABASE_URL")
	if !ok {
		dbURL = "host=127.0.0.1 port=5432 user=postgres password=postgres database=kvstore pool_max_conns=10"
	}
	return pgxpool.Connect(context.Background(), dbURL)
}
