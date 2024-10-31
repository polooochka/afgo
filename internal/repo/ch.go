package repo

import (
	"afgo/internal/configs"
	"context"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func MustChConn(cfg *configs.Config) driver.Conn {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ChHost + ":" + cfg.ChPort},
		Auth: clickhouse.Auth{
			Database: cfg.ChDatabase,
			Username: cfg.ChUser,
			Password: cfg.ChPassword,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "an-example-go-client", Version: "0.1"},
			},
		},

		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
	})

	if err != nil {
		log.Fatal("CH Conn err", err)
	}

	err = conn.Ping(context.Background())
	if err != nil {
		log.Fatal("CH Ping err", err)
	}
	return conn
}
