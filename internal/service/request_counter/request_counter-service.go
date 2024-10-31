package requestcounter

import (
	"afgo/internal/models"
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type RequestCounterService interface {
	Add(counter models.RequestCounter) (uint64, error)
}

type requestCounterServiceImpl struct {
	ctx context.Context
	db  driver.Conn
}

func NewRequestCounterService(ctx context.Context, db driver.Conn) RequestCounterService {
	return &requestCounterServiceImpl{
		ctx: ctx,
		db:  db,
	}
}

func (s *requestCounterServiceImpl) Add(counter models.RequestCounter) (uint64, error) {
	now := time.Now().UTC().Format("2006-01-02")
	counter.Day = now

	batch, err := s.db.PrepareBatch(s.ctx, `insert into base_db.request_count`)
	if err != nil {
		return 0, err
	}
	err = batch.AppendStruct(&counter)
	if err != nil {
		return 0, err
	}
	err = batch.Send()
	if err != nil {
		return 0, err
	}

	query := `SELECT count(*)
			 FROM base_db.request_count
			 	WHERE path = ? 
			   	AND tracker = ?
			   	AND cabinet = ?
				AND day = ?`

	var c uint64
	err = s.db.QueryRow(s.ctx, query, counter.Path, counter.Tracker, counter.Cabinet, counter.Day).Scan(&c)
	if err != nil {
		return 0, err
	}
	return c, nil
}
