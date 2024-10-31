package dbsaver

import (
	"afgo/internal/models"
	"context"
	"encoding/csv"
	"io"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type DbSaverService interface {
	Save_InAppEvents(ur *models.UrlBuilderResponse) error
	Save(ur *models.UrlBuilderResponse) error
}

type dbSaverServiceImpl struct {
	ctx context.Context
	db  driver.Conn
}

func NewDbSaverService(ctx context.Context, db driver.Conn) DbSaverService {
	return &dbSaverServiceImpl{
		ctx: ctx,
		db:  db,
	}
}

func (s *dbSaverServiceImpl) Save(ur *models.UrlBuilderResponse) error {
	switch m := ur.Builder.Meta["method"]; m {
	case "in-app_events", "in-app-events-retarget":
		err := s.Save_InAppEvents(ur)
		if err != nil {
			return err
		}
	case "geo_by_date_report":
		err := s.Save_GeoAggDaily(ur)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *dbSaverServiceImpl) Save_GeoAggDaily(ur *models.UrlBuilderResponse) error {
	// defer ur.Resp.Body.Close()
	bytesbody, err := io.ReadAll(ur.Resp.Body)
	if err != nil {
		return err
	}
	body := string(bytesbody)

	batch, err := s.db.PrepareBatch(s.ctx, `INSERT INTO base_db."af_geo_agg"`)
	if err != nil {
		return err
	}

	err = batch.Append(ur.Builder.Meta, body)
	if err != nil {
		return err
	}

	err = batch.Send()
	if err != nil {
		return err
	}
	return nil
}

func (s *dbSaverServiceImpl) Save_InAppEvents(ur *models.UrlBuilderResponse) error {
	log.Println(time.Now(), "start func")

	// defer ur.Resp.Body.Close()

	reader := csv.NewReader(ur.Resp.Body)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	columns := make([]string, len(records[0]))
	copy(columns, records[0])

	for i, col := range columns {
		if i == 0 {
			col = col[3:]
		}
		str_col := strings.ReplaceAll(col, " ", "_")
		columns[i] = str_col
	}

	colsMap := make(map[string][]string)
	log.Println(time.Now(), "records cnt", len(records))
	log.Println(time.Now(), "start records")

	for _, col := range columns {
		colsMap[col] = []string{}
	}

	for _, record := range records[1:] {
		for i, col := range columns {
			colsMap[col] = append(colsMap[col], record[i])
		}
	}

	log.Println(time.Now(), "done records")

	batch, err := s.db.PrepareBatch(s.ctx, `INSERT INTO base_db."in-app_events"`)
	if err != nil {
		return err
	}

	rows, err := s.db.Query(s.ctx, `SELECT * FROM base_db."in-app_events" LIMIT 0`)
	if err != nil {
		return err
	}

	for i, colType := range rows.ColumnTypes() {

		col, ok := colsMap[colType.Name()]
		if !ok {
			col = make([]string, len(records[1:]))
		}

		err := batch.Column(i).Append(col)
		if err != nil {
			return err
		}
	}

	err = s.db.Exec(s.ctx,
		`ALTER TABLE base_db."in-app_events" DELETE
			WHERE toDate(Event_Time, 'Europe/Moscow') BETWEEN toDate(?, 'UTC') AND toDate(?, 'UTC')
				AND App_ID = ?
				AND Partner = ?`,
		ur.Builder.Meta["from"], ur.Builder.Meta["to"], ur.Builder.Meta["app_id"], ur.Builder.Meta["cabinet"])
	if err != nil {
		return err
	}

	err = batch.Send()
	if err != nil {
		return err
	}

	return nil
}
