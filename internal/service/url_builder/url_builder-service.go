package urlbuilder

import (
	"afgo/internal/models"
	"context"
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type UrlBuilderService interface {
	GenerateReq(ubr models.UrlBuilderRequest) (models.UrlBuilder, error)
}

type urlbuilderServiceImpl struct {
	ctx context.Context
	db  driver.Conn
}

func NewUrlBuilderService(ctx context.Context, db driver.Conn) UrlBuilderService {
	return &urlbuilderServiceImpl{
		ctx: ctx,
		db:  db,
	}
}

func (s *urlbuilderServiceImpl) GenerateReq(ubr models.UrlBuilderRequest) (models.UrlBuilder, error) {
	u := models.NewUrlBuilder()

	err := ubr.FromRequest(&u)
	if err != nil {
		return u, err
	}

	var token string
	err = s.db.QueryRow(s.ctx, "SELECT token FROM base_db.tokens WHERE tracker = ? AND cabinet = ?", u.Meta["tracker"], u.Meta["cabinet"]).Scan(&token)
	switch {
	case err != nil:
		return u, err
	case token == "":
		return u, errors.New("token dont exist")
	}

	u.SetHeaders(token)

	err = u.CreateReq()
	if err != nil {
		return u, err
	}
	return u, nil
}
