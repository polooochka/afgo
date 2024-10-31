package parser

import (
	"afgo/internal/models"
	dbsaver "afgo/internal/service/db_saver"
	requestcounter "afgo/internal/service/request_counter"
	urlbuilder "afgo/internal/service/url_builder"
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ParserService interface {
	AddToQueue(ubr models.UrlBuilderRequest)
	SendRequest(httpclient *http.Client, u *models.UrlBuilder)
	AddManyToQueue(ubrArray []models.UrlBuilderRequest)
	Parser()
	SaveToDb()
	GetPreparedRequests(from string, to string) error
}

type parserServiceImpl struct {
	ctx            context.Context
	db             driver.Conn
	urlBuilder     urlbuilder.UrlBuilderService
	dbSaver        dbsaver.DbSaverService
	requestCounter requestcounter.RequestCounterService
	in             chan *models.UrlBuilder
	out            chan *models.UrlBuilderResponse
	done           chan struct{}
}

func NewParserService(
	ctx context.Context,
	db driver.Conn,
	urlBuilder urlbuilder.UrlBuilderService,
	dbSaver dbsaver.DbSaverService,
	requestCounter requestcounter.RequestCounterService) ParserService {

	s := parserServiceImpl{
		ctx:            ctx,
		db:             db,
		urlBuilder:     urlBuilder,
		dbSaver:        dbSaver,
		requestCounter: requestCounter,
		in:             make(chan *models.UrlBuilder, 20),
		out:            make(chan *models.UrlBuilderResponse, 20),
		done:           make(chan struct{}, 10),
	}

	go s.Parser()
	go s.SaveToDb()
	return &s
}

func (s *parserServiceImpl) GetPreparedRequests(from string, to string) error {
	var requests []models.UrlBuilderRequest

	// формирование сырых ивентов
	var inAppEvents []models.UrlBuilderRawRequest
	err := s.db.Select(s.ctx, &inAppEvents, `SELECT * EXCEPT (reattr) from base_db."auto_parsing_af" where method in ('in-app_events', 'in-app-events-retarget')`)
	if err != nil {
		return err
	}
	for _, v := range inAppEvents {
		v.From = from
		v.To = to
		requests = append(requests, &v)
	}

	// формирование агг
	var geoAggDaily []models.UrlBuilderAggRequest
	err = s.db.Select(s.ctx, &geoAggDaily, `SELECT * EXCEPT (event_names) from base_db."auto_parsing_af" where method in ('geo_by_date_report')`)
	if err != nil {
		return err
	}
	for _, v := range geoAggDaily {
		v.From = from
		v.To = to
		requests = append(requests, &v)
	}

	log.Println(&requests)
	s.AddManyToQueue(requests)
	return nil
}

func (s *parserServiceImpl) AddToQueue(ubr models.UrlBuilderRequest) {
	u, err := s.urlBuilder.GenerateReq(ubr)
	if err != nil {
		u.Err = err
	}
	s.in <- &u
}

func (s *parserServiceImpl) ResendToQueue(u models.UrlBuilder, sleep time.Duration) {
	time.Sleep(sleep * time.Second)
	s.in <- &u
}

func (s *parserServiceImpl) AddManyToQueue(ubrArray []models.UrlBuilderRequest) {
	for _, ubr := range ubrArray {
		go s.AddToQueue(ubr)
	}
}

func (s *parserServiceImpl) SendRequest(httpclient *http.Client, u *models.UrlBuilder) {
	var ubresp models.UrlBuilderResponse
	log.Println("start req")

	u.AddMeta("request_time", time.Now().UTC().Format("2006-01-02 15:04:05"))
	resp, err := httpclient.Do(u.Req)
	if err == nil {
		c := models.RequestCounter{
			Path:    u.Path,
			Tracker: u.Meta["tracker"],
			Cabinet: u.Meta["cabinet"],
		}
		count, err := s.requestCounter.Add(c)
		if err != nil {
			log.Println(err)
		}
		u.AddMeta("count", strconv.FormatUint(count, 10))
	}
	u.AddMeta("response_time", time.Now().UTC().Format("2006-01-02 15:04:05"))

	ubresp.Builder = u
	ubresp.Resp = resp
	ubresp.Err = err
	log.Println(resp.StatusCode)

	s.out <- &ubresp
}

func (s *parserServiceImpl) Parser() {
	httpclient := &http.Client{}

	for {
		u := <-s.in

		go func(u *models.UrlBuilder) {
			s.done <- struct{}{}
			s.SendRequest(httpclient, u)
			<-s.done
		}(u)
	}
}

func (s *parserServiceImpl) SaveToDb() {
	for {
		ur := <-s.out
		go func(ur *models.UrlBuilderResponse) {

			log.Println(ur.Builder.Meta)
			log.Println("")
			log.Println("")
			log.Println("")
			if ur.Resp.StatusCode == http.StatusOK {
				err := s.dbSaver.Save(ur)
				log.Println(err)
			} else if ur.Resp.StatusCode == http.StatusForbidden {
				s.ResendToQueue(*ur.Builder, 10)

			} else if ur.Resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
				s.ResendToQueue(*ur.Builder, 60)
			} else {
				log.Println("err in req")
			}

			ur.Resp.Body.Close()
		}(ur)
	}
}
