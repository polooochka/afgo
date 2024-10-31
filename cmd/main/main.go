package main

import (
	"afgo/internal/configs"
	"log"
	"time"

	// "afgo/internal/models"
	"afgo/internal/repo"
	dbsaver "afgo/internal/service/db_saver"
	requestcounter "afgo/internal/service/request_counter"

	parser "afgo/internal/service/parser"
	urlbuilder "afgo/internal/service/url_builder"
	"context"
	"fmt"
)

func main() {
	ctx := context.Background()

	cfg := configs.MustLoadConfig()
	fmt.Println(cfg)

	ChCLient := repo.MustChConn(cfg)

	urlBuilerService := urlbuilder.NewUrlBuilderService(ctx, ChCLient)
	dbSaverService := dbsaver.NewDbSaverService(ctx, ChCLient)
	requestCounterService := requestcounter.NewRequestCounterService(ctx, ChCLient)

	parserService := parser.NewParserService(ctx, ChCLient, urlBuilerService, dbSaverService, requestCounterService)

	err := parserService.GetPreparedRequests("2024-10-01", "2024-10-31")
	if err != nil {
		log.Fatalln(err)
	}

	time.Sleep(3 * time.Hour)
}
