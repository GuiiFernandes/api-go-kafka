package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi/v5"
	"github.com/guiifernandes/api-go-kafka/internal/infra/akafka"
	"github.com/guiifernandes/api-go-kafka/internal/infra/repository"
	"github.com/guiifernandes/api-go-kafka/internal/infra/web"
	"github.com/guiifernandes/api-go-kafka/internal/usecase"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(host.docker.internal:3306)/products")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	repository := repository.NewProductRepositoryMySQL(db)
	createProductUsecase := usecase.NewCreateProductUseCase(repository)
	listProductsUseCase := usecase.NewListProductUseCase(repository)

	ProductHandlers := web.NewProductHandlers(createProductUsecase, listProductsUseCase)

	r := chi.NewRouter()
	r.Post("/products", ProductHandlers.CreateProductHandler)
	r.Get("/products", ProductHandlers.ListProductsHandler)

	go http.ListenAndServe(":8000", r)

	msgChan := make(chan *kafka.Message)
	go akafka.Consume([]string{"product"}, "host.docker.internal:9094", msgChan)

	for msg := range msgChan {
		dto := usecase.CreateProductInputDto{}
		err := json.Unmarshal(msg.Value, &dto)
		if err != nil {
			fmt.Println(err)
		}
		_, err = createProductUsecase.Execute(dto)
	}
}
