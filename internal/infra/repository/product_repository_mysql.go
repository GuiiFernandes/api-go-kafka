package repository

import (
	"database/sql"

	"github.com/guiifernandes/api-go-kafka/internal/entity"
)

type ProductRepositoryMySQL struct {
	DB *sql.DB
}

func NewProductRepositoryMySQL(db *sql.DB) *ProductRepositoryMySQL {
	return &ProductRepositoryMySQL{DB: db}
}

func (r *ProductRepositoryMySQL) Create(product *entity.Product) error {
	_, err := r.DB.Exec("INSERT INTO products (id, name, price) VALUES (?, ?, ?)", product.ID, product.Name, product.Price)

	if err != nil {
		return err
	}
	return nil
}

func (r *ProductRepositoryMySQL) FindAll() ([]*entity.Product, error) {
	rows, err := r.DB.Query("SELECT id, name, price FROM products")

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var products []*entity.Product

	for rows.Next() {
		var product entity.Product
		err := rows.Scan(&product.ID, &product.Name, &product.Price)

		if err != nil {
			return nil, err
		}

		products = append(products, &product)
	}

	return products, nil
}