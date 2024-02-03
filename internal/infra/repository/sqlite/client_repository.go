package repository

import (
	"database/sql"

	"github.com/Sup3r-Us3r/race-condition/internal/entity"
)

func FindClientById(db *sql.DB, userId int) (entity.Client, error) {
	var id int
	var name string

	err := db.QueryRow("SELECT * FROM clients WHERE id = ?", userId).Scan(&id, &name)

	if err != nil {
		if err == sql.ErrNoRows {
			return entity.Client{}, nil
		}

		return entity.Client{}, err
	}

	client := entity.Client{
		ID:   id,
		Name: name,
	}

	return client, nil
}

func AddClient(db *sql.DB, client entity.Client) error {
	stmt, err := db.Prepare("INSERT INTO clients (id, name) VALUES(?,?)")
	if err != nil {
		panic(err.Error())
	}

	_, err = stmt.Exec(client.ID, client.Name)
	if err != nil {
		return err
	}

	return nil
}
