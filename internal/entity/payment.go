package entity

import "time"

type Payment struct {
	ID            int       `json:"id"`
	ClientID      int       `json:"clientId"`
	Value         int       `json:"value"`
	OperationDate time.Time `json:"operationDate"`
}
