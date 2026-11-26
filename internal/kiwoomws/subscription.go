package kiwoomws

import (
	"encoding/json"
	"errors"
)

type registration struct {
	TRNM    string             `json:"trnm"`
	GroupNo string             `json:"grp_no"`
	Refresh string             `json:"refresh"`
	Data    []registrationData `json:"data"`
}

type registrationData struct {
	Item []string `json:"item"`
	Type []string `json:"type"`
}

var quoteTypes = map[Account][]string{
	AccountKR: {"0B", "0D", "0H", "1h"},
	AccountUS: {"FE", "FT"},
}

var accountTypes = map[Account][]string{
	AccountKR: {"00", "04"},
	AccountUS: {"F4", "F5"},
}

// BuildSubscription creates the documented initial REG message. Account
// events use item [""] because they are scoped to the access-token account.
func BuildSubscription(account Account, symbols []string) ([]byte, error) {
	if account != AccountKR && account != AccountUS {
		return nil, errors.New("Kiwoom WS 계정 유형이 잘못되었습니다")
	}
	if err := ValidateSymbols(symbols); err != nil {
		return nil, err
	}
	data := make([]registrationData, 0, 2)
	if len(symbols) > 0 {
		data = append(data, registrationData{Item: append([]string(nil), symbols...), Type: append([]string(nil), quoteTypes[account]...)})
	}
	data = append(data, registrationData{Item: []string{""}, Type: append([]string(nil), accountTypes[account]...)})
	return json.Marshal(registration{TRNM: "REG", GroupNo: "1", Refresh: "0", Data: data})
}

// BuildRemoval creates a documented REMOVE message for the same account data.
func BuildRemoval(account Account, symbols []string) ([]byte, error) {
	payload, err := BuildSubscription(account, symbols)
	if err != nil {
		return nil, err
	}
	var message registration
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, err
	}
	message.TRNM = "REMOVE"
	message.Refresh = "1"
	return json.Marshal(message)
}
