package kismockedge

import (
	"math/big"
	"testing"
)

func TestKRXTicksPortAutoTraderAdjustRules(t *testing.T) {
	tests := []struct {
		price     string
		side      string
		wantTick  int64
		wantPrice string
	}{
		{price: "1999", side: "buy", wantTick: 1, wantPrice: "1999"},
		{price: "2000", side: "buy", wantTick: 5, wantPrice: "2000"},
		{price: "15723", side: "buy", wantTick: 10, wantPrice: "15720"},
		{price: "15723", side: "sell", wantTick: 10, wantPrice: "15730"},
		{price: "327272", side: "buy", wantTick: 500, wantPrice: "327000"},
		{price: "327272", side: "sell", wantTick: 500, wantPrice: "327500"},
		{price: "1098000", side: "buy", wantTick: 1000, wantPrice: "1098000"},
	}
	for _, test := range tests {
		t.Run(test.price+"-"+test.side, func(t *testing.T) {
			price, _ := new(big.Int).SetString(test.price, 10)
			if tick := GetTickSizeKR(price); tick.Int64() != test.wantTick {
				t.Fatalf("tick=%s want=%d", tick, test.wantTick)
			}
			adjusted, valid := AdjustTickSizeKR(price, test.side)
			if !valid || adjusted.String() != test.wantPrice {
				t.Fatalf("adjusted=%v valid=%v want=%s", adjusted, valid, test.wantPrice)
			}
		})
	}
}

func TestValidationDoesNotAllowNumericCoercionOrTickRepricing(t *testing.T) {
	command := testCommand("validation")
	command.Price = "70001"
	if code := ValidateCommand(command); code != ErrorTickMismatch {
		t.Fatalf("mismatched tick code=%q", code)
	}
	command.Price = "70000.0"
	if code := ValidateCommand(command); code != ErrorInvalidCommand {
		t.Fatalf("decimal price code=%q", code)
	}
	command.Price = "70000"
	command.Quantity = "0"
	if code := ValidateCommand(command); code != ErrorInvalidCommand {
		t.Fatalf("zero quantity code=%q", code)
	}
}
