package kismockedge

import "testing"

func TestKISMockUSValidationUsesUSSymbolsAndStrictDecimals(t *testing.T) {
	command := testKISMockUSCommand("us-validation")
	command.StockCode = "BRK.B"
	command.Quantity = "001"
	command.Price = "1.25"
	if code := ValidateCommand(command); code != "" {
		t.Fatalf("valid US command code=%q", code)
	}
	for _, test := range []struct {
		name string
		edit func(*string, *string)
	}{
		{name: "lowercase ticker", edit: func(stock, price *string) { *stock = "aapl" }},
		{name: "KIS slash form", edit: func(stock, price *string) { *stock = "BRK/B" }},
		{name: "zero price", edit: func(stock, price *string) { *price = "0" }},
		{name: "exponent price", edit: func(stock, price *string) { *price = "1e0" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := testKISMockUSCommand("us-invalid")
			test.edit(&candidate.StockCode, &candidate.Price)
			if code := ValidateCommand(candidate); code != ErrorInvalidCommand {
				t.Fatalf("code=%q, want %q", code, ErrorInvalidCommand)
			}
		})
	}
}

func TestKISMockUSCapsRemainIndependentOfDomesticKRWCap(t *testing.T) {
	command := testKISMockUSCommand("us-cap")
	command.Price = "1000"
	if code := ValidateOrderCaps(command); code != "" {
		t.Fatalf("cap code=%q", code)
	}
	command.Price = "1000.01"
	if code := ValidateOrderCaps(command); code != ErrorOrderLimitExceeded {
		t.Fatalf("cap code=%q, want %q", code, ErrorOrderLimitExceeded)
	}
}
