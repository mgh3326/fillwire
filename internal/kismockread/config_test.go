package kismockread

import "testing"

func TestSplitAccountNoAcceptsCANOAndExplicitProductCode(t *testing.T) {
	tests := []struct {
		name        string
		accountNo   string
		wantCANO    string
		wantProduct string
	}{
		{
			name:        "eight digit CANO defaults product code",
			accountNo:   "12345678",
			wantCANO:    "12345678",
			wantProduct: "01",
		},
		{
			name:        "ten digit account number",
			accountNo:   "1234567802",
			wantCANO:    "12345678",
			wantProduct: "02",
		},
		{
			name:        "eight two hyphen form",
			accountNo:   "12345678-01",
			wantCANO:    "12345678",
			wantProduct: "01",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cano, productCode, err := splitAccountNo(test.accountNo)
			if err != nil {
				t.Fatalf("splitAccountNo(%q): %v", test.accountNo, err)
			}
			if cano != test.wantCANO || productCode != test.wantProduct {
				t.Fatalf("splitAccountNo(%q) = (%q, %q), want (%q, %q)", test.accountNo, cano, productCode, test.wantCANO, test.wantProduct)
			}
		})
	}
}

func TestSplitAccountNoRejectsInvalidLengthsAndShapes(t *testing.T) {
	for _, accountNo := range []string{
		"1234567",
		"123456789",
		"12345678901",
		"1234567-01",
		"12345678-0",
		"1234-567801",
	} {
		t.Run(accountNo, func(t *testing.T) {
			_, _, err := splitAccountNo(accountNo)
			if err == nil || err.Code != CodeInvalidInput {
				t.Fatalf("splitAccountNo(%q) error = %v, want %s", accountNo, err, CodeInvalidInput)
			}
		})
	}
}

func TestConfigFromEnvAcceptsEightDigitCANO(t *testing.T) {
	config, err := ConfigFromEnv(func(name string) string {
		return map[string]string{
			"KIS_MOCK_APP_KEY":    "key",
			"KIS_MOCK_APP_SECRET": "secret",
			"KIS_MOCK_ACCOUNT_NO": "12345678",
			"REDIS_URL":           "redis://cache.example:6379/0",
		}[name]
	})
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if config.AccountNo != "12345678" {
		t.Fatalf("AccountNo = %q, want eight-digit CANO", config.AccountNo)
	}
}
