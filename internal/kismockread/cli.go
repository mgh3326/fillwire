package kismockread

import (
	"context"
	"flag"
	"io"
)

// RunCLI is the binary entry point. It emits only closed-vocabulary failures;
// no underlying error text is rendered.
func RunCLI(
	ctx context.Context,
	args []string,
	lookup func(string) string,
	stdout io.Writer,
	stderr io.Writer,
) int {
	request, asJSON, parseErr := parseCLI(args)
	if parseErr != nil {
		emitFailure(stdout, stderr, asJSON, request.Operation, parseErr.Code)
		return 2
	}
	config, configErr := ConfigFromEnv(lookup)
	if configErr != nil {
		emitFailure(stdout, stderr, asJSON, request.Operation, configErr.Code)
		return 1
	}
	getter, getterErr := NewRedisGETClient(config.RedisURL)
	if getterErr != nil {
		emitFailure(stdout, stderr, asJSON, request.Operation, getterErr.Code)
		return 1
	}
	result, executeErr := (Executor{TokenGetter: getter}).Execute(ctx, config, request)
	if executeErr != nil {
		emitFailure(stdout, stderr, asJSON, request.Operation, executeErr.Code)
		return 1
	}
	if asJSON {
		writeJSONOutput(stdout, successOutput(result))
	} else {
		writeHumanSuccess(stdout, result)
	}
	return 0
}

func parseCLI(args []string) (ReadRequest, bool, *SafeError) {
	request := ReadRequest{Operation: OperationUnknown}
	asJSON := hasJSONFlag(args)
	if len(args) == 0 {
		return request, asJSON, safeError(CodeInvalidInput)
	}
	operation, operationErr := ParseOperation(args[0])
	if operationErr != nil {
		return request, asJSON, operationErr
	}
	request.Operation = operation
	flags := flag.NewFlagSet(string(operation), flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	jsonFlag := flags.Bool("json", false, "")
	fromDate := flags.String("from", "", "")
	toDate := flags.String("to", "", "")
	stockCode := flags.String("stock-code", "", "")
	side := flags.String("side", "", "")
	orderNo := flags.String("order-no", "", "")
	exchange := flags.String("exchange", "", "")
	currency := flags.String("currency", "", "")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
		return request, asJSON, safeError(CodeInvalidInput)
	}
	request.FromDate = *fromDate
	request.ToDate = *toDate
	request.StockCode = *stockCode
	request.Side = *side
	request.OrderNo = *orderNo
	request.Exchange = *exchange
	request.Currency = *currency
	return request, asJSON || *jsonFlag, nil
}

func hasJSONFlag(args []string) bool {
	for _, argument := range args {
		if argument == "--json" {
			return true
		}
	}
	return false
}

func emitFailure(stdout io.Writer, stderr io.Writer, asJSON bool, operation Operation, code ErrorCode) {
	if asJSON {
		writeJSONOutput(stdout, failureOutput(operation, code))
		return
	}
	writeHumanFailure(stderr, code)
}
