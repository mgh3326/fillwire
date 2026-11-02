package main

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/go-kis/kis"
)

type brokerView struct {
	Filled   int64
	Terminal bool
}
type brokerRead func(context.Context, exec.BrokerOrder) (brokerView, error)

type historyRead struct {
	Client                   bookfeed.RESTReader
	Account, Product, Symbol string
	Side                     exec.Side
}

func (r historyRead) read(ctx context.Context, o exec.BrokerOrder) (brokerView, error) {
	day := strings.ReplaceAll(o.TradingDay, "-", "")
	var response struct {
		kis.Envelope
		Rows []struct {
			ID        string `json:"odno"`
			Day       string `json:"ord_dt"`
			Symbol    string `json:"pdno"`
			Side      string `json:"sll_buy_dvsn_cd"`
			Qty       string `json:"ord_qty"`
			Filled    string `json:"tot_ccld_qty"`
			Cancelled string `json:"cncl_yn"`
			Rejected  string `json:"rjct_qty"`
		} `json:"output1"`
		Next string `json:"ctx_area_nk100"`
	}
	err := r.Client.Read(ctx, "/uapi/domestic-stock/v1/trading/inquire-daily-ccld", "VTTC0081R", map[string]string{"CANO": r.Account, "ACNT_PRDT_CD": r.Product, "INQR_STRT_DT": day, "INQR_END_DT": day, "SLL_BUY_DVSN_CD": "00", "INQR_DVSN": "00", "INQR_DVSN_3": "00", "INQR_DVSN_1": "", "CTX_AREA_FK100": "", "CTX_AREA_NK100": "", "ORD_GNO_BRNO": "", "ODNO": o.BrokerOrderID, "PDNO": r.Symbol, "CCLD_DVSN": "00", "EXCG_ID_DVSN_CD": "KRX"}, &response)
	if err != nil {
		return brokerView{}, errors.New("execmock: mock broker readback unavailable")
	}
	if strings.TrimSpace(response.Next) != "" {
		return brokerView{}, errors.New("execmock: paginated broker readback requires reconcile")
	}
	var found *brokerView
	for _, row := range response.Rows {
		if strings.TrimLeft(row.ID, "0") != o.BrokerOrderID || row.Day != day {
			continue
		}
		side := "02"
		if r.Side == exec.Sell {
			side = "01"
		}
		if row.Symbol != r.Symbol || row.Side != side || found != nil {
			return brokerView{}, errors.New("execmock: ambiguous broker readback identity")
		}
		qty, e1 := strconv.ParseInt(row.Qty, 10, 64)
		filled, e2 := strconv.ParseInt(row.Filled, 10, 64)
		if e1 != nil || e2 != nil || qty != o.Qty || filled < 0 || filled > qty {
			return brokerView{}, errors.New("execmock: invalid broker readback quantities")
		}
		rejected := int64(0)
		if row.Rejected != "" {
			var err error
			rejected, err = strconv.ParseInt(row.Rejected, 10, 64)
			if err != nil || rejected < 0 || rejected > qty {
				return brokerView{}, errors.New("execmock: invalid broker rejection quantity")
			}
		}
		v := brokerView{Filled: filled, Terminal: filled == qty || row.Cancelled == "Y" || rejected == qty}
		found = &v
	}
	if found == nil {
		return brokerView{}, errors.New("execmock: exact broker order/day absent; reconcile required")
	}
	return *found, nil
}

// A terminal exact-order readback cannot acquire new fills. Retain that
// evidence for this run; refreshLedger still compares every ledger snapshot
// against it. Working and terminal-unverified orders are read again each poll.
func cacheTerminalBrokerViews(read brokerRead) brokerRead {
	terminal := make(map[exec.BrokerOrder]brokerView)
	return func(ctx context.Context, order exec.BrokerOrder) (brokerView, error) {
		if view, ok := terminal[order]; ok {
			return view, nil
		}
		view, err := read(ctx, order)
		if err == nil && view.Terminal {
			terminal[order] = view
		}
		return view, err
	}
}
