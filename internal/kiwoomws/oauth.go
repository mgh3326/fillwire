package kiwoomws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

// IssueMockToken exchanges mock app credentials through Kiwoom's au10001
// client-credentials endpoint. Error messages never include request/response
// bodies, which may contain credentials or a bearer token.
func IssueMockToken(ctx context.Context, appKey, appSecret string) (string, error) {
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return issueMockToken(ctx, appKey, appSecret, MockRESTEndpoint+"/oauth2/token", client)
}

func issueMockToken(ctx context.Context, appKey, appSecret, endpoint string, client *http.Client) (string, error) {
	if strings.TrimSpace(appKey) == "" || strings.TrimSpace(appSecret) == "" {
		return "", errors.New("Kiwoom 모의 OAuth 앱 자격 증명이 비어 있습니다")
	}
	body, err := json.Marshal(map[string]string{
		"grant_type": "client_credentials",
		"appkey":     appKey,
		"secretkey":  appSecret,
	})
	if err != nil {
		return "", errors.New("Kiwoom 모의 OAuth 요청을 만들 수 없습니다")
	}
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", errors.New("Kiwoom 모의 OAuth 요청을 만들 수 없습니다")
	}
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	response, err := client.Do(req)
	if err != nil {
		return "", errors.New("Kiwoom 모의 OAuth 토큰 발급에 실패했습니다")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return "", ErrPermanentAuth
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", errors.New("Kiwoom 모의 OAuth 토큰 발급이 거부되었습니다")
	}
	var result struct {
		Token      string `json:"token"`
		ReturnCode *int   `json:"return_code"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return "", errors.New("Kiwoom 모의 OAuth 응답이 올바르지 않습니다")
	}
	if strings.TrimSpace(result.Token) == "" || (result.ReturnCode != nil && *result.ReturnCode != 0) {
		return "", errors.New("Kiwoom 모의 OAuth 응답이 올바르지 않습니다")
	}
	return result.Token, nil
}
