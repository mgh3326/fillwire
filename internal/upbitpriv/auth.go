package upbitpriv

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
)

func authToken(accessKey, secretKey, nonce string) string {
	claims, _ := json.Marshal(map[string]string{"access_key": accessKey, "nonce": nonce})
	encode := base64.RawURLEncoding.EncodeToString
	unsigned := encode([]byte(`{"alg":"HS512","typ":"JWT"}`)) + "." + encode(claims)
	mac := hmac.New(sha512.New, []byte(secretKey))
	_, _ = mac.Write([]byte(unsigned))
	return unsigned + "." + encode(mac.Sum(nil))
}
