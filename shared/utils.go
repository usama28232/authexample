package shared

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

func HashPassword(password string) (string, error) {
	hashedBytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashedBytes), nil
}

func ComparePassword(hashedPass, input string) error {
	return bcrypt.CompareHashAndPassword([]byte(hashedPass), []byte(input))
}

func DecodeBase64(encoded string) string {
	decodedBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return ""
	}
	return string(decodedBytes)
}

func StringSplit(input, delim string) []string {
	return strings.Split(input, delim)
}

func EncodeResponse(data interface{}, w io.Writer) error {
	enc := json.NewEncoder(w)
	err := enc.Encode(data)
	if err == nil {
		return nil
	} else {
		return err
	}
}

func CollectionContains(data []string, target string) bool {
	for _, value := range data {
		if value == target {
			return true
		}
	}
	return false
}
