package utfstring

import (
	"bytes"
	"strings"

	"golang.org/x/text/encoding/unicode"
)

// All supported BOMs (Byte Order Marks)
var (
	bomUTF8              = []byte{0xef, 0xbb, 0xbf}
	bomUTF16BigEndian    = []byte{0xfe, 0xff}
	bomUTF16LittleEndian = []byte{0xff, 0xfe}
)

// Decode removes a byte order mark and converts the bytes to UTF-8.
func Decode(data []byte) ([]byte, error) {
	if bytes.HasPrefix(data, bomUTF8) {
		return data[len(bomUTF8):], nil
	}

	if !bytes.HasPrefix(data, bomUTF16BigEndian) && !bytes.HasPrefix(data, bomUTF16LittleEndian) {
		// no encoding specified, let's assume UTF-8
		return data, nil
	}

	// UseBom means automatic endianness selection
	e := unicode.UTF16(unicode.BigEndian, unicode.UseBOM)
	return e.NewDecoder().Bytes(data)
}

func GetCredentialFromBuffer(buf []byte) (string, error) {
	s, err := Decode(buf)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(s)), nil
}
