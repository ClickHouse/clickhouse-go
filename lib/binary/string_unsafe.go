//go:build amd64 || arm64
// +build amd64 arm64

package binary

func str2Bytes(str string) []byte {
	return unsafeStr2Bytes(str)
}
