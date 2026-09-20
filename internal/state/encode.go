package state

import "encoding/hex"

// encodeSpillName 把复合键稳定编码为安全文件名。
func encodeSpillName(k Key) string {
	return hex.EncodeToString([]byte(k.ID()))
}
