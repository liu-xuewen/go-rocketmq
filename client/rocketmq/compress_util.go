package rocketmq

import (
	"bytes"
	"compress/zlib"
)

//CompressWithLevel compress byte array with level
func CompressWithLevel(body []byte, level int) (compressBody []byte, err error) {
	var (
		in bytes.Buffer
		w  *zlib.Writer
	)
	w, err = zlib.NewWriterLevel(&in, level)
	if err != nil {
		return
	}
	_, err = w.Write(body)
	w.Close()
	compressBody = in.Bytes()
	return
}
