//go:build !wasm

package wasman

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
)

var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(io.Discard)
	},
}

var gzipReaderPool = sync.Pool{}

func compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, _ := gzipWriterPool.Get().(*gzip.Writer)
	if zw == nil {
		zw = gzip.NewWriter(&buf)
	} else {
		zw.Reset(&buf)
	}
	_, err := zw.Write(data)
	if err != nil {
		return nil, err
	}
	err = zw.Close()
	if err != nil {
		return nil, err
	}
	gzipWriterPool.Put(zw)
	return buf.Bytes(), nil
}

func decompressData(data []byte) ([]byte, error) {
	zr, _ := gzipReaderPool.Get().(*gzip.Reader)
	if zr == nil {
		var err error
		zr, err = gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
	} else {
		err := zr.Reset(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		_ = zr.Close()
		gzipReaderPool.Put(zr)
	}()
	return io.ReadAll(zr)
}

func isGzipped(data []byte) bool {
	return len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

