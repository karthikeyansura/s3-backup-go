package backup

import (
	"io"
)

// SectorWriter wraps an io.Writer to enforce 512-byte sector-aligned padding
// and track the absolute byte offset of the stream.
type SectorWriter struct {
	w            io.Writer
	totalWritten int64
	noio         bool
}

// NewSectorWriter initializes a SectorWriter.
func NewSectorWriter(w io.Writer, noio bool) *SectorWriter {
	return &SectorWriter{
		w:    w,
		noio: noio,
	}
}

// Write pushes data to the underlying stream and increments the offset counter
// strictly by the number of bytes successfully written.
func (sw *SectorWriter) Write(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if sw.noio {
		sw.totalWritten += int64(len(data))
		return nil
	}

	n, err := sw.w.Write(data)
	sw.totalWritten += int64(n)
	return err
}

// WritePadded writes the provided data and appends zero-byte padding
// to align the stream to the next 512-byte sector boundary.
func (sw *SectorWriter) WritePadded(data []byte) (int64, error) {
	nbytes := int64(len(data))
	if err := sw.Write(data); err != nil {
		return 0, err
	}

	padLen := roundUp(nbytes, 512) - nbytes
	if padLen > 0 {
		pad := make([]byte, padLen)
		if err := sw.Write(pad); err != nil {
			return 0, err
		}
	}

	return roundUp(nbytes, 512) / 512, nil
}

// Offset returns the current stream position calculated in 512-byte sectors.
func (sw *SectorWriter) Offset() int64 {
	return sw.totalWritten / 512
}

// TotalWritten returns the absolute stream position in bytes.
func (sw *SectorWriter) TotalWritten() int64 {
	return sw.totalWritten
}

func roundUp(a, b int64) int64 {
	return b * ((a + b - 1) / b)
}
