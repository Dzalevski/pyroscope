package dimension

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/petethepig/pyroscope/pkg/util/varint"
)

func (s *Dimension) Serialize(w io.Writer) error {
	for _, k := range s.keys {
		varint.Write(w, uint64(len(k)))
		w.Write([]byte(k))
	}
	return nil
}

func Deserialize(r io.Reader) (*Dimension, error) {
	s := New()

	br := bufio.NewReader(r) // TODO if it's already a bytereader skip

	for {
		keyLen, err := binary.ReadUvarint(br)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		keyBuf := make([]byte, keyLen) // TODO: there are better ways to do this?
		_, err = io.ReadAtLeast(br, keyBuf, int(keyLen))
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (t *Dimension) Bytes() []byte {
	b := bytes.Buffer{}
	t.Serialize(&b)
	return b.Bytes()
}

func FromBytes(p []byte) *Dimension {
	t, _ := Deserialize(bytes.NewReader(p))
	return t
}