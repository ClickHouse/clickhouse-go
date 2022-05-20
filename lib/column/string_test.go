package column

import "testing"

type binaryUnmarshaler struct {
	data []byte
}

func (b *binaryUnmarshaler) UnmarshalBinary(data []byte) error {
	b.data = append(b.data[:0], data...)
	return nil
}

func TestString_ScanRow(t *testing.T) {
	t.Run("encoding.BinaryUnmarshaler", func(t *testing.T) {
		col := String([]string{"hello", "world"})

		var dest binaryUnmarshaler
		for i, s := range col {
			err := col.ScanRow(&dest, i)
			if err != nil {
				t.Fatalf("unexpected ScanRow error: %v", err)
			}
			if string(dest.data) != s {
				t.Fatalf("ScanRow resulted in %q instead of %q", dest.data, s)
			}
		}
	})
}
