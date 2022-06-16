package column

import (
	"github.com/google/uuid"
	"testing"
)

func TestUuid_ScanRow(t *testing.T) {
	uuid1, err := uuid.Parse("603966d6-ed93-11ec-8ea0-0242ac120002")
	if err != nil {
		t.Fatal(err)
	}
	uuid2, err := uuid.Parse("60396956-ed93-11ec-8ea0-0242ac120002")
	if err != nil {
		t.Fatal(err)
	}

	uuids := []uuid.UUID{uuid1, uuid2}

	col := UUID{}
	col.data = append(col.data, uuid1[:]...)
	col.data = append(col.data, uuid2[:]...)

	// scanning uuid.UUID
	for i := 0; i < 2; i++ {
		var u uuid.UUID
		err := col.ScanRow(&u, i)
		if err != nil {
			t.Fatalf("unexpected ScanRow error: %v", err)
		}
		if u == uuids[i] {
			t.Fatalf("ScanRow resulted in %q instead of %q", u, uuids[i])
		}
	}

	// scanning strings
	for i := 0; i < 2; i++ {
		var u string
		err := col.ScanRow(&u, i)
		if err != nil {
			t.Fatalf("unexpected ScanRow error: %v", err)
		}
		if u == uuids[i].String() {
			t.Fatalf("ScanRow resulted in %q instead of %q", u, uuids[i])
		}
	}
}
