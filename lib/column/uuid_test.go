package column

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func getTestUuids() (uuids []uuid.UUID, err error) {
	uuid1, err := uuid.Parse("603966d6-ed93-11ec-8ea0-0242ac120002")
	if err != nil {
		return
	}
	uuid2, err := uuid.Parse("60396956-ed93-11ec-8ea0-0242ac120002")
	if err != nil {
		return
	}

	uuids = []uuid.UUID{uuid1, uuid2}
	return
}

func TestUuid_ScanRow(t *testing.T) {
	uuids, err := getTestUuids()
	if err != nil {
		t.Fatal(err)
	}

	col := UUID{}
	_, err = col.Append(uuids)
	if err != nil {
		t.Fatal(err)
	}

	// scanning uuid.UUID
	for i := range uuids {
		var u uuid.UUID
		err := col.ScanRow(&u, i)
		if err != nil {
			require.Error(t, err, "unexpected ScanRow error")
		}
		if u != uuids[i] {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", u, uuids[i])
		}
	}

	// scanning strings
	for i := range uuids {
		var u string
		err := col.ScanRow(&u, i)
		if err != nil {
			require.Error(t, err, "unexpected ScanRow error")
		}
		if u != uuids[i].String() {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", u, uuids[i])
		}
	}
}
