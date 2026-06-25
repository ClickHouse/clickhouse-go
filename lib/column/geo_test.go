package column

import (
	"fmt"
	"testing"

	"github.com/paulmach/orb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// geoScanner is a test implementation of sql.Scanner that records the value it receives.
type geoScanner struct {
	val any
	err error
}

func (s *geoScanner) Scan(src any) error {
	s.val = src
	return s.err
}

// geoScannerErr is a sql.Scanner that returns an error from Scan.
type geoScannerErr struct {
	scanErr error
}

func (s *geoScannerErr) Scan(src any) error {
	return s.scanErr
}

func newGeoCol(t *testing.T, colType string) Interface {
	t.Helper()
	col, err := Type(colType).Column("test", &ServerContext{})
	require.NoError(t, err)
	return col
}

func TestPoint_ScanRow_SQLScanner(t *testing.T) {
	col := newGeoCol(t, "Point")

	pt := orb.Point{1.5, 2.5}
	require.NoError(t, col.AppendRow(pt))

	var scanner geoScanner
	require.NoError(t, col.ScanRow(&scanner, 0))
	assert.Equal(t, pt, scanner.val)
}

func TestPoint_ScanRow_NativeScan(t *testing.T) {
	col := newGeoCol(t, "Point")

	pt := orb.Point{3.0, 4.0}
	require.NoError(t, col.AppendRow(pt))

	var got orb.Point
	require.NoError(t, col.ScanRow(&got, 0))
	assert.Equal(t, pt, got)
}

func TestPoint_ScanRow_NativeScanDoublePtr(t *testing.T) {
	col := newGeoCol(t, "Point")

	pt := orb.Point{5.0, 6.0}
	require.NoError(t, col.AppendRow(pt))

	var got *orb.Point
	require.NoError(t, col.ScanRow(&got, 0))
	require.NotNil(t, got)
	assert.Equal(t, pt, *got)
}

func TestPoint_ScanRow_UnknownType_ReturnsError(t *testing.T) {
	col := newGeoCol(t, "Point")

	require.NoError(t, col.AppendRow(orb.Point{1, 2}))

	var s string
	err := col.ScanRow(&s, 0)
	assert.Error(t, err)
	assert.IsType(t, &ColumnConverterError{}, err)
}

func TestPoint_ScanRow_SQLScanner_PropagatesError(t *testing.T) {
	col := newGeoCol(t, "Point")

	require.NoError(t, col.AppendRow(orb.Point{1, 2}))

	scanner := &geoScannerErr{scanErr: fmt.Errorf("scan failed")}
	err := col.ScanRow(scanner, 0)
	assert.EqualError(t, err, "scan failed")
}

func TestLineString_ScanRow_SQLScanner(t *testing.T) {
	col := newGeoCol(t, "LineString")

	ls := orb.LineString{{1, 2}, {3, 4}}
	require.NoError(t, col.AppendRow(ls))

	var scanner geoScanner
	require.NoError(t, col.ScanRow(&scanner, 0))
	assert.Equal(t, ls, scanner.val)
}

func TestLineString_ScanRow_NativeScan(t *testing.T) {
	col := newGeoCol(t, "LineString")

	ls := orb.LineString{{0, 0}, {1, 1}}
	require.NoError(t, col.AppendRow(ls))

	var got orb.LineString
	require.NoError(t, col.ScanRow(&got, 0))
	assert.Equal(t, ls, got)
}

func TestLineString_ScanRow_NativeScanDoublePtr(t *testing.T) {
	col := newGeoCol(t, "LineString")

	ls := orb.LineString{{2, 3}, {4, 5}}
	require.NoError(t, col.AppendRow(ls))

	var got *orb.LineString
	require.NoError(t, col.ScanRow(&got, 0))
	require.NotNil(t, got)
	assert.Equal(t, ls, *got)
}

func TestLineString_ScanRow_UnknownType_ReturnsError(t *testing.T) {
	col := newGeoCol(t, "LineString")

	require.NoError(t, col.AppendRow(orb.LineString{{1, 2}}))

	var s string
	err := col.ScanRow(&s, 0)
	assert.Error(t, err)
	assert.IsType(t, &ColumnConverterError{}, err)
}

func TestLineString_ScanRow_SQLScanner_PropagatesError(t *testing.T) {
	col := newGeoCol(t, "LineString")

	require.NoError(t, col.AppendRow(orb.LineString{{1, 2}}))

	scanner := &geoScannerErr{scanErr: fmt.Errorf("scan failed")}
	err := col.ScanRow(scanner, 0)
	assert.EqualError(t, err, "scan failed")
}

func TestRing_ScanRow_SQLScanner(t *testing.T) {
	col := newGeoCol(t, "Ring")

	ring := orb.Ring{{0, 0}, {1, 0}, {1, 1}, {0, 0}}
	require.NoError(t, col.AppendRow(ring))

	var scanner geoScanner
	require.NoError(t, col.ScanRow(&scanner, 0))
	assert.Equal(t, ring, scanner.val)
}

func TestRing_ScanRow_NativeScan(t *testing.T) {
	col := newGeoCol(t, "Ring")

	ring := orb.Ring{{0, 0}, {2, 0}, {2, 2}, {0, 0}}
	require.NoError(t, col.AppendRow(ring))

	var got orb.Ring
	require.NoError(t, col.ScanRow(&got, 0))
	assert.Equal(t, ring, got)
}

func TestRing_ScanRow_NativeScanDoublePtr(t *testing.T) {
	col := newGeoCol(t, "Ring")

	ring := orb.Ring{{0, 0}, {3, 0}, {3, 3}, {0, 0}}
	require.NoError(t, col.AppendRow(ring))

	var got *orb.Ring
	require.NoError(t, col.ScanRow(&got, 0))
	require.NotNil(t, got)
	assert.Equal(t, ring, *got)
}

func TestRing_ScanRow_UnknownType_ReturnsError(t *testing.T) {
	col := newGeoCol(t, "Ring")

	require.NoError(t, col.AppendRow(orb.Ring{{0, 0}, {1, 0}, {0, 0}}))

	var s string
	err := col.ScanRow(&s, 0)
	assert.Error(t, err)
	assert.IsType(t, &ColumnConverterError{}, err)
}

func TestRing_ScanRow_SQLScanner_PropagatesError(t *testing.T) {
	col := newGeoCol(t, "Ring")

	require.NoError(t, col.AppendRow(orb.Ring{{0, 0}, {1, 0}, {0, 0}}))

	scanner := &geoScannerErr{scanErr: fmt.Errorf("scan failed")}
	err := col.ScanRow(scanner, 0)
	assert.EqualError(t, err, "scan failed")
}

func TestPolygon_ScanRow_SQLScanner(t *testing.T) {
	col := newGeoCol(t, "Polygon")

	poly := orb.Polygon{{{0, 0}, {1, 0}, {1, 1}, {0, 0}}}
	require.NoError(t, col.AppendRow(poly))

	var scanner geoScanner
	require.NoError(t, col.ScanRow(&scanner, 0))
	assert.Equal(t, poly, scanner.val)
}

func TestPolygon_ScanRow_NativeScan(t *testing.T) {
	col := newGeoCol(t, "Polygon")

	poly := orb.Polygon{{{0, 0}, {2, 0}, {2, 2}, {0, 0}}}
	require.NoError(t, col.AppendRow(poly))

	var got orb.Polygon
	require.NoError(t, col.ScanRow(&got, 0))
	assert.Equal(t, poly, got)
}

func TestPolygon_ScanRow_NativeScanDoublePtr(t *testing.T) {
	col := newGeoCol(t, "Polygon")

	poly := orb.Polygon{{{0, 0}, {3, 0}, {3, 3}, {0, 0}}}
	require.NoError(t, col.AppendRow(poly))

	var got *orb.Polygon
	require.NoError(t, col.ScanRow(&got, 0))
	require.NotNil(t, got)
	assert.Equal(t, poly, *got)
}

func TestPolygon_ScanRow_UnknownType_ReturnsError(t *testing.T) {
	col := newGeoCol(t, "Polygon")

	require.NoError(t, col.AppendRow(orb.Polygon{{{0, 0}, {1, 0}, {0, 0}}}))

	var s string
	err := col.ScanRow(&s, 0)
	assert.Error(t, err)
	assert.IsType(t, &ColumnConverterError{}, err)
}

func TestPolygon_ScanRow_SQLScanner_PropagatesError(t *testing.T) {
	col := newGeoCol(t, "Polygon")

	require.NoError(t, col.AppendRow(orb.Polygon{{{0, 0}, {1, 0}, {0, 0}}}))

	scanner := &geoScannerErr{scanErr: fmt.Errorf("scan failed")}
	err := col.ScanRow(scanner, 0)
	assert.EqualError(t, err, "scan failed")
}

func TestMultiLineString_ScanRow_SQLScanner(t *testing.T) {
	col := newGeoCol(t, "MultiLineString")

	mls := orb.MultiLineString{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}}
	require.NoError(t, col.AppendRow(mls))

	var scanner geoScanner
	require.NoError(t, col.ScanRow(&scanner, 0))
	assert.Equal(t, mls, scanner.val)
}

func TestMultiLineString_ScanRow_NativeScan(t *testing.T) {
	col := newGeoCol(t, "MultiLineString")

	mls := orb.MultiLineString{{{0, 0}, {1, 1}}}
	require.NoError(t, col.AppendRow(mls))

	var got orb.MultiLineString
	require.NoError(t, col.ScanRow(&got, 0))
	assert.Equal(t, mls, got)
}

func TestMultiLineString_ScanRow_NativeScanDoublePtr(t *testing.T) {
	col := newGeoCol(t, "MultiLineString")

	mls := orb.MultiLineString{{{2, 3}, {4, 5}}}
	require.NoError(t, col.AppendRow(mls))

	var got *orb.MultiLineString
	require.NoError(t, col.ScanRow(&got, 0))
	require.NotNil(t, got)
	assert.Equal(t, mls, *got)
}

func TestMultiLineString_ScanRow_UnknownType_ReturnsError(t *testing.T) {
	col := newGeoCol(t, "MultiLineString")

	require.NoError(t, col.AppendRow(orb.MultiLineString{{{1, 2}}}))

	var s string
	err := col.ScanRow(&s, 0)
	assert.Error(t, err)
	assert.IsType(t, &ColumnConverterError{}, err)
}

func TestMultiLineString_ScanRow_SQLScanner_PropagatesError(t *testing.T) {
	col := newGeoCol(t, "MultiLineString")

	require.NoError(t, col.AppendRow(orb.MultiLineString{{{1, 2}}}))

	scanner := &geoScannerErr{scanErr: fmt.Errorf("scan failed")}
	err := col.ScanRow(scanner, 0)
	assert.EqualError(t, err, "scan failed")
}

func TestMultiPolygon_ScanRow_SQLScanner(t *testing.T) {
	col := newGeoCol(t, "MultiPolygon")

	mp := orb.MultiPolygon{{{{0, 0}, {1, 0}, {1, 1}, {0, 0}}}}
	require.NoError(t, col.AppendRow(mp))

	var scanner geoScanner
	require.NoError(t, col.ScanRow(&scanner, 0))
	assert.Equal(t, mp, scanner.val)
}

func TestMultiPolygon_ScanRow_NativeScan(t *testing.T) {
	col := newGeoCol(t, "MultiPolygon")

	mp := orb.MultiPolygon{{{{0, 0}, {2, 0}, {2, 2}, {0, 0}}}}
	require.NoError(t, col.AppendRow(mp))

	var got orb.MultiPolygon
	require.NoError(t, col.ScanRow(&got, 0))
	assert.Equal(t, mp, got)
}

func TestMultiPolygon_ScanRow_NativeScanDoublePtr(t *testing.T) {
	col := newGeoCol(t, "MultiPolygon")

	mp := orb.MultiPolygon{{{{0, 0}, {3, 0}, {3, 3}, {0, 0}}}}
	require.NoError(t, col.AppendRow(mp))

	var got *orb.MultiPolygon
	require.NoError(t, col.ScanRow(&got, 0))
	require.NotNil(t, got)
	assert.Equal(t, mp, *got)
}

func TestMultiPolygon_ScanRow_UnknownType_ReturnsError(t *testing.T) {
	col := newGeoCol(t, "MultiPolygon")

	require.NoError(t, col.AppendRow(orb.MultiPolygon{{{{0, 0}, {1, 0}, {0, 0}}}}))

	var s string
	err := col.ScanRow(&s, 0)
	assert.Error(t, err)
	assert.IsType(t, &ColumnConverterError{}, err)
}

func TestMultiPolygon_ScanRow_SQLScanner_PropagatesError(t *testing.T) {
	col := newGeoCol(t, "MultiPolygon")

	require.NoError(t, col.AppendRow(orb.MultiPolygon{{{{0, 0}, {1, 0}, {0, 0}}}}))

	scanner := &geoScannerErr{scanErr: fmt.Errorf("scan failed")}
	err := col.ScanRow(scanner, 0)
	assert.EqualError(t, err, "scan failed")
}
