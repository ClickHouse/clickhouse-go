package column

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestTimeScanType verifies that Time type returns time.Duration from ScanType()
func TestTimeScanType_issue1757(t *testing.T) {
	col := &Time{}

	scanType := col.ScanType()
	expectedType := reflect.TypeOf(time.Duration(0))

	assert.Equal(t, expectedType, scanType,
		"Time.ScanType() should return time.Duration, got %v", scanType)
}

// TestTime64ScanType verifies that Time64 type returns time.Duration from ScanType()
func TestTime64ScanType_issue1757(t *testing.T) {
	col := &Time64{}

	scanType := col.ScanType()
	expectedType := reflect.TypeOf(time.Duration(0))

	assert.Equal(t, expectedType, scanType,
		"Time64.ScanType() should return time.Duration, got %v", scanType)
}
