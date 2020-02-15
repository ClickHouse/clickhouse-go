package clickhouse

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

func Test_bootstrap_Open(t *testing.T) {
	type args struct {
		dsn string
	}
	tests := []struct {
		name    string
		d       *bootstrap
		args    args
		want    driver.Conn
		wantErr bool
	}{
		{
			name:    "Return nil connection when error occured",
			d:       &bootstrap{},
			args:    args{dsn: "rubbish"},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &bootstrap{}
			got, err := d.Open(tt.args.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("bootstrap.Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("bootstrap.Open() = %#v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpen(t *testing.T) {
	type args struct {
		dsn string
	}
	tests := []struct {
		name    string
		args    args
		want    driver.Conn
		wantErr bool
	}{
		{
			name:    "Return nil connection when error occured",
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Open(tt.args.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Open() = %#v, want %v", got, tt.want)
			}
		})
	}
}

func Test_now(t *testing.T) {
	tests := []struct {
		name          string
		sleepDuration time.Duration
		want          time.Time
	}{
		{
			name:          "1 second",
			sleepDuration: time.Second,
			want:          time.Unix(1, 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			time.Sleep(tt.sleepDuration)
			time.Sleep(time.Millisecond)
			got := now()
			if !got.Equal(tt.want) {
				t.Errorf("now() = %s, want %s", got.Format(time.RFC3339), tt.want.Format(time.RFC3339))
			}
		})
	}
}
