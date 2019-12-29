package clickhouse

import (
	"database/sql/driver"
	"reflect"
	"testing"
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
