package clickhouse

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func Test_parseDsn(t *testing.T) {
	type (
		args struct {
			dsn string
		}
		expectedVal struct {
			value interface{}
			field string
		}
		testCase struct {
			name    string
			args    args
			want    expectedVal
			wantErr bool
		}
	)

	tests := []testCase{
		{
			name: "Return error on invalid DSN",
			args: args{
				dsn: "carl:/localhost:44432",
			},
			want: expectedVal{
				value: nil,
			},
			wantErr: true,
		},
		{
			name: "Correctly url-escape username",
			args: args{
				dsn: "tcp://127.0.0.1:9000?username=testUsername++",
			},
			want: expectedVal{
				value: "testUsername++",
				field: "username",
			},
			wantErr: false,
		},
		{
			name: "Correctly url-escape password",
			args: args{
				dsn: "tcp://@127.0.0.1:9000?password=RSzqnN+n",
			},
			want: expectedVal{
				value: "RSzqnN+n",
				field: "password",
			},
			wantErr: false,
		},
		{
			name: "Correctly get conn open strategy",
			args: args{
				dsn: "tcp://@127.0.0.1:9000?connection_open_strategy=in_order",
			},
			want: expectedVal{
				value: connOpenInOrder,
				field: "connection_open_strategy",
			},
			wantErr: false,
		},
		{
			name: "Correctly get compress",
			args: args{
				dsn: "tcp://@127.0.0.1:9000?compress=true",
			},
			want: expectedVal{
				value: true,
				field: "compress",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedDsn, err := parseDsn(tt.args.dsn)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parseDsn() error = %v, wantErr %v", err, tt.wantErr)
				} else {
					return
				}
			}

			if tt.want.value != nil {
				switch tt.want.field {
				case "username":
					assert.Equal(t, tt.want.value.(string), parsedDsn.username)
				case "password":
					assert.Equal(t, tt.want.value.(string), parsedDsn.password)
				case "connection_open_strategy":
					assert.Equal(t, tt.want.value.(openStrategy), parsedDsn.connOpts.openStrategy)
				case "compress":
					assert.Equal(t, tt.want.value.(bool), parsedDsn.compress)
				}
			}
		})
	}

}
