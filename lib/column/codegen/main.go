package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"go/format"
	"log"
	"os"
	"sort"
	"text/template"
)

var (
	//go:embed column.tpl
	columnSrc string
)
var (
	types []_type
)

type _type struct {
	ChType string
	GoType string
}

func init() {
	for _, size := range []int{8, 16, 32, 64} {
		types = append(types, _type{
			ChType: fmt.Sprintf("Int%d", size),
			GoType: fmt.Sprintf("int%d", size),
		}, _type{
			ChType: fmt.Sprintf("UInt%d", size),
			GoType: fmt.Sprintf("uint%d", size),
		})
	}
	for _, size := range []int{32, 64} {
		types = append(types, _type{
			ChType: fmt.Sprintf("Float%d", size),
			GoType: fmt.Sprintf("float%d", size),
		})
	}
	sort.Slice(types, func(i, j int) bool {
		return sequenceKey(types[i].ChType) < sequenceKey(types[j].ChType)
	})
}
func write(name string, v interface{}, t *template.Template) error {
	out := new(bytes.Buffer)
	if err := t.Execute(out, v); err != nil {
		return err
	}
	//	fmt.Println(out.String())
	data, err := format.Source(out.Bytes())
	if err != nil {
		return err
	}
	if err := os.WriteFile(name+".go", data, 0o600); err != nil {
		return err
	}
	return nil
}

func main() {
	var (
		columnTpl = template.Must(template.New("column").Parse(columnSrc))
	)
	if err := write("column_gen", types, columnTpl); err != nil {
		log.Fatal(err)
	}
}

const maxByte = 1<<8 - 1

func isDigit(d byte) bool {
	return '0' <= d && d <= '9'
}

func sequenceKey(key string) string {
	sKey := make([]byte, 0, len(key)+8)
	j := -1
	for i := 0; i < len(key); i++ {
		b := key[i]
		if !isDigit(b) {
			sKey = append(sKey, b)
			j = -1
			continue
		}
		if j == -1 {
			sKey = append(sKey, 0x00)
			j = len(sKey) - 1
		}
		if sKey[j] == 1 && sKey[j+1] == '0' {
			sKey[j+1] = b
			continue
		}
		if sKey[j]+1 > maxByte {
			panic("sequenceKey: invalid key")
		}
		sKey = append(sKey, b)
		sKey[j]++
	}
	return string(sKey)
}
