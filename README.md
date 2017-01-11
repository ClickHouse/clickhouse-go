# clickhouse

```go 
import(
    "database/sql"
   _ "github.com/kshvakov/clickhouse"
)
func main(){
    connect, _ := sql.Open("clickhouse", "http://127.0.0.1:8123?debug=true")
    rows, _ := connect.Query("SELECT app_id, language, country, date, datetime FROM stats WHERE app_id IN (?, ?, ?) LIMIT 20", 1, 2, 3)
    fmt.Println(rows.Columns())
    for rows.Next() {
        var (
            appID             int
            language, country string
            date, datetime    time.Time
        )
        if err := rows.Scan(&appID, &language, &country, &date, &datetime); err == nil {
            fmt.Printf("AppID: %d, language: %s, country: %s, date: %s, datetime: %s\n", appID, language, country, date, datetime)
        }
    }

    tx, _ := connect.Begin()
    if stmt, err := tx.Prepare("INSERT INTO imps (a, b, c, d) VALUES (?, ?, ?, ?)"); err == nil {
        for i := 0; i < 10; i++ {
            stmt.Exec(1, 2, 3, 4)
        }
    }
    tx.Commit()
}
```