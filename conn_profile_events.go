package clickhouse

import (
	"time"
)

type ProfileEvent struct {
	Hostname    string
	CurrentTime time.Time
	ThreadID    uint64
	Type        string
	Name        string
	Value       int64
}

func (c *connect) profileEvents() ([]ProfileEvent, error) {
	block, err := c.readData(false)
	if err != nil {
		return nil, err
	}
	c.debugf("[profile events] rows=%d", block.Rows())
	var (
		events []ProfileEvent
		names  = block.ColumnsNames()
	)
	for r := 0; r < block.Rows(); r++ {
		var event ProfileEvent
		for i, b := range block.Columns {
			switch names[i] {
			case "host_name":
				if err := b.ScanRow(&event.Hostname, r); err != nil {
					return nil, err
				}
			case "current_time":
				if err := b.ScanRow(&event.CurrentTime, r); err != nil {
					return nil, err
				}
			case "thread_id":
				if err := b.ScanRow(&event.ThreadID, r); err != nil {
					return nil, err
				}
			case "type":
				if err := b.ScanRow(&event.Type, r); err != nil {
					return nil, err
				}
			case "name":
				if err := b.ScanRow(&event.Name, r); err != nil {
					return nil, err
				}
			case "value":
				if err := b.ScanRow(&event.Value, r); err != nil {
					return nil, err
				}
			}
		}
		events = append(events, event)
	}
	return events, nil
}
