package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"

	chproto "github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

const flattenedJSONSetting = "output_format_native_use_flattened_dynamic_and_json_serialization"
const jsonSerializationProbe = `SELECT toUInt8(countIf(
 readonly = 0 AND (max IS NULL OR max = '1')
) = 1) FROM system.settings WHERE name = '` + flattenedJSONSetting + `'`

type jsonSerializationState struct{ disabled atomic.Bool }

func (s *jsonSerializationState) observeError(automatic bool, err error) {
	var exception *Exception
	if automatic && errors.As(err, &exception) && (exception.Code == 164 || exception.Code == 452) && strings.Contains(exception.Message, flattenedJSONSetting) {
		s.disabled.Store(true)
	}
}

func supportsFlattenedJSON(version proto.Version) bool {
	return proto.CheckMinVersion(proto.Version{Major: 25, Minor: 6}, version)
}

func (o *Options) useFlattenedJSON(enabled bool, querySettings Settings) bool {
	if !enabled || o.DisableAutoFlattenedJSONOutput {
		return false
	}
	if _, ok := o.Settings[flattenedJSONSetting]; ok {
		return false
	}
	_, ok := querySettings[flattenedJSONSetting]
	return !ok
}

func readJSONSerializationSettings(block *proto.Block) (bool, error) {
	if block.Rows() != 1 || len(block.Columns) != 1 {
		return false, errors.New("unexpected json settings probe result")
	}
	var allowed uint8
	if err := block.Columns[0].ScanRow(&allowed, 0); err != nil {
		return false, err
	}
	return allowed == 1, nil
}

func (c *connect) probeJSONSerialization(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, c.opt.DialTimeout)
	defer cancel()
	defer func() {
		if ctx.Err() != nil {
			err = fmt.Errorf("json settings probe: %w", ctx.Err())
			return
		}
		var exception *Exception
		if errors.As(err, &exception) {
			c.flattenedJSON = false
			c.logger.Warn("JSON settings probe failed, automatic "+flattenedJSONSetting+" disabled, set DisableAutoFlattenedJSONOutput to skip the probe", slog.Any("error", err))
			err = nil
		}
	}()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.startReadWriteTimeout(ctx)
	defer c.clearReadWriteTimeout(ctx)
	options := QueryOptions{settingsProbe: true}
	on := options.onProcess()
	var enabled bool
	scanErr := errors.New("no rows returned for json settings probe")
	on.data = func(block *proto.Block) {
		if block.Packet == proto.ServerData && scanErr != nil {
			enabled, scanErr = readJSONSerializationSettings(block)
		}
	}
	if err = c.sendQuery(jsonSerializationProbe, &options); err == nil {
		err = c.process(ctx, on)
	}
	if err != nil {
		return fmt.Errorf("json settings probe: %w", err)
	}
	if scanErr != nil {
		return fmt.Errorf("json settings probe: %w", scanErr)
	}
	c.flattenedJSON = enabled
	return nil
}

func (h *httpConnect) probeJSONSerialization(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, h.opt.DialTimeout)
	defer cancel()
	defer func() {
		if ctx.Err() != nil {
			err = fmt.Errorf("json settings probe: %w", ctx.Err())
			return
		}
		var exception *Exception
		if errors.As(err, &exception) {
			h.flattenedJSON = false
			h.logger.Warn("JSON settings probe failed, automatic "+flattenedJSONSetting+" disabled, set DisableAutoFlattenedJSONOutput to skip the probe", slog.Any("error", err))
			err = nil
		}
	}()
	options := QueryOptions{settingsProbe: true, settings: make(Settings)}
	h.applyNativeCompressionSettings(options.settings, false)
	res, err := h.sendQuery(ctx, jsonSerializationProbe, &options, nil) //nolint:bodyclose // closed below
	if err != nil {
		return fmt.Errorf("json settings probe: %w", err)
	}
	defer discardAndClose(res.Body)
	rw := h.compressionPool.Get()
	defer h.compressionPool.Put(rw)
	reader, err := rw.NewReader(res)
	if err != nil {
		return fmt.Errorf("json settings probe: %w", err)
	}
	captured := &capturingReader{reader: reader}
	chReader := chproto.NewReader(captured)
	scanErr := errors.New("no rows returned for json settings probe")
	var enabled bool
	for {
		block, err := h.readData(chReader, nil, &captured.buffer)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("json settings probe: %w", err)
		}
		if block.Rows() != 0 && scanErr != nil {
			enabled, scanErr = readJSONSerializationSettings(block)
		}
	}
	if scanErr != nil {
		return fmt.Errorf("json settings probe: %w", scanErr)
	}
	h.flattenedJSON = enabled
	return nil
}
