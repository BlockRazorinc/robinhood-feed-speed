package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"rh-feed-speed/internal/delayhist"
)

const (
	writeWait      = 5 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second
	MaxMessageSize = 15 * 1024 * 1024

	incrementalSummaryInterval = 5 * time.Minute
)

type sourceEntry struct {
	Name string
	URL  string
}

type sourceListFlag []sourceEntry

func (f *sourceListFlag) String() string {
	parts := make([]string, 0, len(*f))
	for _, source := range *f {
		parts = append(parts, source.Name+"="+source.URL)
	}
	return strings.Join(parts, "\n")
}

func (f *sourceListFlag) Set(value string) error {
	name, url, ok := strings.Cut(value, "=")
	if !ok {
		return fmt.Errorf("source must be in name=url format")
	}
	name = strings.TrimSpace(name)
	url = strings.TrimSpace(url)
	if name == "" {
		return fmt.Errorf("source name is required")
	}
	if url == "" {
		return fmt.Errorf("source url is required")
	}
	*f = append(*f, sourceEntry{Name: normalizeSourceName(name), URL: url})
	return nil
}

type sourceSubscriptionFlag map[string][]string

func (f *sourceSubscriptionFlag) String() string {
	if f == nil || *f == nil {
		return ""
	}
	names := make([]string, 0, len(*f))
	for name := range *f {
		names = append(names, name)
	}
	sort.Strings(names)

	parts := make([]string, 0)
	for _, name := range names {
		for _, payload := range (*f)[name] {
			parts = append(parts, name+"="+payload)
		}
	}
	return strings.Join(parts, "\n")
}

func (f *sourceSubscriptionFlag) Set(value string) error {
	name, payload, ok := strings.Cut(value, "=")
	if !ok {
		return fmt.Errorf("source subscription must be in name=payload format")
	}
	name = strings.TrimSpace(name)
	payload = strings.TrimSpace(payload)
	if name == "" {
		return fmt.Errorf("source subscription name is required")
	}
	if payload == "" {
		return fmt.Errorf("source subscription payload is required")
	}
	if *f == nil {
		*f = make(map[string][]string)
	}
	name = normalizeSourceName(name)
	(*f)[name] = append((*f)[name], payload)
	return nil
}

type sourceConfig struct {
	Name                 string
	URL                  string
	SubscriptionMessages []string
	ReconnectInterval    time.Duration
}

type blockStreamPayload struct {
	Version  int                  `json:"version"`
	Messages []blockStreamMessage `json:"messages"`
}

type blockStreamMessage struct {
	SequenceNumber uint64             `json:"sequenceNumber"`
	Message        nestedBlockMessage `json:"message"`
	BlockHash      string             `json:"blockHash"`
}

type nestedBlockMessage struct {
	Message nestedBlockPayload `json:"message"`
}

type nestedBlockPayload struct {
	Header blockHeader `json:"header"`
}

type blockHeader struct {
	BlockNumber uint64 `json:"blockNumber"`
	Timestamp   uint64 `json:"timestamp"`
}

type blockInfo struct {
	BlockHash      string
	BlockNumber    uint64
	SequenceNumber uint64
}

func (b blockInfo) ID() string {
	if hash := strings.TrimSpace(b.BlockHash); hash != "" {
		return strings.ToLower(hash)
	}
	if b.BlockNumber > 0 {
		return fmt.Sprintf("block:%d", b.BlockNumber)
	}
	return ""
}

func main() {
	metricsListenAddr := flag.String("metrics-listen", "127.0.0.1:9092", "Metrics HTTP listen address")
	metricsPath := flag.String("metrics-path", "/summary", "Metrics HTTP path")
	dedupTTL := flag.Duration("dedup-ttl", 30*time.Second, "How long speed-test keeps block arrival records")
	reconnectInterval := flag.Duration("reconnect-interval", 2*time.Second, "Reconnect interval after websocket disconnect")
	debug := flag.Bool("debug", false, "Enable per-message debug logs")

	var sourcesFlag sourceListFlag
	var sourceSubscriptions sourceSubscriptionFlag
	flag.Var(&sourcesFlag, "source", "Websocket source in name=url format. Can be repeated.")
	flag.Var(&sourceSubscriptions, "source-subscribe", "Websocket text message in name=payload format sent to a source after connect. Can be repeated.")
	flag.Parse()

	selfMetricsPath := normalizePath(*metricsPath)
	if selfMetricsPath == "/metrics" {
		log.Fatal("-metrics-path cannot be /metrics because /metrics is reserved for prometheus")
	}

	sources, err := buildSources(
		sourcesFlag,
		sourceSubscriptions,
		*reconnectInterval,
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceNames := sourceNamesFromConfigs(sources)
	tracker := NewTracker(*dedupTTL, sourceNames)
	for _, source := range sources {
		tracker.SetSourceURL(source.Name, source.URL)
	}
	promRegistry, winnerCounter := newPrometheusRegistry(sourceNames)
	tracker.SetWinnerCounter(winnerCounter)
	log.Printf("[speed-test] sources=%s", strings.Join(sourceNames, ","))
	log.Print(blockOutputHeader(sourceNames))

	metricsServer := &http.Server{
		Addr:              *metricsListenAddr,
		Handler:           metricsHandler(selfMetricsPath, tracker, promRegistry),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 3)
	go func() {
		log.Printf("[metrics] listening on http://%s%s prometheus=http://%s/metrics", *metricsListenAddr, selfMetricsPath, *metricsListenAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go runIncrementalSummaries(ctx, tracker, incrementalSummaryInterval)

	var wg sync.WaitGroup
	for _, source := range sources {
		wg.Add(1)
		go func(source sourceConfig) {
			defer wg.Done()
			runWebsocketSource(ctx, tracker, source, *debug)
		}(source)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		log.Printf("[speed-test] server error: %v", err)
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[metrics] shutdown error: %v", err)
	}
	wg.Wait()
	log.Printf("[speed-test] stopped")
}

func newPrometheusRegistry(sourceNames []string) (*prometheus.Registry, *prometheus.CounterVec) {
	registry := prometheus.NewRegistry()
	winnerCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "speed_test_source_wins_total",
			Help: "Total completed block wins by source.",
		},
		[]string{"source"},
	)
	registry.MustRegister(winnerCounter)
	for _, sourceName := range sourceNames {
		winnerCounter.WithLabelValues(sourceName).Add(0)
	}
	return registry, winnerCounter
}

func runIncrementalSummaries(ctx context.Context, tracker *Tracker, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			printSummary(tracker.SummaryWithPercentiles(now, interval))
		}
	}
}

func printSummary(summary SummarySnapshot) {
	log.Printf("[summary]\ttype\t%s\twindow\t%s\tcompleted_blocks\t%d",
		summary.Type,
		summary.Window,
		summary.CompletedBlocks,
	)
	for _, source := range summary.WinnerCounts {
		if source.Percentiles == nil {
			log.Printf("[summary]\ttype\t%s\tsource\t%s\twinner\t%d",
				summary.Type,
				source.Source,
				source.Count,
			)
			continue
		}
		log.Printf("[summary]\ttype\t%s\tsource\t%s\twinner\t%d\tp10\t%s\tp50\t%s\tp75\t%s\tp90\t%s\tp95\t%s\tp99\t%s\tp99.9\t%s\tmax\t%s",
			summary.Type,
			source.Source,
			source.Count,
			source.Percentiles.P10,
			source.Percentiles.P50,
			source.Percentiles.P75,
			source.Percentiles.P90,
			source.Percentiles.P95,
			source.Percentiles.P99,
			source.Percentiles.P999,
			source.Percentiles.Max,
		)
	}
}

func buildSources(
	sourcesFlag sourceListFlag,
	sourceSubscriptions sourceSubscriptionFlag,
	reconnectInterval time.Duration,
) ([]sourceConfig, error) {
	subscriptionsBySource := make(map[string][]string)
	for name, payloads := range sourceSubscriptions {
		normalized := normalizeSourceName(name)
		subscriptionsBySource[normalized] = append(subscriptionsBySource[normalized], payloads...)
	}

	if len(sourcesFlag) == 0 {
		return nil, fmt.Errorf("at least one -source is required")
	}

	sources := make([]sourceConfig, 0, len(sourcesFlag))
	for _, source := range sourcesFlag {
		sources = append(sources, sourceConfig{
			Name:              source.Name,
			URL:               source.URL,
			ReconnectInterval: reconnectInterval,
		})
	}

	seen := make(map[string]struct{}, len(sources))
	for i := range sources {
		sources[i].Name = normalizeSourceName(sources[i].Name)
		sources[i].URL = strings.TrimSpace(sources[i].URL)
		if sources[i].URL == "" {
			return nil, fmt.Errorf("source %q url is required", sources[i].Name)
		}
		if _, ok := seen[sources[i].Name]; ok {
			return nil, fmt.Errorf("duplicate source name %q", sources[i].Name)
		}
		seen[sources[i].Name] = struct{}{}
		sources[i].SubscriptionMessages = append([]string(nil), subscriptionsBySource[sources[i].Name]...)
	}
	return sources, nil
}

func sourceNamesFromConfigs(sources []sourceConfig) []string {
	names := make([]string, 0, len(sources))
	for _, source := range sources {
		names = append(names, source.Name)
	}
	return names
}

func runWebsocketSource(ctx context.Context, tracker *Tracker, source sourceConfig, debug bool) {
	if source.ReconnectInterval <= 0 {
		source.ReconnectInterval = 2 * time.Second
	}

	for {
		if ctx.Err() != nil {
			return
		}

		log.Printf("[%s] connecting %s", source.Name, source.URL)
		conn, _, err := websocket.DefaultDialer.Dial(source.URL, nil)
		if err != nil {
			tracker.SetDisconnected(source.Name, err)
			log.Printf("[%s] connect failed: %v", source.Name, err)
			if !waitOrDone(ctx, source.ReconnectInterval) {
				return
			}
			continue
		}

		tracker.SetConnected(source.Name, source.URL)
		log.Printf("[%s] connected", source.Name)
		err = consumeWebsocket(ctx, tracker, conn, source, debug)
		_ = conn.Close()
		tracker.SetDisconnected(source.Name, err)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("[%s] disconnected: %v", source.Name, err)
		}
		if !waitOrDone(ctx, source.ReconnectInterval) {
			return
		}
	}
}

func consumeWebsocket(ctx context.Context, tracker *Tracker, conn *websocket.Conn, source sourceConfig, debug bool) error {
	conn.SetReadLimit(MaxMessageSize)
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for _, payload := range source.SubscriptionMessages {
		_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
		if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
			return err
		}
	}

	stopPing := make(chan struct{})
	go pingWebsocket(ctx, conn, stopPing)
	defer close(stopPing)

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		receivedAt := time.Now()
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		blocks, err := extractBlocks(data)
		if err != nil {
			if debug {
				log.Printf("[%s] skip message size=%d parse_block_error=%v", source.Name, len(data), err)
			}
			continue
		}
		for _, block := range blocks {
			result := tracker.RecordBlock(source.Name, block, receivedAt)
			if result.CompletedLine != "" {
				log.Print(result.CompletedLine)
			}
			if debug {
				log.Printf("[%s] block_id=%s sequence_number=%d first_source=%s delay=%s later=%v duplicate=%v complete=%v",
					source.Name,
					result.BlockID,
					result.SequenceNumber,
					result.FirstSource,
					result.Delay,
					result.Later,
					result.Duplicate,
					result.Completed,
				)
			}
		}
	}
}

func pingWebsocket(ctx context.Context, conn *websocket.Conn, stop <-chan struct{}) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				_ = conn.Close()
				return
			}
		}
	}
}

func waitOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func extractBlocks(data []byte) ([]blockInfo, error) {
	var payload blockStreamPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}

	blocks := make([]blockInfo, 0, len(payload.Messages))
	for _, message := range payload.Messages {
		block := blockInfo{
			BlockHash:      strings.TrimSpace(message.BlockHash),
			BlockNumber:    message.Message.Message.Header.BlockNumber,
			SequenceNumber: message.SequenceNumber,
		}
		if block.ID() == "" {
			continue
		}
		blocks = append(blocks, block)
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("no block entries found")
	}
	return blocks, nil
}

func blockOutputHeader(sourceNames []string) string {
	columns := []string{"[block]", "sequence_number", "block_hash"}
	columns = append(columns, sourceNames...)
	columns = append(columns, "winner")
	return strings.Join(columns, "\t")
}

type Tracker struct {
	mu                     sync.Mutex
	ttl                    time.Duration
	records                blockRecordHeap
	newestFirstSeen        time.Time
	sources                map[string]*sourceStats
	sourceNames            []string
	winnerCounter          *prometheus.CounterVec
	totalArrivals          uint64
	totalFirstArrivals     uint64
	totalLaterArrivals     uint64
	totalDuplicateArrivals uint64
	totalCompletedBlocks   uint64
}

type blockRecord struct {
	BlockID        string
	BlockHash      string
	BlockNumber    uint64
	SequenceNumber uint64
	FirstSource    string
	FirstSeenAt    time.Time
	LastSeenAt     time.Time
	Arrivals       uint64
	SeenBy         map[string]sourceArrival
	Completed      bool
	CompletedAt    time.Time
}

type blockRecordHeap struct {
	items   []blockRecord
	indexes map[string]int
}

func newBlockRecordHeap(capacity int) blockRecordHeap {
	return blockRecordHeap{
		items:   make([]blockRecord, 0, capacity),
		indexes: make(map[string]int, capacity),
	}
}

func (h blockRecordHeap) Len() int {
	return len(h.items)
}

func (h blockRecordHeap) Less(i, j int) bool {
	if h.items[i].FirstSeenAt.Equal(h.items[j].FirstSeenAt) {
		return h.items[i].BlockID < h.items[j].BlockID
	}
	return h.items[i].FirstSeenAt.Before(h.items[j].FirstSeenAt)
}

func (h blockRecordHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.indexes[h.items[i].BlockID] = i
	h.indexes[h.items[j].BlockID] = j
}

func (h *blockRecordHeap) Push(value any) {
	record := value.(blockRecord)
	if h.indexes == nil {
		h.indexes = make(map[string]int)
	}
	h.indexes[record.BlockID] = len(h.items)
	h.items = append(h.items, record)
}

func (h *blockRecordHeap) Pop() any {
	n := len(h.items)
	record := h.items[n-1]
	delete(h.indexes, record.BlockID)
	h.items[n-1] = blockRecord{}
	h.items = h.items[:n-1]
	return record
}

func (h *blockRecordHeap) get(blockID string) (blockRecord, bool) {
	index, ok := h.indexes[blockID]
	if !ok {
		return blockRecord{}, false
	}
	return h.items[index], true
}

func (h *blockRecordHeap) set(record blockRecord) bool {
	index, ok := h.indexes[record.BlockID]
	if !ok {
		return false
	}
	oldFirstSeenAt := h.items[index].FirstSeenAt
	h.items[index] = record
	if !record.FirstSeenAt.Equal(oldFirstSeenAt) {
		heap.Fix(h, index)
	}
	return true
}

func (h *blockRecordHeap) push(record blockRecord) {
	if h.set(record) {
		return
	}
	heap.Push(h, record)
}

func (h *blockRecordHeap) peek() (blockRecord, bool) {
	if len(h.items) == 0 {
		return blockRecord{}, false
	}
	return h.items[0], true
}

func (h *blockRecordHeap) pop() blockRecord {
	return heap.Pop(h).(blockRecord)
}

func (h *blockRecordHeap) snapshotMap() map[string]blockRecord {
	records := make(map[string]blockRecord, len(h.items))
	for _, record := range h.items {
		record.SeenBy = cloneSourceArrivals(record.SeenBy)
		records[record.BlockID] = record
	}
	return records
}

func cloneSourceArrivals(arrivals map[string]sourceArrival) map[string]sourceArrival {
	if arrivals == nil {
		return nil
	}
	clone := make(map[string]sourceArrival, len(arrivals))
	for sourceName, arrival := range arrivals {
		clone[sourceName] = arrival
	}
	return clone
}

type sourceArrival struct {
	SeenAt time.Time
	Delay  time.Duration
	Count  uint64
}

type sourceStats struct {
	Name               string
	URL                string
	Connected          bool
	LastError          string
	ReconnectCount     uint64
	LastConnectedAt    time.Time
	LastDisconnectedAt time.Time
	LastMessageAt      time.Time
	ArrivalCount       uint64
	FirstCount         uint64
	LaterCount         uint64
	DuplicateCount     uint64
	WinCount           uint64
	TotalLaterDelay    time.Duration
	DelaySamples       []time.Duration
	Buckets            []uint64
}

type RecordResult struct {
	BlockID        string
	BlockHash      string
	SequenceNumber uint64
	FirstSource    string
	Delay          time.Duration
	Later          bool
	Duplicate      bool
	Completed      bool
	CompletedLine  string
}

type MetricsSnapshot struct {
	Now                    time.Time        `json:"now"`
	DedupTTL               string           `json:"dedup_ttl"`
	TotalUniqueInMap       int              `json:"total_unique_in_map"`
	TotalBlocksInMap       int              `json:"total_blocks_in_map"`
	TotalArrivals          uint64           `json:"total_arrivals"`
	TotalFirstArrivals     uint64           `json:"total_first_arrivals"`
	TotalLaterArrivals     uint64           `json:"total_later_arrivals"`
	TotalDuplicateArrivals uint64           `json:"total_duplicate_arrivals"`
	TotalCompletedBlocks   uint64           `json:"total_completed_blocks"`
	OldestFirstSeen        *time.Time       `json:"oldest_first_seen,omitempty"`
	NewestFirstSeen        *time.Time       `json:"newest_first_seen,omitempty"`
	WinnerCounts           []WinnerSnapshot `json:"winner_counts"`
	Sources                []SourceSnapshot `json:"sources"`
}

type SourceSnapshot struct {
	Source                 string             `json:"source"`
	URL                    string             `json:"url"`
	Connected              bool               `json:"connected"`
	LastError              string             `json:"last_error,omitempty"`
	ReconnectCount         uint64             `json:"reconnect_count"`
	LastConnectedAt        *time.Time         `json:"last_connected_at,omitempty"`
	LastDisconnectedAt     *time.Time         `json:"last_disconnected_at,omitempty"`
	LastMessageAt          *time.Time         `json:"last_message_at,omitempty"`
	ArrivalCount           uint64             `json:"arrival_count"`
	FirstCount             uint64             `json:"first_count"`
	LaterCount             uint64             `json:"later_count"`
	DuplicateCount         uint64             `json:"duplicate_count"`
	WinnerCount            uint64             `json:"winner_count"`
	TotalLaterDelay        string             `json:"total_later_delay"`
	TotalLaterDelayNanos   int64              `json:"total_later_delay_nanos"`
	AverageLaterDelay      string             `json:"average_later_delay"`
	AverageLaterDelayNanos int64              `json:"average_later_delay_nanos"`
	Histogram              []delayhist.Bucket `json:"histogram"`
}

type WinnerSnapshot struct {
	Source      string            `json:"source"`
	Count       uint64            `json:"count"`
	Percentiles *DelayPercentiles `json:"percentiles,omitempty"`
}

type SummarySnapshot struct {
	Now             time.Time        `json:"now"`
	Type            string           `json:"type"`
	Window          string           `json:"window"`
	WindowStart     *time.Time       `json:"window_start,omitempty"`
	WindowEnd       time.Time        `json:"window_end"`
	CompletedBlocks uint64           `json:"completed_blocks"`
	WinnerCounts    []WinnerSnapshot `json:"winner_counts"`
}

type DelayPercentiles struct {
	P10  time.Duration `json:"p10"`
	P50  time.Duration `json:"p50"`
	P75  time.Duration `json:"p75"`
	P90  time.Duration `json:"p90"`
	P95  time.Duration `json:"p95"`
	P99  time.Duration `json:"p99"`
	P999 time.Duration `json:"p99_9"`
	Max  time.Duration `json:"max"`
}

func NewTracker(ttl time.Duration, sourceNames []string) *Tracker {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	tracker := &Tracker{
		ttl:     ttl,
		records: newBlockRecordHeap(len(sourceNames)),
		sources: make(map[string]*sourceStats),
	}
	for _, sourceName := range sourceNames {
		sourceName = normalizeSourceName(sourceName)
		if _, ok := tracker.sources[sourceName]; ok {
			continue
		}
		tracker.sourceNames = append(tracker.sourceNames, sourceName)
		tracker.sources[sourceName] = &sourceStats{
			Name:    sourceName,
			Buckets: delayhist.NewCounts(),
		}
	}
	return tracker
}

func (t *Tracker) SetSourceURL(sourceName, url string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.ensureSourceLocked(sourceName)
	stats.URL = strings.TrimSpace(url)
}

func (t *Tracker) SetWinnerCounter(counter *prometheus.CounterVec) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.winnerCounter = counter
}

func (t *Tracker) SetConnected(sourceName, url string) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.ensureSourceLocked(sourceName)
	stats.URL = strings.TrimSpace(url)
	stats.Connected = true
	stats.LastError = ""
	stats.ReconnectCount++
	stats.LastConnectedAt = now
}

func (t *Tracker) SetDisconnected(sourceName string, err error) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.ensureSourceLocked(sourceName)
	stats.Connected = false
	stats.LastDisconnectedAt = now
	if err != nil && !errors.Is(err, context.Canceled) {
		stats.LastError = err.Error()
	}
}

func (t *Tracker) RecordBlock(sourceName string, block blockInfo, now time.Time) RecordResult {
	if now.IsZero() {
		now = time.Now()
	}
	sourceName = normalizeSourceName(sourceName)
	blockID := block.ID()

	t.mu.Lock()
	defer t.mu.Unlock()

	if blockID == "" {
		return RecordResult{}
	}

	t.pruneLocked(now)
	stats := t.ensureSourceLocked(sourceName)
	stats.ArrivalCount++
	stats.LastMessageAt = now
	t.totalArrivals++

	if record, ok := t.records.get(blockID); ok {
		if arrival, ok := record.SeenBy[sourceName]; ok {
			arrival.Count++
			record.SeenBy[sourceName] = arrival
			record.LastSeenAt = now
			record.Arrivals++
			stats.DuplicateCount++
			t.totalDuplicateArrivals++
			t.records.set(record)
			return RecordResult{
				BlockID:        blockID,
				BlockHash:      record.BlockHash,
				SequenceNumber: record.SequenceNumber,
				FirstSource:    record.FirstSource,
				Delay:          arrival.Delay,
				Duplicate:      true,
				Completed:      record.Completed,
			}
		}

		delay := now.Sub(record.FirstSeenAt)
		if delay < 0 {
			delay = 0
		}

		stats.LaterCount++
		stats.TotalLaterDelay += delay
		stats.Buckets = delayhist.Add(stats.Buckets, delay)
		t.totalLaterArrivals++

		record.LastSeenAt = now
		record.Arrivals++
		record.SeenBy[sourceName] = sourceArrival{
			SeenAt: now,
			Delay:  delay,
			Count:  1,
		}

		result := RecordResult{
			BlockID:        blockID,
			BlockHash:      record.BlockHash,
			SequenceNumber: record.SequenceNumber,
			FirstSource:    record.FirstSource,
			Delay:          delay,
			Later:          true,
			Completed:      record.Completed,
		}
		if line, completed := t.completeRecordIfReadyLocked(&record); completed {
			result.Completed = true
			result.CompletedLine = line
		}
		t.records.set(record)

		return result
	}

	stats.FirstCount++
	stats.Buckets = delayhist.Add(stats.Buckets, 0)
	t.totalFirstArrivals++
	record := blockRecord{
		BlockID:        blockID,
		BlockHash:      strings.TrimSpace(block.BlockHash),
		BlockNumber:    block.BlockNumber,
		SequenceNumber: block.SequenceNumber,
		FirstSource:    sourceName,
		FirstSeenAt:    now,
		LastSeenAt:     now,
		Arrivals:       1,
		SeenBy: map[string]sourceArrival{
			sourceName: {
				SeenAt: now,
				Delay:  0,
				Count:  1,
			},
		},
	}
	result := RecordResult{
		BlockID:        blockID,
		BlockHash:      record.BlockHash,
		SequenceNumber: record.SequenceNumber,
		FirstSource:    sourceName,
		Delay:          0,
	}
	if line, completed := t.completeRecordIfReadyLocked(&record); completed {
		result.Completed = true
		result.CompletedLine = line
	}
	t.records.push(record)
	if t.newestFirstSeen.IsZero() || record.FirstSeenAt.After(t.newestFirstSeen) {
		t.newestFirstSeen = record.FirstSeenAt
	}

	return result
}

func (t *Tracker) Snapshot() MetricsSnapshot {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	t.pruneLocked(now)

	var oldest *time.Time
	if record, ok := t.records.peek(); ok {
		value := record.FirstSeenAt
		oldest = &value
	}
	var newest *time.Time
	if t.records.Len() > 0 && !t.newestFirstSeen.IsZero() {
		value := t.newestFirstSeen
		newest = &value
	}

	sources := make([]SourceSnapshot, 0, len(t.sources))
	for _, stats := range t.sources {
		average := time.Duration(0)
		if stats.LaterCount > 0 {
			average = time.Duration(int64(stats.TotalLaterDelay) / int64(stats.LaterCount))
		}
		sources = append(sources, SourceSnapshot{
			Source:                 stats.Name,
			URL:                    stats.URL,
			Connected:              stats.Connected,
			LastError:              stats.LastError,
			ReconnectCount:         stats.ReconnectCount,
			LastConnectedAt:        timePtr(stats.LastConnectedAt),
			LastDisconnectedAt:     timePtr(stats.LastDisconnectedAt),
			LastMessageAt:          timePtr(stats.LastMessageAt),
			ArrivalCount:           stats.ArrivalCount,
			FirstCount:             stats.FirstCount,
			LaterCount:             stats.LaterCount,
			DuplicateCount:         stats.DuplicateCount,
			WinnerCount:            stats.WinCount,
			TotalLaterDelay:        stats.TotalLaterDelay.String(),
			TotalLaterDelayNanos:   int64(stats.TotalLaterDelay),
			AverageLaterDelay:      average.String(),
			AverageLaterDelayNanos: int64(average),
			Histogram:              delayhist.Snapshot(stats.Buckets),
		})
	}
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].Source < sources[j].Source
	})

	winnerCounts := make([]WinnerSnapshot, 0, len(t.sourceNames))
	for _, sourceName := range t.sourceNames {
		stats := t.ensureSourceLocked(sourceName)
		winnerCounts = append(winnerCounts, WinnerSnapshot{
			Source:      sourceName,
			Count:       stats.WinCount,
			Percentiles: delayPercentilesPtr(stats.DelaySamples),
		})
	}

	return MetricsSnapshot{
		Now:                    now,
		DedupTTL:               t.ttl.String(),
		TotalUniqueInMap:       t.records.Len(),
		TotalBlocksInMap:       t.records.Len(),
		TotalArrivals:          t.totalArrivals,
		TotalFirstArrivals:     t.totalFirstArrivals,
		TotalLaterArrivals:     t.totalLaterArrivals,
		TotalDuplicateArrivals: t.totalDuplicateArrivals,
		TotalCompletedBlocks:   t.totalCompletedBlocks,
		OldestFirstSeen:        oldest,
		NewestFirstSeen:        newest,
		WinnerCounts:           winnerCounts,
		Sources:                sources,
	}
}

func (t *Tracker) WinnerSummary() (uint64, []WinnerSnapshot) {
	summary := t.Summary(time.Now(), 0)
	return summary.CompletedBlocks, summary.WinnerCounts
}

func (t *Tracker) Summary(now time.Time, window time.Duration) SummarySnapshot {
	if now.IsZero() {
		now = time.Now()
	}

	t.mu.Lock()
	t.pruneLocked(now)
	records := t.records.snapshotMap()
	sourceNames := append([]string(nil), t.sourceNames...)
	sources := make(map[string]sourceStats, len(t.sources))
	for sourceName, stats := range t.sources {
		sources[sourceName] = cloneSourceStats(*stats)
	}
	totalCompletedBlocks := t.totalCompletedBlocks
	t.mu.Unlock()

	return buildSummary(now, window, records, sourceNames, sources, totalCompletedBlocks, false)
}

func (t *Tracker) SummaryWithPercentiles(now time.Time, window time.Duration) SummarySnapshot {
	if now.IsZero() {
		now = time.Now()
	}

	t.mu.Lock()
	t.pruneLocked(now)
	records := t.records.snapshotMap()
	sourceNames := append([]string(nil), t.sourceNames...)
	sources := make(map[string]sourceStats, len(t.sources))
	for sourceName, stats := range t.sources {
		sources[sourceName] = cloneSourceStats(*stats)
	}
	totalCompletedBlocks := t.totalCompletedBlocks
	t.mu.Unlock()

	return buildSummary(now, window, records, sourceNames, sources, totalCompletedBlocks, true)
}

func cloneSourceStats(stats sourceStats) sourceStats {
	stats.DelaySamples = append([]time.Duration(nil), stats.DelaySamples...)
	stats.Buckets = append([]uint64(nil), stats.Buckets...)
	return stats
}

func buildSummary(
	now time.Time,
	window time.Duration,
	records map[string]blockRecord,
	sourceNames []string,
	sources map[string]sourceStats,
	totalCompletedBlocks uint64,
	includeWindowPercentiles bool,
) SummarySnapshot {
	summaryType := "full"
	windowText := "all"
	var windowStart *time.Time
	if window > 0 {
		summaryType = "incremental"
		windowText = window.String()
		start := now.Add(-window)
		windowStart = &start
	}

	if windowStart != nil {
		return incrementalRecordSummary(now, windowText, *windowStart, records, sourceNames, includeWindowPercentiles)
	}

	winners := make([]WinnerSnapshot, 0, len(sourceNames))
	for _, sourceName := range sourceNames {
		stats := sources[sourceName]
		winners = append(winners, WinnerSnapshot{
			Source:      sourceName,
			Count:       stats.WinCount,
			Percentiles: delayPercentilesPtr(stats.DelaySamples),
		})
	}

	return SummarySnapshot{
		Now:             now,
		Type:            summaryType,
		Window:          windowText,
		WindowStart:     windowStart,
		WindowEnd:       now,
		CompletedBlocks: totalCompletedBlocks,
		WinnerCounts:    winners,
	}
}

func incrementalRecordSummary(
	now time.Time,
	windowText string,
	windowStart time.Time,
	records map[string]blockRecord,
	sourceNames []string,
	includePercentiles bool,
) SummarySnapshot {
	type sourceSummary struct {
		wins    uint64
		samples []time.Duration
	}
	bySource := make(map[string]*sourceSummary, len(sourceNames))
	for _, sourceName := range sourceNames {
		bySource[sourceName] = &sourceSummary{}
	}

	completedBlocks := uint64(0)
	for _, record := range records {
		if !record.Completed {
			continue
		}
		completedAt := record.CompletedAt
		if completedAt.IsZero() {
			completedAt = record.LastSeenAt
		}
		if completedAt.Before(windowStart) || completedAt.After(now) {
			continue
		}
		completedBlocks++
		if summary := bySource[record.FirstSource]; summary != nil {
			summary.wins++
		}
		if includePercentiles {
			for _, sourceName := range sourceNames {
				arrival, ok := record.SeenBy[sourceName]
				if !ok {
					continue
				}
				bySource[sourceName].samples = append(bySource[sourceName].samples, arrival.Delay)
			}
		}
	}

	winners := make([]WinnerSnapshot, 0, len(sourceNames))
	for _, sourceName := range sourceNames {
		sourceSummary := bySource[sourceName]
		winner := WinnerSnapshot{
			Source: sourceName,
			Count:  sourceSummary.wins,
		}
		if includePercentiles {
			winner.Percentiles = delayPercentilesPtr(sourceSummary.samples)
		}
		winners = append(winners, winner)
	}
	return SummarySnapshot{
		Now:             now,
		Type:            "incremental",
		Window:          windowText,
		WindowStart:     &windowStart,
		WindowEnd:       now,
		CompletedBlocks: completedBlocks,
		WinnerCounts:    winners,
	}
}

func delayPercentilesPtr(samples []time.Duration) *DelayPercentiles {
	percentiles := delayPercentiles(samples)
	if len(samples) == 0 {
		return nil
	}
	return &percentiles
}

func delayPercentiles(samples []time.Duration) DelayPercentiles {
	if len(samples) == 0 {
		return DelayPercentiles{}
	}
	sortedSamples := append([]time.Duration(nil), samples...)
	sort.Slice(sortedSamples, func(i, j int) bool {
		return sortedSamples[i] < sortedSamples[j]
	})
	return DelayPercentiles{
		P10:  nearestRankPercentile(sortedSamples, 10),
		P50:  nearestRankPercentile(sortedSamples, 50),
		P75:  nearestRankPercentile(sortedSamples, 75),
		P90:  nearestRankPercentile(sortedSamples, 90),
		P95:  nearestRankPercentile(sortedSamples, 95),
		P99:  nearestRankPercentile(sortedSamples, 99),
		P999: nearestRankPermille(sortedSamples, 999),
		Max:  sortedSamples[len(sortedSamples)-1],
	}
}

func nearestRankPercentile(sortedSamples []time.Duration, percentile int) time.Duration {
	if len(sortedSamples) == 0 {
		return 0
	}
	rank := (percentile*len(sortedSamples) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > len(sortedSamples) {
		rank = len(sortedSamples)
	}
	return sortedSamples[rank-1]
}

func nearestRankPermille(sortedSamples []time.Duration, permille int) time.Duration {
	if len(sortedSamples) == 0 {
		return 0
	}
	rank := (permille*len(sortedSamples) + 999) / 1000
	if rank < 1 {
		rank = 1
	}
	if rank > len(sortedSamples) {
		rank = len(sortedSamples)
	}
	return sortedSamples[rank-1]
}

func (t *Tracker) ensureSourceLocked(sourceName string) *sourceStats {
	sourceName = normalizeSourceName(sourceName)
	stats, ok := t.sources[sourceName]
	if ok {
		return stats
	}
	stats = &sourceStats{
		Name:    sourceName,
		Buckets: delayhist.NewCounts(),
	}
	t.sources[sourceName] = stats
	return stats
}

func (t *Tracker) completeRecordIfReadyLocked(record *blockRecord) (string, bool) {
	if record.Completed {
		return "", false
	}
	for _, sourceName := range t.sourceNames {
		if _, ok := record.SeenBy[sourceName]; !ok {
			return "", false
		}
	}
	record.Completed = true
	record.CompletedAt = record.LastSeenAt
	t.totalCompletedBlocks++
	stats := t.ensureSourceLocked(record.FirstSource)
	stats.WinCount++
	if t.winnerCounter != nil {
		t.winnerCounter.WithLabelValues(record.FirstSource).Inc()
	}
	for _, sourceName := range t.sourceNames {
		arrival := record.SeenBy[sourceName]
		stats := t.ensureSourceLocked(sourceName)
		stats.DelaySamples = append(stats.DelaySamples, arrival.Delay)
	}
	return t.formatBlockLineLocked(*record), true
}

func (t *Tracker) formatBlockLineLocked(record blockRecord) string {
	sequenceNumber := "-"
	if record.SequenceNumber > 0 {
		sequenceNumber = fmt.Sprintf("%d", record.SequenceNumber)
	}
	blockHash := strings.TrimSpace(record.BlockHash)
	if blockHash == "" {
		blockHash = record.BlockID
	}

	columns := []string{"[block]", sequenceNumber, blockHash}
	for _, sourceName := range t.sourceNames {
		arrival, ok := record.SeenBy[sourceName]
		if !ok {
			columns = append(columns, "NA")
			continue
		}
		columns = append(columns, arrival.Delay.String())
	}
	columns = append(columns, record.FirstSource)
	return strings.Join(columns, "\t")
}

func (t *Tracker) pruneLocked(now time.Time) {
	if t.ttl <= 0 {
		return
	}
	for {
		record, ok := t.records.peek()
		if !ok || now.Sub(record.FirstSeenAt) <= t.ttl {
			return
		}
		t.records.pop()
		if t.records.Len() == 0 {
			t.newestFirstSeen = time.Time{}
		}
	}
}

func metricsHandler(metricsPath string, tracker *Tracker, registry *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(metricsPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(tracker.SummaryWithPercentiles(time.Now(), incrementalSummaryInterval)); err != nil {
			log.Printf("[metrics] encode summary failed: %v", err)
		}
	})
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	return mux
}

func normalizePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/selfMetrics"
	}
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}

func normalizeSourceName(sourceName string) string {
	sourceName = strings.TrimSpace(sourceName)
	if sourceName == "" {
		return "unknown"
	}
	return sourceName
}

func timePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}
