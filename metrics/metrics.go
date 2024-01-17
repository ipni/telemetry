package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/pcache"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	api "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
)

var logger = log.Logger("telemetry/metrics")

const shutdownTimeout = 5 * time.Second

type IngestRate struct {
	AvgMhPerAd int64
	MhPerSec   int64
	ProviderID peer.ID
}

type SortableRates []*IngestRate

func (r SortableRates) Len() int           { return len(r) }
func (r SortableRates) Less(i, j int) bool { return r[i].MhPerSec < r[j].MhPerSec }
func (r SortableRates) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

type Metrics struct {
	pcache   *pcache.ProviderCache
	server   *http.Server
	exporter *prometheus.Exporter

	smallDist  int64
	mediumDist int64
	largeDist  int64
	totalDist  int64

	avgMhPerSec     int64
	fastestMhPerSec int64
	slowestMhPerSec int64

	slowRates  []*IngestRate
	slowCount  int
	ratesMutex sync.Mutex

	distanceUpdateCounter      api.Int64Counter
	providerCount              api.Int64ObservableGauge
	providerErrorUpDownCounter api.Int64UpDownCounter

	providerSmallDist  api.Int64ObservableGauge
	providerMediumDist api.Int64ObservableGauge
	providerLargeDist  api.Int64ObservableGauge
	providerTotalDist  api.Int64ObservableGauge

	providerAvgIngestRate     api.Int64ObservableGauge
	providerFastestIngestRate api.Int64ObservableGauge
	providerSlowestIngestRate api.Int64ObservableGauge
	providerSlowCount         api.Int64ObservableGauge
	providerSlowIngestRate    api.Int64ObservableGauge
}

func New(listenAddr string, pc *pcache.ProviderCache) *Metrics {
	return &Metrics{
		pcache: pc,
		server: &http.Server{
			Addr: listenAddr,
		},
	}
}

func (m *Metrics) Start(slowRate, nSlowest int) error {
	const namePrefix = "ipni_telemetry_"

	var err error
	if m.exporter, err = prometheus.New(
		prometheus.WithoutUnits(),
		prometheus.WithoutScopeInfo(),
		prometheus.WithoutTargetInfo()); err != nil {
		return err
	}
	provider := metric.NewMeterProvider(metric.WithReader(m.exporter))
	meter := provider.Meter("ipni-telemetry")

	if m.distanceUpdateCounter, err = meter.Int64Counter(
		namePrefix+"provider_distance_update_count",
		api.WithUnit("1"),
		api.WithDescription("Number of distance updates for all providers."),
	); err != nil {
		return err
	}
	if m.providerErrorUpDownCounter, err = meter.Int64UpDownCounter(
		namePrefix+"provider_error_count",
		api.WithUnit("1"),
		api.WithDescription("Number of providers with ingestion errors"),
	); err != nil {
		return err
	}
	if m.providerCount, err = meter.Int64ObservableGauge(
		namePrefix+"provider_count",
		api.WithUnit("1"),
		api.WithDescription("Number of providers."),
		api.WithInt64Callback(m.reportProviderCount),
	); err != nil {
		return err
	}

	if m.providerSmallDist, err = meter.Int64ObservableGauge(
		namePrefix+"provider_small_distance",
		api.WithUnit("1"),
		api.WithDescription("Number of providers with small distance (less than 33% max)"),
		api.WithInt64Callback(m.reportProviderSmallDist),
	); err != nil {
		return err
	}
	if m.providerMediumDist, err = meter.Int64ObservableGauge(
		namePrefix+"provider_medium_distance",
		api.WithUnit("1"),
		api.WithDescription("Number of providers with medium distance (33% to 66% max)"),
		api.WithInt64Callback(m.reportProviderMediumDist),
	); err != nil {
		return err
	}
	if m.providerLargeDist, err = meter.Int64ObservableGauge(
		namePrefix+"provider_large_distance",
		api.WithUnit("1"),
		api.WithDescription("Number of providers with large distance (greater then 66% max)"),
		api.WithInt64Callback(m.reportProviderLargeDist),
	); err != nil {
		return err
	}
	if m.providerTotalDist, err = meter.Int64ObservableGauge(
		namePrefix+"provider_total_distance",
		api.WithUnit("1"),
		api.WithDescription("Total distance for all non-error providers"),
		api.WithInt64Callback(m.reportProviderTotalDist),
	); err != nil {
		return err
	}

	if m.providerAvgIngestRate, err = meter.Int64ObservableGauge(
		namePrefix+"provider_avg_ingest_rate",
		api.WithUnit("mh/sec"),
		api.WithDescription("Average ingest rate for all non-error providers"),
		api.WithInt64Callback(m.reportProviderAvgIngestRate),
	); err != nil {
		return err
	}
	if m.providerFastestIngestRate, err = meter.Int64ObservableGauge(
		namePrefix+"provider_fastest_ingest_rate",
		api.WithUnit("mh/sec"),
		api.WithDescription("Fastest ingest rate"),
		api.WithInt64Callback(m.reportProviderFastestIngestRate),
	); err != nil {
		return err
	}
	if m.providerSlowestIngestRate, err = meter.Int64ObservableGauge(
		namePrefix+"provider_slowest_ingest_rate",
		api.WithUnit("mh/sec"),
		api.WithDescription("Slowest non-zero ingest rate"),
		api.WithInt64Callback(m.reportProviderSlowestIngestRate),
	); err != nil {
		return err
	}

	if m.providerSlowCount, err = meter.Int64ObservableGauge(
		namePrefix+"provider_slow_count",
		api.WithUnit("1"),
		api.WithDescription(fmt.Sprintf("Number of providers with slow ingestion rate (below %d mh/sec)", slowRate)),
		api.WithInt64Callback(m.reportProviderSlowCount),
	); err != nil {
		return err
	}
	if m.providerSlowIngestRate, err = meter.Int64ObservableGauge(
		namePrefix+"provider_slow_ingest_rate",
		api.WithUnit("mh/sec"),
		api.WithDescription(fmt.Sprintf("%d Slowest Provider Ingestion Rates", nSlowest)),
		api.WithInt64Callback(m.reportProviderSlowIngestRates),
	); err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	m.server.Handler = mux

	go func() {
		_ = m.server.ListenAndServe()
	}()

	m.server.RegisterOnShutdown(func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := m.exporter.Shutdown(ctx); err != nil {
			logger.Errorw("Failed to shut down Prometheus exporter", "err", err)
		}
	})
	logger.Infow("Metric server started", "addr", m.server.Addr)
	return nil
}

func (m *Metrics) NotifyProviderErrored(ctx context.Context, err error) {
	errKindAttr := errKindAttribute(err)
	m.providerErrorUpDownCounter.Add(ctx, 1, api.WithAttributes(errKindAttr))
}

func (m *Metrics) NotifyProviderErrorCleared(ctx context.Context, err error) {
	errKindAttr := errKindAttribute(err)
	m.providerErrorUpDownCounter.Add(ctx, -1, api.WithAttributes(errKindAttr))
}

func (m *Metrics) UpdateProviderDistanceBuckets(ctx context.Context, small, medium, large, totalDist int64) {
	m.smallDist = small
	m.mediumDist = medium
	m.largeDist = large
	m.totalDist = totalDist
}

func (m *Metrics) UpdateIngestRates(slowRates []*IngestRate, slowCount int, avg, fastest, slowest int64) {
	ratesCopy := make([]*IngestRate, len(slowRates))
	copy(ratesCopy, slowRates)

	m.ratesMutex.Lock()
	defer m.ratesMutex.Unlock()

	m.slowRates = ratesCopy
	m.slowCount = slowCount
	m.avgMhPerSec = avg
	m.fastestMhPerSec = fastest
	m.slowestMhPerSec = slowest
}

func (m *Metrics) reportProviderCount(_ context.Context, observer api.Int64Observer) error {
	observer.Observe(int64(m.pcache.Len()))
	return nil
}

func (m *Metrics) reportProviderSmallDist(_ context.Context, observer api.Int64Observer) error {
	observer.Observe(m.smallDist)
	return nil
}

func (m *Metrics) reportProviderMediumDist(_ context.Context, observer api.Int64Observer) error {
	observer.Observe(m.mediumDist)
	return nil
}

func (m *Metrics) reportProviderLargeDist(_ context.Context, observer api.Int64Observer) error {
	observer.Observe(m.largeDist)
	return nil
}

func (m *Metrics) reportProviderTotalDist(_ context.Context, observer api.Int64Observer) error {
	observer.Observe(m.totalDist)
	return nil
}

func (m *Metrics) reportProviderAvgIngestRate(_ context.Context, observer api.Int64Observer) error {
	m.ratesMutex.Lock()
	defer m.ratesMutex.Unlock()
	observer.Observe(m.avgMhPerSec)
	return nil
}

func (m *Metrics) reportProviderFastestIngestRate(_ context.Context, observer api.Int64Observer) error {
	m.ratesMutex.Lock()
	defer m.ratesMutex.Unlock()
	observer.Observe(m.fastestMhPerSec)
	return nil
}

func (m *Metrics) reportProviderSlowestIngestRate(_ context.Context, observer api.Int64Observer) error {
	m.ratesMutex.Lock()
	defer m.ratesMutex.Unlock()
	observer.Observe(m.slowestMhPerSec)
	return nil
}

func (m *Metrics) reportProviderSlowCount(_ context.Context, observer api.Int64Observer) error {
	m.ratesMutex.Lock()
	defer m.ratesMutex.Unlock()
	observer.Observe(int64(m.slowCount))
	return nil
}

func (m *Metrics) reportProviderSlowIngestRates(_ context.Context, observer api.Int64Observer) error {
	m.ratesMutex.Lock()
	defer m.ratesMutex.Unlock()
	for _, provRate := range m.slowRates {
		pidAttr := providerAttr(provRate.ProviderID)
		observer.Observe(provRate.MhPerSec, api.WithAttributes(pidAttr))
	}
	return nil
}

func providerAttr(providerID peer.ID) attribute.KeyValue {
	return attribute.Key("provider-id").String(providerID.String())
}

func errKindAttribute(err error) attribute.KeyValue {
	// TODO check logs for other popular error kinds we might care about.
	var errKind string
	switch {
	case strings.Contains(err.Error(), "failed to sync first entry"):
		errKind = "cannot-fetch-any-entries"
	case strings.Contains(err.Error(), "failed to sync entries"):
		errKind = "fetch-some-entries-failed"
	case strings.Contains(err.Error(), "timed out waiting"):
		errKind = "timed-out-waiting"
	case strings.Contains(err.Error(), "response rejected"):
		errKind = "response-rejected"
	case strings.Contains(err.Error(), "for Complete message from remote peer"):
		errKind = "incomplete-response"
	case strings.Contains(err.Error(), "cannot query head for sync"):
		errKind = "cannot-query-head"
	default:
		errKind = "other"
	}
	return attribute.Key("error-kind").String(errKind)
}

func (m *Metrics) Shutdown() error {
	return m.server.Shutdown(context.Background())
}
