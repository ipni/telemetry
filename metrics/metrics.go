package metrics

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/pcache"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/sdk/metric"
)

var logger = log.Logger("telemetry/metrics")

const shutdownTimeout = 5 * time.Second

type Metrics struct {
	pcache   *pcache.ProviderCache
	server   *http.Server
	exporter *prometheus.Exporter

	distanceUpdateCounter      instrument.Int64Counter
	providerDistanceHistogram  instrument.Int64Histogram
	providerErrorUpDownCounter instrument.Int64UpDownCounter
	providerCount              instrument.Int64ObservableGauge
}

func New(listenAddr string, pc *pcache.ProviderCache) *Metrics {
	return &Metrics{
		pcache: pc,
		server: &http.Server{
			Addr: listenAddr,
		},
	}
}

func (m *Metrics) Start() error {
	var err error
	if m.exporter, err = prometheus.New(
		prometheus.WithoutUnits(),
		prometheus.WithoutScopeInfo(),
		prometheus.WithoutTargetInfo()); err != nil {
		return err
	}
	provider := metric.NewMeterProvider(metric.WithReader(m.exporter))
	meter := provider.Meter("ipni/cassette")

	if m.distanceUpdateCounter, err = meter.Int64Counter(
		"ipni/telemetry/udistance_pdate_count",
		instrument.WithUnit("1"),
		instrument.WithDescription("Number of distance updates for all providers."),
	); err != nil {
		return err
	}
	if m.providerDistanceHistogram, err = meter.Int64Histogram(
		"ipni/telemetry/provider_distance",
		instrument.WithUnit("1"),
		instrument.WithDescription("Provider advertisement distances."),
	); err != nil {
		return err
	}
	if m.providerErrorUpDownCounter, err = meter.Int64UpDownCounter(
		"ipni/telemetry/provider_error_count",
		instrument.WithUnit("1"),
		instrument.WithDescription("Number of providers with ingestion errors."),
	); err != nil {
		return err
	}
	if m.providerCount, err = meter.Int64ObservableGauge(
		"ipni/telemetry/providers_count",
		instrument.WithUnit("1"),
		instrument.WithDescription("The number of providers."),
		instrument.WithInt64Callback(m.reportProviderCount),
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
	m.providerErrorUpDownCounter.Add(ctx, 1, errKindAttr)
}

func (m *Metrics) NotifyProviderErrorCleared(ctx context.Context) {
	m.providerErrorUpDownCounter.Add(ctx, -1)
}

func (m *Metrics) NotifyProviderDistance(ctx context.Context, peerID peer.ID, distance int64) {
	pidAttr := attribute.String("provider-id", peerID.String())
	m.distanceUpdateCounter.Add(ctx, 1, pidAttr)
	m.providerDistanceHistogram.Record(ctx, distance, pidAttr)
}

func (m *Metrics) reportProviderCount(_ context.Context, observer instrument.Int64Observer) error {
	observer.Observe(int64(m.pcache.Len()))
	return nil
}

func errKindAttribute(err error) attribute.KeyValue {
	// TODO check logs for other popular error kinds we might care about.
	var errKind string
	switch {
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
	return attribute.String("error-kind", errKind)
}

func (m *Metrics) Shutdown() error {
	return m.server.Shutdown(context.Background())
}
