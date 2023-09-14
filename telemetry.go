package telemetry

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/pcache"
	"github.com/ipni/ipni-cli/pkg/dtrack"
	"github.com/ipni/telemetry/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("telemetry")

type Telemetry struct {
	adDepthLimit int64
	cancel       context.CancelFunc
	dist         map[peer.ID]int
	metrics      *metrics.Metrics
	rwmutex      sync.RWMutex
	done         chan struct{}
	pcache       *pcache.ProviderCache

	indexerURLs []*url.URL
	nSlowest    int
	slowRate    int64
}

func New(adDepthLimit int64, updateIn, updateTo time.Duration, pc *pcache.ProviderCache, met *metrics.Metrics, indexerAdminURLs []string, slowRate, nSlowest int, topic string) (*Telemetry, error) {
	indexerURLs := make([]*url.URL, len(indexerAdminURLs))
	for i, urlStr := range indexerAdminURLs {
		var err error
		indexerURLs[i], err = parseIndexerBaseURL(urlStr)
		if err != nil {
			return nil, err
		}
	}

	tel := &Telemetry{
		adDepthLimit: adDepthLimit,
		dist:         make(map[peer.ID]int),
		done:         make(chan struct{}),
		metrics:      met,
		pcache:       pc,

		indexerURLs: indexerURLs,
		nSlowest:    nSlowest,
		slowRate:    int64(slowRate),
	}

	var include, exclude map[peer.ID]struct{}

	ctx, cancel := context.WithCancel(context.Background())
	updates, err := dtrack.RunDistanceTracker(ctx, include, exclude, pc, updateIn, updateTo,
		dtrack.WithDepthLimit(adDepthLimit), dtrack.WithTopic(topic))
	if err != nil {
		cancel()
		return nil, err
	}
	tel.cancel = cancel

	go tel.run(ctx, updates)

	return tel, nil
}

func (tel *Telemetry) Close() {
	tel.cancel()
	<-tel.done
	tel.rwmutex.Lock()
	tel.pcache = nil
	tel.rwmutex.Unlock()
}

func (tel *Telemetry) run(ctx context.Context, updates <-chan dtrack.DistanceUpdate) {
	defer close(tel.done)

	errored := make(map[peer.ID]error)
	rateMap := map[peer.ID]*metrics.IngestRate{}

	for update := range updates {
		if update.Err != nil {
			log.Infow("Error getting distance", "provider", update.ID, "err", update.Err)
			if _, ok := errored[update.ID]; !ok {
				tel.metrics.NotifyProviderErrored(ctx, update.Err)
				errored[update.ID] = update.Err
				_, ok := rateMap[update.ID]
				if ok {
					delete(rateMap, update.ID)
					tel.updateIngestRates(rateMap)
				}
			}
		} else {
			if err, ok := errored[update.ID]; ok {
				tel.metrics.NotifyProviderErrorCleared(ctx, err)
				delete(errored, update.ID)
			}

			tel.rwmutex.Lock()
			tel.dist[update.ID] = update.Distance
			tel.rwmutex.Unlock()

			if update.Distance == -1 {
				tel.metrics.NotifyProviderDistance(ctx, update.ID, tel.adDepthLimit)
				log.Infow("Distance update", "provider", update.ID, "distanceExceeds", tel.adDepthLimit)
			} else {
				tel.metrics.NotifyProviderDistance(ctx, update.ID, int64(update.Distance))
				log.Infow("Distance update", "provider", update.ID, "distance", update.Distance)
			}
		}

		// Get latest ingestion rate from indexer.
		irate, ok, err := getIndexerTelemetry(ctx, tel.indexerURLs, update.ID)
		if err != nil {
			log.Errorw("Cannot get ingest rate information for provider", "err", err, "provider", update.ID)
			continue
		}
		if !ok {
			// No new rate data since last query,
			continue
		}

		ingestRate := &metrics.IngestRate{
			AvgMhPerAd: int64(irate.Count) / int64(irate.Samples),
			MhPerSec:   int64(float64(irate.Count) / irate.Elapsed.Seconds()),
			ProviderID: update.ID,
		}
		rateMap[update.ID] = ingestRate
		tel.updateIngestRates(rateMap)
	}
}

func (tel *Telemetry) updateIngestRates(rateMap map[peer.ID]*metrics.IngestRate) {
	rates := make([]*metrics.IngestRate, len(rateMap))
	var i, slowCount int
	var totalMhPerSec int64
	for _, ingestRate := range rateMap {
		rates[i] = ingestRate
		i++
		if ingestRate.MhPerSec <= tel.slowRate {
			slowCount++
		}
		totalMhPerSec += ingestRate.MhPerSec
	}

	sort.Sort(metrics.SortableRates(rates))

	var slowest []*metrics.IngestRate
	if len(rates) > tel.nSlowest {
		slowest = rates[:tel.nSlowest]
	} else {
		slowest = rates
	}
	log.Infof("Slow providers (below %d mh/sec): %d", tel.slowRate, slowCount)
	var avgMhPerSec int64
	if len(rates) != 0 {
		r := rates[len(rates)-1]
		log.Infow("Fastest", "provider", r.ProviderID, "mh/sec", r.MhPerSec, "avgMhPerAd", r.AvgMhPerAd)
		r = rates[0]
		log.Infow("Slowest", "provider", r.ProviderID, "mh/sec", r.MhPerSec, "avgMhPerAd", r.AvgMhPerAd)
		avgMhPerSec = totalMhPerSec / int64(len(rateMap))
		log.Infof("Average ingest rate: %d mh/sec", avgMhPerSec)
	}
	tel.metrics.UpdateIngestRates(slowest, slowCount, avgMhPerSec)
}

func (tel *Telemetry) ListProviders(ctx context.Context, w io.Writer) {
	tel.rwmutex.RLock()
	defer tel.rwmutex.RUnlock()

	if tel.pcache == nil {
		return
	}

	provs := tel.pcache.List()
	for _, pinfo := range provs {
		tel.showProviderInfo(ctx, pinfo, w)
	}
}

func (tel *Telemetry) GetProvider(ctx context.Context, providerID peer.ID, w io.Writer) bool {
	tel.rwmutex.RLock()
	defer tel.rwmutex.RUnlock()

	if tel.pcache == nil {
		return false
	}

	prov, err := tel.pcache.Get(ctx, providerID)
	if err != nil || prov == nil {
		return false
	}

	tel.showProviderInfo(ctx, prov, w)
	return true
}

func (tel *Telemetry) showProviderInfo(ctx context.Context, pinfo *model.ProviderInfo, w io.Writer) {
	fmt.Fprintln(w, "Provider", pinfo.AddrInfo.ID)
	var timeStr string
	if pinfo.LastAdvertisement.Defined() {
		timeStr = pinfo.LastAdvertisementTime
	}
	fmt.Fprintln(w, "    LastAdvertisementTime:", timeStr)
	if pinfo.Publisher != nil {
		fmt.Fprintln(w, "    Publisher:", pinfo.Publisher.ID)
		fmt.Fprintln(w, "        Publisher Addrs:", pinfo.Publisher.Addrs)
	} else {
		fmt.Fprintln(w, "    Publisher: none")
	}
	if pinfo.Inactive {
		fmt.Fprintln(w, "    Inactive: true")
	}
	if pinfo.LastError != "" {
		fmt.Fprintln(w, "    LastError:", pinfo.LastError)
	} else {
		dist, ok := tel.dist[pinfo.AddrInfo.ID]
		if ok {
			if dist == -1 {
				fmt.Fprintf(w, "    Distance: exceeded limit %d+", tel.adDepthLimit)
			} else {
				fmt.Fprintln(w, "    Distance:", dist)
			}
		} else {
			fmt.Fprintf(w, "    Distance: unknown")
		}
	}
	fmt.Fprintln(w)
}
