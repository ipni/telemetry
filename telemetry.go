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

// Number of buckest is 3: small, medium, large
const nbuckets = 3

// Time to clear any errors not cleared by update.
const errorRefreshTime = 5 * time.Minute

type Telemetry struct {
	adDepthLimit int64
	cancel       context.CancelFunc
	dist         map[peer.ID]int64
	metrics      *metrics.Metrics
	rwmutex      sync.RWMutex
	done         chan struct{}
	pcache       *pcache.ProviderCache

	indexerURLs []*url.URL
	nSlowest    int
	slowRate    int64

	distBuckets   []int64
	bucketIndexes map[peer.ID]int
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
		dist:         map[peer.ID]int64{},
		done:         make(chan struct{}),
		metrics:      met,
		pcache:       pc,

		indexerURLs: indexerURLs,
		nSlowest:    nSlowest,
		slowRate:    int64(slowRate),

		distBuckets:   make([]int64, 3),
		bucketIndexes: map[peer.ID]int{},
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

	errTimer := time.NewTimer(errorRefreshTime)
	defer errTimer.Stop()

	errored := make(map[peer.ID]error)
	rateMap := map[peer.ID]*metrics.IngestRate{}
	var distSum int64

	for update := range updates {
		if update.Err != nil {
			log.Infow("Error getting distance", "provider", update.ID, "err", update.Err)
			if prevErr, ok := errored[update.ID]; ok {
				if prevErr.Error() != update.Err.Error() {
					tel.metrics.NotifyProviderErrorCleared(ctx, prevErr)
					errored[update.ID] = update.Err
					tel.metrics.NotifyProviderErrored(ctx, update.Err)
					log.Infow("Provider error updated", "provider", update.ID, "totalErroring", len(errored))
				}
			} else {
				errored[update.ID] = update.Err
				tel.metrics.NotifyProviderErrored(ctx, update.Err)
				log.Infow("Provider marked as erroring", "provider", update.ID, "totalErroring", len(errored))
			}
			prevLen := len(rateMap)
			delete(rateMap, update.ID)
			if len(rateMap) != prevLen {
				tel.updateIngestRates(rateMap)
			}
			distSum = tel.removeProviderFromDistBucket(ctx, update.ID, distSum)
		} else {
			if err, ok := errored[update.ID]; ok {
				tel.metrics.NotifyProviderErrorCleared(ctx, err)
				delete(errored, update.ID)
				log.Infow("Provider error cleared", "provider", update.ID, "totalErroring", len(errored))
			}
			distSum = tel.updateDistBuckets(ctx, update.ID, int64(update.Distance), distSum)
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

		select {
		case <-errTimer.C:
			tel.refreshErrors(ctx, errored)
			errTimer.Reset(errorRefreshTime)
		default:
		}
	}
}

func (tel *Telemetry) refreshErrors(ctx context.Context, errored map[peer.ID]error) {
	provs := tel.pcache.List()
	for _, pinfo := range provs {
		if pinfo.LastError == "" {
			if err, ok := errored[pinfo.AddrInfo.ID]; ok {
				tel.metrics.NotifyProviderErrorCleared(ctx, err)
				delete(errored, pinfo.AddrInfo.ID)
			}
		}
	}
}

func (tel *Telemetry) updateDistBuckets(ctx context.Context, peerID peer.ID, distance, distSum int64) int64 {
	if distance == -1 {
		log.Infow("Distance update", "provider", peerID, "distanceExceeds", tel.adDepthLimit)
		distance = tel.adDepthLimit
	} else {
		log.Infow("Distance update", "provider", peerID, "distance", distance)
	}

	bucketIndex, ok := tel.bucketIndexes[peerID]
	if ok {
		tel.distBuckets[bucketIndex]--
	}
	// bucketIndex = distance / bucketSize
	bucketIndex = int(distance * nbuckets / tel.adDepthLimit)
	if bucketIndex >= len(tel.distBuckets) {
		bucketIndex = len(tel.distBuckets) - 1
	}
	tel.distBuckets[bucketIndex]++
	tel.bucketIndexes[peerID] = bucketIndex

	tel.rwmutex.Lock()
	prevDist := tel.dist[peerID]
	if prevDist != distance {
		tel.dist[peerID] = distance
		distSum += distance - prevDist
	}
	tel.rwmutex.Unlock()

	tel.metrics.UpdateProviderDistanceBuckets(ctx, tel.distBuckets[0], tel.distBuckets[1], tel.distBuckets[2], distSum)
	return distSum
}

func (tel *Telemetry) removeProviderFromDistBucket(ctx context.Context, peerID peer.ID, distSum int64) int64 {
	bucketIndex, ok := tel.bucketIndexes[peerID]
	if ok {
		tel.distBuckets[bucketIndex]--
		delete(tel.bucketIndexes, peerID)
	}

	tel.rwmutex.Lock()
	prevDist, ok := tel.dist[peerID]
	if ok {
		distSum -= prevDist
		delete(tel.dist, peerID)
	}
	tel.rwmutex.Unlock()

	tel.metrics.UpdateProviderDistanceBuckets(ctx, tel.distBuckets[0], tel.distBuckets[1], tel.distBuckets[2], distSum)
	return distSum
}

func (tel *Telemetry) updateIngestRates(rateMap map[peer.ID]*metrics.IngestRate) {
	rates := make([]*metrics.IngestRate, len(rateMap))
	var i, slowCount int
	var totalMhPerSec int64
	for _, ingestRate := range rateMap {
		rates[i] = ingestRate
		i++
		mhPerSec := ingestRate.MhPerSec
		if mhPerSec <= tel.slowRate {
			slowCount++
		}
		totalMhPerSec += mhPerSec
	}

	sort.Sort(metrics.SortableRates(rates))

	var slowest []*metrics.IngestRate
	if len(rates) > tel.nSlowest {
		slowest = rates[:tel.nSlowest]
	} else {
		slowest = rates
	}
	log.Infof("Slow providers (below %d mh/sec): %d", tel.slowRate, slowCount)
	var avgMhPerSec, fastestMhPerSec, slowestMhPerSec int64
	if len(rates) != 0 {
		r := rates[len(rates)-1]
		fastestMhPerSec = r.MhPerSec
		log.Infow("Fastest", "provider", r.ProviderID, "mh/sec", r.MhPerSec, "avgMhPerAd", r.AvgMhPerAd)
		r = rates[0]
		slowestMhPerSec = r.MhPerSec
		log.Infow("Slowest", "provider", r.ProviderID, "mh/sec", r.MhPerSec, "avgMhPerAd", r.AvgMhPerAd)
		avgMhPerSec = totalMhPerSec / int64(len(rateMap))
		log.Infof("Average ingest rate: %d mh/sec", avgMhPerSec)
	}
	tel.metrics.UpdateIngestRates(slowest, slowCount, avgMhPerSec, fastestMhPerSec, slowestMhPerSec)
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
