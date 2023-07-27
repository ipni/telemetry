package telemetry

import (
	"context"
	"fmt"
	"io"
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
	mutex        sync.Mutex
	done         chan struct{}
	pcache       *pcache.ProviderCache
	updateIn     time.Duration
}

func New(adDepthLimit int64, updateIn time.Duration, pc *pcache.ProviderCache, met *metrics.Metrics) *Telemetry {
	tel := &Telemetry{
		adDepthLimit: adDepthLimit,
		dist:         make(map[peer.ID]int),
		done:         make(chan struct{}),
		metrics:      met,
		pcache:       pc,
		updateIn:     updateIn,
	}

	ctx, cancel := context.WithCancel(context.Background())
	tel.cancel = cancel
	go tel.run(ctx)

	return tel
}

func (tel *Telemetry) Close() {
	tel.cancel()
	<-tel.done
	tel.mutex.Lock()
	tel.pcache = nil
	tel.mutex.Unlock()
}

func (tel *Telemetry) run(ctx context.Context) {
	defer close(tel.done)

	var include, exclude map[peer.ID]struct{}
	errored := make(map[peer.ID]struct{})

	updates := dtrack.RunDistanceTracker(ctx, include, exclude, tel.pcache, tel.adDepthLimit, tel.updateIn)
	for update := range updates {
		if update.Err != nil {
			tel.metrics.NotifyProviderErrored(ctx, update.Err)
			log.Infow("Error getting distance", "provider", update.ID, "err", update.Err)
			errored[update.ID] = struct{}{}
			continue
		}
		if _, ok := errored[update.ID]; ok {
			tel.metrics.NotifyProviderErrorCleared(ctx)
			delete(errored, update.ID)
		}
		tel.metrics.NotifyProviderDistance(ctx, update.ID, int64(update.Distance))
		log.Infow("Distance update", "provider", update.ID, "distance", update.Distance)

		tel.mutex.Lock()
		tel.dist[update.ID] = update.Distance
		tel.mutex.Unlock()
	}
}

func (tel *Telemetry) ListProviders(ctx context.Context, w io.Writer) {
	tel.mutex.Lock()
	defer tel.mutex.Unlock()

	if tel.pcache == nil {
		return
	}

	provs := tel.pcache.List()
	for _, pinfo := range provs {
		tel.showProviderInfo(ctx, pinfo, w)
	}
}

func (tel *Telemetry) GetProvider(ctx context.Context, providerID peer.ID, w io.Writer) bool {
	tel.mutex.Lock()
	defer tel.mutex.Unlock()

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
		fmt.Fprintln(w, "    Distance:", tel.dist[pinfo.AddrInfo.ID])
	}
	fmt.Fprintln(w)
}