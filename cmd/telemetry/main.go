package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/pcache"
	"github.com/ipni/telemetry"
	"github.com/ipni/telemetry/metrics"
	"github.com/ipni/telemetry/server"

	"net/http"
	_ "net/http/pprof"
)

var log = logging.Logger("telemetry/cmd")

type arrayFlags []string

func (a *arrayFlags) String() string {
	return strings.Join(*a, ", ")
}

func (a *arrayFlags) Set(value string) error {
	*a = append(*a, value)
	return nil
}

const (
	defaultUpdateInterval = "2m"
	defaultUpdateTimeout  = "5m"
)

func main() {
	var (
		indexerURLs    arrayFlags
		listenAddr     string
		logLevel       string
		maxDepth       int
		metricsAddr    string
		nSlowest       int
		providersURLs  arrayFlags
		updateInterval string
		updateTimeout  string
		showVersion    bool
		slowRate       int
		topic          string
	)
	flag.Var(&indexerURLs, "indexerURL", "indexer admin URL to get ingestion rate from. Multiple OK")
	flag.StringVar(&listenAddr, "listenAddr", "0.0.0.0:40080", "telemetry server address:port to listen on")
	flag.StringVar(&logLevel, "logLevel", "info", "logging level applied if GOLOG_LOG_LEVEL is not set")
	flag.IntVar(&maxDepth, "maxDepth", 5000, "advertisement chain depth limit")
	flag.StringVar(&metricsAddr, "metricsAddr", "0.0.0.0:40081", "telemetry metrics server address:port to listen on")
	flag.IntVar(&slowRate, "slowRate", 250, "rate at which ingestion is considered slow in mh/sec (default 250)")
	flag.IntVar(&nSlowest, "nSlowest", 10, "number of slowest indexers to track ingestion rates for")
	flag.Var(&providersURLs, "providersURL", "URL to get provider infomation. Multiple OK")
	flag.StringVar(&updateInterval, "updateIn", defaultUpdateInterval, "update interval. Integer string ending in 's', 'm', or 'h'.")
	flag.StringVar(&updateTimeout, "updateTimeout", defaultUpdateTimeout, "update timeout. Integer string ending in 's', 'm', or 'h'.")
	flag.BoolVar(&showVersion, "version", false, "print version")
	flag.StringVar(&topic, "topic", "", "topic used to fetch head advertisement with graphsync, specify only if using non-standard topic")
	flag.Parse()

	if showVersion {
		fmt.Println(telemetry.Version)
		return
	}

	go http.ListenAndServe(":8080", nil)

	// Disable logging that happens in packages such as data-transfer.
	_ = logging.SetLogLevel("*", "fatal")

	if _, set := os.LookupEnv("GOLOG_LOG_LEVEL"); !set {
		_ = logging.SetLogLevel("telemetry", logLevel)
		_ = logging.SetLogLevel("telemetry/cmd", logLevel)
		_ = logging.SetLogLevel("telemetry/metrics", logLevel)
		_ = logging.SetLogLevel("telemetry/server", logLevel)
	}

	if len(indexerURLs) == 0 {
		log.Fatal("One or more indexer admin URLs must be specified")
		return
	}

	updateIn, err := time.ParseDuration(updateInterval)
	if err != nil {
		log.Errorf("Invalid update interval %s, using default %s", updateIn, defaultUpdateInterval)
		updateIn, err = time.ParseDuration(defaultUpdateInterval)
		if err != nil {
			panic(err)
		}
	}

	updateTo, err := time.ParseDuration(updateTimeout)
	if err != nil {
		log.Errorf("Invalid update timeout %s, using default %s", updateTimeout, defaultUpdateTimeout)
		updateTo, err = time.ParseDuration(defaultUpdateTimeout)
		if err != nil {
			panic(err)
		}
	}

	if len(providersURLs) == 0 {
		log.Fatal("One or more providers URLs must be specified")
		return
	}
	pc, err := pcache.New(pcache.WithSourceURL(providersURLs...), pcache.WithRefreshInterval(0))
	if err != nil {
		log.Fatalw("Failed to create provider cache", "err", err)
		return
	}

	mets := metrics.New(metricsAddr, pc)
	err = mets.Start(slowRate, nSlowest)
	if err != nil {
		log.Fatalw("Failed to start metrics server", "err", err)
		return
	}

	tel, err := telemetry.New(int64(maxDepth), updateIn, updateTo, pc, mets, indexerURLs, slowRate, nSlowest, topic)
	if err != nil {
		log.Fatalw("Failed to telemetry service", "err", err)
		return
	}

	svr, err := server.New(listenAddr, tel)
	if err != nil {
		log.Fatalw("Failed to create server", "err", err)
		return
	}

	if err = svr.Start(); err != nil {
		log.Fatalw("Failed to start server", "err", err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	log.Info("Terminating...")
	if err = svr.Close(); err != nil {
		log.Warnw("Failure occurred while shutting down server.", "err", err)
	} else {
		log.Info("Shut down server successfully.")
	}
	tel.Close()
	if err = mets.Shutdown(); err != nil {
		log.Warnw("Failure occurred while shutting down metrics server.", "err", err)
	} else {
		log.Info("Shut down metrics server successfully.")
	}
}
