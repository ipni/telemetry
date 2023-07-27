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
	defaultUpdateIn = "2m"
)

func main() {
	var (
		listenAddr     string
		logLevel       string
		maxDepth       int
		metricsAddr    string
		providersURLs  arrayFlags
		updateInterval string
		showVersion    bool
	)
	flag.StringVar(&listenAddr, "listenAddr", "0.0.0.0:8080", "telemetry server address:port to listen on")
	flag.StringVar(&logLevel, "logLevel", "info", "logging level applied if GOLOG_LOG_LEVEL is not set")
	flag.IntVar(&maxDepth, "maxDepth", 5000, "advertisement chain depth limit")
	flag.StringVar(&metricsAddr, "metricsAddr", "0.0.0.0:8081", "telemetry metrics server address:port to listen on")
	flag.Var(&providersURLs, "providersURL", "URL to get provider infomation. Multiple OK")
	flag.StringVar(&updateInterval, "updateIn", defaultUpdateIn, "update interval. Integr string ending in 's', 'm', or 'h'.")
	flag.BoolVar(&showVersion, "version", false, "print version")
	flag.Parse()

	if showVersion {
		fmt.Println(telemetry.Version)
		return
	}

	// Disable logging that happens in packages such as data-transfer.
	_ = logging.SetLogLevel("*", "fatal")

	if _, set := os.LookupEnv("GOLOG_LOG_LEVEL"); !set {
		_ = logging.SetLogLevel("telemetry", logLevel)
		_ = logging.SetLogLevel("telemetry/cmd", logLevel)
		_ = logging.SetLogLevel("telemetry/metrics", logLevel)
		_ = logging.SetLogLevel("telemetry/server", logLevel)
	}

	updateIn, err := time.ParseDuration(updateInterval)
	if err != nil {
		log.Errorf("Invalid update interval %s, using default %s", updateIn, defaultUpdateIn)
		updateIn, err = time.ParseDuration(defaultUpdateIn)
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
	err = mets.Start()
	if err != nil {
		log.Fatalw("Failed to start metrics server", "err", err)
		return
	}

	tel := telemetry.New(int64(maxDepth), updateIn, pc, mets)

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
