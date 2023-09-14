package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const telemetryPath = "/telemetry/providers"

type Rate struct {
	Count   uint64
	Elapsed time.Duration
	Samples int
}

func parseIndexerBaseURL(baseURL string) (*url.URL, error) {
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = telemetryPath
	return u, nil
}

func getIndexerTelemetry(ctx context.Context, baseURLs []*url.URL, providerID peer.ID) (Rate, bool, error) {
	for _, baseURL := range baseURLs {
		r, ok, err := getOneIndexerTelemetry(ctx, baseURL, providerID)
		if err != nil {
			return Rate{}, false, err
		}
		if ok {
			return r, true, nil
		}
	}
	return Rate{}, false, nil
}

func getOneIndexerTelemetry(ctx context.Context, baseURL *url.URL, providerID peer.ID) (Rate, bool, error) {
	u := baseURL.JoinPath(providerID.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Rate{}, false, err
	}
	c := http.DefaultClient

	resp, err := c.Do(req)
	if err != nil {
		return Rate{}, false, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Rate{}, false, err
	}

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNoContent:
		return Rate{}, false, nil
	default:
		return Rate{}, false, fmt.Errorf("%d %s", resp.StatusCode, resp.Status)
	}

	var ingestRate Rate
	err = json.Unmarshal(body, &ingestRate)
	if err != nil {
		return Rate{}, false, err
	}

	return ingestRate, true, nil
}
