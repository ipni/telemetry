package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/telemetry"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("telemetry/server")

type Server struct {
	server   *http.Server
	listener net.Listener
	tel      *telemetry.Telemetry
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

func New(listen string, tel *telemetry.Telemetry) (*Server, error) {
	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	server := &http.Server{
		Handler: mux,
	}
	s := &Server{
		server:   server,
		listener: l,
		tel:      tel,
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	})
	mux.HandleFunc("/providers", s.listProviders)
	mux.HandleFunc("/providers/", s.getProvider)
	mux.HandleFunc("/ready", s.handleReady)

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("http server listening", "listenAddr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) Close() error {
	return s.server.Shutdown(context.Background())
}

func (s *Server) listProviders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	s.tel.ListProviders(r.Context(), w)
}

func (s *Server) getProvider(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	providerID, err := peer.Decode(path.Base(r.URL.Path))
	if err != nil {
		err = fmt.Errorf("cannot decode provider id: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	s.tel.GetProvider(r.Context(), providerID, w)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Cache-Control", "no-cache")
	http.Error(w, telemetry.Version, http.StatusOK)
}
