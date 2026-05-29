package http

import (
	"net/http"

	"github.com/dora-network/bond-trading-strategies/docs/openapi"
)

func (h *Handler) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(openapi.Spec) //nolint:errcheck
}
