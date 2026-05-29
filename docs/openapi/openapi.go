// Package openapi exposes the strategy-server OpenAPI specification as embedded
// Go data, accessible to any package in this module without runtime file I/O.
package openapi

import _ "embed"

//go:embed strategy-server.json
var Spec []byte
