package notifications

import "encoding/json"

// jsonMarshal is a tiny seam so tests can replace it if needed.
//
//nolint:gochecknoglobals
var jsonMarshal = json.Marshal

// jsonUnmarshal is a tiny seam so tests can replace it if needed.
//
//nolint:gochecknoglobals
var jsonUnmarshal = json.Unmarshal
