package notifications_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dora-network/bond-trading-strategies/notifications"
)

func TestEventOrderUpdateConstant(t *testing.T) {
	t.Parallel()
	assert.Equal(t, notifications.EventType("dora.order_update"), notifications.EventOrderUpdate,
		"EventOrderUpdate must be the documented v2 DORA relay event type")
}
