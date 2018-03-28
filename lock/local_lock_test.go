package lock

import(
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalLock(t *testing.T) {
	var l = NewLocalLock()

	assert.Nil(t, l.Lock())
	assert.NotNil(t, l.Lock())
	assert.Nil(t, l.Unlock())
	assert.Nil(t, l.Unlock())
}