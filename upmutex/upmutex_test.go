package upmutex_test

import (
	"github.com/neilotoole/streamcache/upmutex"
	"sync"
	"testing"
)

func TestMu(t *testing.T) {

	mu := &upmutex.UpgradableRWMutex{}
	mu.UpgradableRLock()
	defer mu.UpgradableRUnlock()

	mu.UpgradeWLock()

}

func TestStd(t *testing.T) {
	mu := sync.RWMutex{}
	mu.RLock()
	mu.Lock()
}
