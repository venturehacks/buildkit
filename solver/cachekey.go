package solver

import (
	"sync"
	"time"

	digest "github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

// NewCacheKey creates a new cache key for a specific output index
func NewCacheKey(dgst digest.Digest, output Index) *CacheKey {
	return &CacheKey{
		ID:     rootKey(dgst, output).String(),
		digest: dgst,
		output: output,
		ids:    map[*cacheManager]string{},
	}
}

// CacheKeyWithSelector combines a cache key with an optional selector digest.
// Used to limit the matches for dependency cache key.
type CacheKeyWithSelector struct {
	Selector digest.Digest
	CacheKey ExportableCacheKey
}

type CacheKey struct {
	mu sync.RWMutex

	ID     string
	deps   [][]CacheKeyWithSelector // only [][]*inMemoryCacheKey
	digest digest.Digest
	output Index
	ids    map[*cacheManager]string

	indexIDs []string
}

func (ck *CacheKey) Deps() [][]CacheKeyWithSelector {
	start := time.Now()
	ck.mu.RLock()
	elapsed := time.Since(start)
	if elapsed.Milliseconds() > 1 {
		logrus.Infof("(ck *CacheKey) Deps(): ck.mu.RLock() for %s took more than a millisecond: %s", ck.digest, elapsed)
	}
	defer ck.mu.RUnlock()
	deps := make([][]CacheKeyWithSelector, len(ck.deps))
	for i := range ck.deps {
		deps[i] = append([]CacheKeyWithSelector(nil), ck.deps[i]...)
	}
	return deps
}

func (ck *CacheKey) Digest() digest.Digest {
	return ck.digest
}
func (ck *CacheKey) Output() Index {
	return ck.output
}

func (ck *CacheKey) clone() *CacheKey {
	nk := &CacheKey{
		ID:     ck.ID,
		digest: ck.digest,
		output: ck.output,
		ids:    map[*cacheManager]string{},
	}
	for cm, id := range ck.ids {
		nk.ids[cm] = id
	}
	return nk
}
