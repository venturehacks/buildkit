package cacheimport

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"
)

func (it *item) Self() string {
	return it.c.Self()
}

func (c *CacheChains) Self() string {
	return fmt.Sprintf("cc-%p", c)
}

func (c *CacheChains) checkCacheChainsCoherence() {
	mixedItems := 0
	mixedBacklinks := 0
	mixedLinks := 0
	for _, v := range c.items {
		if v.Self() != c.Self() {
			mixedItems++
			logrus.Errorf("checkCacheChainsCoherence(): original CacheChains: %s, foreign ITEM CacheChains: %s, Digest: %s", c.Self(), v.Self(), v.dgst.String())
		}

		for bl := range v.backlinks {
			if bl.Self() != c.Self() {
				mixedBacklinks++
				logrus.Errorf("checkCacheChainsCoherence(): original CacheChains: %s, foreign BACKLINK CacheChains: %s, Digest: %s", c.Self(), bl.Self(), v.dgst.String())
			}
		}
		for _, l := range v.links {
			for k := range l {
				if k.src.Self() != c.Self() {
					mixedLinks++
					logrus.Errorf("checkCacheChainsCoherence(): original CacheChains: %s, foreign LINK CacheChains: %s, Digest: %s", c.Self(), k.src.Self(), v.dgst.String())
				}
			}
		}
	}
	if mixedItems > 0 || mixedBacklinks > 0 || mixedLinks > 0 {
		logrus.Errorf("checkCacheChainsCoherence(): corrupted CacheChains: %s, ITEMS: %d, BACKLINKS: %d, LINKS: %d, TOTAL: %d", c.Self(), mixedItems, mixedBacklinks, mixedLinks, len(c.items))
	}
	logrus.Infof("checkCacheChainsCoherence(): CacheChains %s contains %d items", c.Self(), len(c.items))
}

type dumpableLink struct {
	Selector     string
	Source       string
	SourceDigest string
}

type dumpableBacklink struct {
	Address string
	Digest  string
}

type dumpableItem struct {
	Address       string
	Digest        string
	CacheChainRef string
	ResultRef     string
	Invalid       bool
	Links         map[int][]dumpableLink
	BackLinks     []dumpableBacklink
	ResultTime    string
}

func (c *CacheChains) String() string {
	items := []dumpableItem{}
	for _, it := range c.items {
		r := dumpableItem{
			Address:       fmt.Sprintf("%p", it),
			Digest:        it.dgst.String(),
			CacheChainRef: fmt.Sprintf("%p", it.c),
			ResultRef:     fmt.Sprintf("%p", it.result),
			Invalid:       it.invalid,
			Links:         map[int][]dumpableLink{},
			BackLinks:     []dumpableBacklink{},
			ResultTime:    it.resultTime.String(),
		}

		for i, m := range it.links {
			r.Links[i] = []dumpableLink{}
			for k := range m {
				r.Links[i] = append(r.Links[i], dumpableLink{
					Selector:     k.selector,
					Source:       fmt.Sprintf("%p", k.src),
					SourceDigest: k.src.dgst.String(),
				})
			}
		}
		for k := range it.backlinks {
			r.BackLinks = append(r.BackLinks, dumpableBacklink{
				Address: fmt.Sprintf("%p", k),
				Digest:  k.dgst.String(),
			})
		}

		sort.Slice(r.BackLinks, func(i, j int) bool {
			return r.BackLinks[i].Digest < r.BackLinks[j].Digest
		})

		items = append(items, r)
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Digest == items[j].Digest {
			return items[i].ResultTime < items[j].ResultTime
		}
		return items[i].Digest < items[j].Digest
	})

	out, _ := json.Marshal(items)
	return string(out)
}
