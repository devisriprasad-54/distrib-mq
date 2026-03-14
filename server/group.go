package main

import (
    "fmt"
    "sort"
    "sync"
    "time"
)

type Consumer struct {
	id          string
	groupID     string
	partition   int
	lastSeen    time.Time
	conn        interface{ Write([]byte) (int, error) }
}

type ConsumerGroup struct {
	mu        sync.Mutex
	groupID   string
	consumers map[string]*Consumer // consumerID → Consumer
	partitions int
}

var (
	groups   = make(map[string]*ConsumerGroup)
	groupsMu sync.Mutex
)

func getGroup(groupID string) *ConsumerGroup {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if g, ok := groups[groupID]; ok {
		return g
	}

	g := &ConsumerGroup{
		groupID:    groupID,
		consumers:  make(map[string]*Consumer),
		partitions: DEFAULT_PARTITIONS,
	}
	groups[groupID] = g
	return g
}

func (g *ConsumerGroup) join(consumerID string, conn interface{ Write([]byte) (int, error) }) int {
	g.mu.Lock()
	defer g.mu.Unlock()

	// add consumer
	g.consumers[consumerID] = &Consumer{
		id:       consumerID,
		groupID:  g.groupID,
		lastSeen: time.Now(),
		conn:     conn,
	}

	// rebalance
	g.rebalance()

	return g.consumers[consumerID].partition
}

func (g *ConsumerGroup) rebalance() {
    ids := make([]string, 0, len(g.consumers))
    for id := range g.consumers {
        ids = append(ids, id)
    }

    // sort so assignment is always stable and predictable
    sort.Strings(ids)

    for i, id := range ids {
        g.consumers[id].partition = i % g.partitions
    }

    fmt.Printf("Rebalanced group %s: %d consumers, %d partitions\n", g.groupID, len(ids), g.partitions)
    for _, id := range ids {
        fmt.Printf("  Consumer %s → partition %d\n", id, g.consumers[id].partition)
    }
}

func (g *ConsumerGroup) heartbeat(consumerID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	c, ok := g.consumers[consumerID]
	if !ok {
		return false
	}
	c.lastSeen = time.Now()
	return true
}

func (g *ConsumerGroup) getPartition(consumerID string) (int, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	c, ok := g.consumers[consumerID]
	if !ok {
		return 0, false
	}
	return c.partition, true
}

func (g *ConsumerGroup) removeDead() {
	g.mu.Lock()
	defer g.mu.Unlock()

	changed := false
	for id, c := range g.consumers {
		if time.Since(c.lastSeen) > 60*time.Second {
			fmt.Printf("Consumer %s in group %s timed out, removing\n", id, g.groupID)
			delete(g.consumers, id)
			changed = true
		}
	}

	if changed {
		g.rebalance()
	}
}

// background goroutine — checks for dead consumers every 2 seconds
func startHealthChecker() {
	for {
		time.Sleep(2 * time.Second)
		groupsMu.Lock()
		for _, g := range groups {
			g.removeDead()
		}
		groupsMu.Unlock()
	}
}