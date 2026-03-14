package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DEFAULT_PARTITIONS = 3

var (
	// key: "topic-partition" → Log
	logs   = make(map[string]*Log)
	logsMu sync.Mutex
)

func getLog(topic string, partition int) (*Log, error) {
	logsMu.Lock()
	defer logsMu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if l, ok := logs[key]; ok {
		return l, nil
	}

	l, err := NewLog(topic, partition)
	if err != nil {
		return nil, err
	}
	logs[key] = l
	return l, nil
}

// consistent hashing — same key always returns same partition
func getPartition(key string, totalPartitions int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % totalPartitions
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 4)

		if len(parts) < 2 {
			fmt.Fprintln(conn, "ERROR invalid command")
			continue
		}

		command := parts[0]
		topic := parts[1]

		switch command {

		case "SEND":
			// SEND <topic> <key> <message>
			if len(parts) < 4 {
				fmt.Fprintln(conn, "ERROR usage: SEND <topic> <key> <message>")
				continue
			}
			key := parts[2]
			message := parts[3]

			partition := getPartition(key, DEFAULT_PARTITIONS)
			l, err := getLog(topic, partition)
			if err != nil {
				fmt.Fprintln(conn, "ERROR", err)
				continue
			}
			offset, err := l.Append(message)
			if err != nil {
				fmt.Fprintln(conn, "ERROR", err)
				continue
			}
			fmt.Fprintln(conn, "OK", partition, offset)

		case "READ":
			// READ <topic> <partition> <offset>
			if len(parts) < 4 {
				fmt.Fprintln(conn, "ERROR usage: READ <topic> <partition> <offset>")
				continue
			}
			partition, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Fprintln(conn, "ERROR invalid partition")
				continue
			}
			offset, err := strconv.Atoi(parts[3])
			if err != nil {
				fmt.Fprintln(conn, "ERROR invalid offset")
				continue
			}
			l, err := getLog(topic, partition)
			if err != nil {
				fmt.Fprintln(conn, "ERROR", err)
				continue
			}
			msg, err := l.Read(offset)
			if err != nil {
				fmt.Fprintln(conn, "EMPTY")
				continue
			}
			fmt.Fprintln(conn, "MSG", msg)

		case "PARTITIONS":
			// PARTITIONS <topic>
			fmt.Fprintln(conn, "OK", DEFAULT_PARTITIONS)
		case "JOIN":
			// JOIN <groupID> <consumerID>
			if len(parts) < 3 {
				fmt.Fprintln(conn, "ERROR usage: JOIN <groupID> <consumerID>")
				continue
			}
			groupID := parts[1]
			consumerID := parts[2]

			g := getGroup(groupID)
			partition := g.join(consumerID, conn)
			fmt.Fprintln(conn, "ASSIGNED", partition)

		case "PING":
		// PING <groupID> <consumerID>
		if len(parts) < 3 {
			fmt.Fprintln(conn, "ERROR usage: PING <groupID> <consumerID>")
			continue
		}
		groupID := parts[1]
		consumerID := parts[2]

		g := getGroup(groupID)
		if g.heartbeat(consumerID) {
			fmt.Fprintln(conn, "PONG")
		} else {
			fmt.Fprintln(conn, "ERROR consumer not found")
		}

		case "CONSUME":
		// CONSUME <groupID> <consumerID> <topic>
		if len(parts) < 4 {
			fmt.Fprintln(conn, "ERROR usage: CONSUME <groupID> <consumerID> <topic>")
			continue
		}
		groupID := parts[1]
		consumerID := parts[2]
		topic := parts[3]

		g := getGroup(groupID)
		partition, ok := g.getPartition(consumerID)
		if !ok {
			fmt.Fprintln(conn, "ERROR consumer not in group, JOIN first")
			continue
		}

		l, err := getLog(topic, partition)
		if err != nil {
			fmt.Fprintln(conn, "ERROR", err)
			continue
		}

			// read next unread message for this consumer
			// for now read from offset 0 — we'll track per consumer offset next
			msg, err := l.Read(0)
			if err != nil {
		fmt.Fprintln(conn, "EMPTY")
		continue
		}
		fmt.Fprintln(conn, "MSG", msg)
			default:
				fmt.Fprintln(conn, "ERROR unknown command")
			}
		}
}
func startReplication(leaderAddr string) {
	fmt.Println("Starting replication from", leaderAddr)

	// track our offset per partition so we know what to ask for next
	followerOffsets := make(map[string]int)

	for {
		// connect to leader
		conn, err := net.Dial("tcp", leaderAddr)
		if err != nil {
			fmt.Println("Cannot reach leader, retrying in 2s...")
			time.Sleep(2 * time.Second)
			continue
		}

		replicate(conn, followerOffsets)
		conn.Close()

		// if replicate returns, connection dropped — retry
		fmt.Println("Lost connection to leader, retrying in 2s...")
		time.Sleep(2 * time.Second)
	}
}

func replicate(conn net.Conn, followerOffsets map[string]int) {
	scanner := bufio.NewScanner(conn)

	// topics and partitions to replicate
	// in production this comes from a metadata server
	// for now we hardcode orders with 3 partitions
	topics := []string{"orders"}
	partitions := []int{0, 1, 2}

	for {
		for _, topic := range topics {
			for _, partition := range partitions {
				key := fmt.Sprintf("%s-%d", topic, partition)
				offset := followerOffsets[key]

				// ask leader for next message
				fmt.Fprintf(conn, "READ %s %d %d\n", topic, partition, offset)

				if !scanner.Scan() {
					return // connection dropped
				}

				response := scanner.Text()
				parts := strings.SplitN(response, " ", 2)

				if parts[0] == "MSG" {
					message := parts[1]

					// write to our local log
					l, err := getLog(topic, partition)
					if err != nil {
						fmt.Println("Replication error:", err)
						continue
					}
					l.Append(message)
					followerOffsets[key]++
					fmt.Printf("Replicated %s partition %d offset %d\n", topic, partition, offset)
				}
				// if EMPTY — nothing new, move on
			}
		}
		time.Sleep(100 * time.Millisecond) // poll every 100ms
	}
}