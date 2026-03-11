package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"net"
	"strconv"
	"strings"
	"sync"
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

		default:
			fmt.Fprintln(conn, "ERROR unknown command")
		}
	}
}