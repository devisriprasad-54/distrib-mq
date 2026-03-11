package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

var (
	logs   = make(map[string]*Log)
	logsMu sync.Mutex
)

func getLog(topic string) (*Log, error) {
	logsMu.Lock()
	defer logsMu.Unlock()

	if l, ok := logs[topic]; ok {
		return l, nil
	}

	l, err := NewLog(topic)
	if err != nil {
		return nil, err
	}
	logs[topic] = l
	return l, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 3)

		if len(parts) < 2 {
			fmt.Fprintln(conn, "ERROR invalid command")
			continue
		}

		command := parts[0]
		topic := parts[1]

		switch command {

		case "SEND":
			if len(parts) < 3 {
				fmt.Fprintln(conn, "ERROR missing message")
				continue
			}
			message := parts[2]
			l, err := getLog(topic)
			if err != nil {
				fmt.Fprintln(conn, "ERROR", err)
				continue
			}
			offset, err := l.Append(message)
			if err != nil {
				fmt.Fprintln(conn, "ERROR", err)
				continue
			}
			fmt.Fprintln(conn, "OK", offset)

		case "READ":
			if len(parts) < 3 {
				fmt.Fprintln(conn, "ERROR missing offset")
				continue
			}
			offset, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Fprintln(conn, "ERROR invalid offset")
				continue
			}
			l, err := getLog(topic)
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

		default:
			fmt.Fprintln(conn, "ERROR unknown command")
		}
	}
}