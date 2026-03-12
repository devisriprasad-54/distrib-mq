package main

import (
	"flag"
	"fmt"
	"net"
)

var (
	brokerID   int
	port       string
	leaderAddr string
)

func main() {
	flag.IntVar(&brokerID, "id", 0, "Broker ID")
	flag.StringVar(&port, "port", "9090", "Port to listen on")
	flag.StringVar(&leaderAddr, "leader", "", "Leader address (empty if this is the leader)")
	flag.Parse()

	// if leaderAddr is empty this broker is the leader
	isLeader := leaderAddr == ""

	if isLeader {
		fmt.Printf("Broker %d starting as LEADER on port %s\n", brokerID, port)
	} else {
		fmt.Printf("Broker %d starting as FOLLOWER on port %s, replicating from %s\n", brokerID, port, leaderAddr)
		// start replication in background
		go startReplication(leaderAddr)
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Listening on port %s...\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}
		go handleConnection(conn)
	}
}