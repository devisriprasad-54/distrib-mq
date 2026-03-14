package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"strings"
	"time"
)

func main() {
	groupID := flag.String("group", "order-processor", "Consumer group ID")
	consumerID := flag.String("id", "consumerA", "Consumer ID")
	topic := flag.String("topic", "orders", "Topic to consume")
	brokerAddr := flag.String("broker", "localhost:9090", "Broker address")
	flag.Parse()

	// connect to broker
	conn, err := net.Dial("tcp", *brokerAddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// join the group
	fmt.Fprintf(conn, "JOIN %s %s\n", *groupID, *consumerID)
	response, _ := reader.ReadString('\n')
	response = strings.TrimSpace(response)
	fmt.Println("Join response:", response)

	// parse assigned partition
	var partition int
	fmt.Sscanf(response, "ASSIGNED %d", &partition)
	fmt.Printf("Assigned to partition %d\n", partition)

	// start heartbeat in background
	go func() {
		for {
			time.Sleep(2 * time.Second)
			fmt.Fprintf(conn, "PING %s %s\n", *groupID, *consumerID)
			pong, _ := reader.ReadString('\n')
			pong = strings.TrimSpace(pong)
			if pong != "PONG" {
				fmt.Println("Heartbeat failed:", pong)
			}
		}
	}()

	// consume messages
	offset := 0
	for {
		fmt.Fprintf(conn, "READ %s %d %d\n", *topic, partition, offset)
		msg, _ := reader.ReadString('\n')
		msg = strings.TrimSpace(msg)

		if strings.HasPrefix(msg, "MSG") {
			fmt.Printf("Consumed partition=%d offset=%d message=%s\n", partition, offset, strings.TrimPrefix(msg, "MSG "))
			offset++
		} else {
			// no new messages, wait before polling again
			time.Sleep(500 * time.Millisecond)
		}
	}
}