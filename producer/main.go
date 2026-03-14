package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"strings"
	"time"
)

func main() {
	messages := flag.Int("messages", 10000, "Number of messages to send")
	brokerAddr := flag.String("broker", "localhost:9090", "Broker address")
	topic := flag.String("topic", "orders", "Topic to produce to")
	flag.Parse()

	conn, err := net.Dial("tcp", *brokerAddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	fmt.Printf("Benchmarking: sending %d messages to %s...\n", *messages, *brokerAddr)

	latencies := make([]float64, 0, *messages)
	start := time.Now()

	for i := 0; i < *messages; i++ {
		key := fmt.Sprintf("user-%d", rand.Intn(1000))
		message := fmt.Sprintf("order-%d", i)

		msgStart := time.Now()

		fmt.Fprintf(conn, "SEND %s %s %s\n", *topic, key, message)
		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error:", err)
			break
		}

		latency := float64(time.Since(msgStart).Microseconds()) / 1000.0 // ms
		latencies = append(latencies, latency)

		if !strings.HasPrefix(strings.TrimSpace(response), "OK") {
			fmt.Println("Unexpected response:", response)
		}
	}

	totalTime := time.Since(start).Seconds()

	// calculate stats
	sort.Float64s(latencies)

	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]
	throughput := float64(*messages) / totalTime

	fmt.Println("\n--- Benchmark Results ---")
	fmt.Printf("Messages sent:     %d\n", *messages)
	fmt.Printf("Total time:        %.2f seconds\n", totalTime)
	fmt.Printf("Throughput:        %.0f messages/sec\n", throughput)
	fmt.Printf("Latency p50:       %.2f ms\n", p50)
	fmt.Printf("Latency p99:       %.2f ms\n", p99)
	fmt.Printf("Latency min:       %.2f ms\n", latencies[0])
	fmt.Printf("Latency max:       %.2f ms\n", latencies[len(latencies)-1])
}