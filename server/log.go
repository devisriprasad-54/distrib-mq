package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type Log struct {
	mu      sync.Mutex
	file    *os.File
	index   *os.File
	offsets []int64
}

func NewLog(topic string) (*Log, error) {
	// open log file
	f, err := os.OpenFile("data/"+topic+".log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// open index file
	idx, err := os.OpenFile("data/"+topic+".index", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	l := &Log{file: f, index: idx}

	// restore offsets from index file on startup
	err = l.loadIndex()
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Log) loadIndex() error {
	// each offset is stored as 8 bytes (int64)
	for {
		var pos int64
		err := binary.Read(l.index, binary.LittleEndian, &pos)
		if err != nil {
			break // EOF — done reading
		}
		l.offsets = append(l.offsets, pos)
	}
	fmt.Printf("Restored %d offsets from index\n", len(l.offsets))
	return nil
}

func (l *Log) Append(message string) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// get current byte position
	pos, _ := l.file.Seek(0, 1)
	l.offsets = append(l.offsets, pos)

	// persist offset to index file immediately
	err := binary.Write(l.index, binary.LittleEndian, pos)
	if err != nil {
		return 0, err
	}

	// write message to log
	_, err = fmt.Fprintln(l.file, message)
	if err != nil {
		return 0, err
	}

	return len(l.offsets) - 1, nil
}

func (l *Log) Read(offset int) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if offset >= len(l.offsets) {
		return "", fmt.Errorf("EMPTY")
	}

	bytePos := l.offsets[offset]
	l.file.Seek(bytePos, 0)

	buf := make([]byte, 1024)
	n, err := l.file.Read(buf)
	if err != nil {
		return "", err
	}

	msg := string(buf[:n])
	for i, c := range msg {
		if c == '\n' {
			msg = msg[:i]
			break
		}
	}

	return msg, nil
}