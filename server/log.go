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

func NewLog(topic string, partition int) (*Log, error) {
	base := fmt.Sprintf("data/%s-%d", topic, partition)

	f, err := os.OpenFile(base+".log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	idx, err := os.OpenFile(base+".index", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	l := &Log{file: f, index: idx}
	err = l.loadIndex()
	if err != nil {
		return nil, err
	}

	// seek to end of log file so new appends go to the correct position
	_, err = l.file.Seek(0, 2) // 2 = seek from end
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Log) loadIndex() error {
	for {
		var pos int64
		err := binary.Read(l.index, binary.LittleEndian, &pos)
		if err != nil {
			break
		}
		l.offsets = append(l.offsets, pos)
	}
	return nil
}

func (l *Log) Append(message string) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	pos, _ := l.file.Seek(0, 1)
	l.offsets = append(l.offsets, pos)

	err := binary.Write(l.index, binary.LittleEndian, pos)
	if err != nil {
		return 0, err
	}

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