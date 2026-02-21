package main

import (
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	mu   sync.Mutex
	done chan struct{}
}

func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		done: make(chan struct{}),
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				aof.mu.Lock()
				aof.file.Sync()
				aof.mu.Unlock()
			case <-aof.done:
				return
			}
		}
	}()

	return aof, nil
}

func (aof *Aof) Close() error {
	close(aof.done)

	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}

func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err
	}

	// Flush to ensure data is written immediately
	err = aof.file.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (aof *Aof) Read(callback func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	resp := NewResp(aof.file)

	for {
		value, err := resp.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		callback(value)
	}

	return nil
}
