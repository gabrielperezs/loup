package cmd

import (
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gabrielperezs/loup/lib"
)

var (
	retry = 1 * time.Second
)

func New(name string, cfg map[string]interface{}, o lib.Aggregator) *Cmd {

	c := &Cmd{
		name:       name,
		done:       make(chan struct{}),
		aggregator: o,
	}

	for k, v := range cfg {
		switch strings.ToLower(k) {
		case "cmd":
			for _, cmdArgs := range v.([]interface{}) {
				c.command = append(c.command, cmdArgs.(string))
			}
		}
	}

	go c.listen()

	return c
}

type Cmd struct {
	m          sync.Mutex
	name       string
	command    []string
	aggregator lib.Aggregator
	cmd        *exec.Cmd
	done       chan struct{}
	exiting    bool

	limitLines int
}

func (c *Cmd) isExiting() bool {
	c.m.Lock()
	defer c.m.Unlock()

	if c.exiting {
		return true
	}
	return false
}

func (c *Cmd) listen() {
	defer func() {
		c.done <- struct{}{}
	}()

	for {

		if c.isExiting() {
			return
		}

		if c.cmd != nil {
			time.Sleep(retry)
		}

		log.Printf("[I:%s] Start", c.name)

		cmd := exec.Command(c.command[0], c.command[1:]...)
		cmd.Stdout = c.aggregator

		if err := cmd.Start(); err != nil {
			log.Printf("[I:%s] ERROR Start: %s", c.name, err)
			return
		}

		c.m.Lock()
		c.cmd = cmd
		c.m.Unlock()

		if c.isExiting() {
			return
		}

		if err := cmd.Wait(); err != nil {
			log.Printf("[I:%s] ERROR Wait: %s", c.name, err)
		}
	}

}

func (c *Cmd) Exit() {
	log.Printf("[I:%s]: Exit 1", c.name)

	var proc *os.Process
	c.m.Lock()
	c.exiting = true
	cmd := c.cmd
	if cmd != nil {
		proc = cmd.Process
	}
	c.m.Unlock()

	if proc != nil {
		proc.Signal(syscall.SIGTERM)
		<-c.done
	}

	log.Printf("[I:%s]: Exit 2", c.name)

	c.aggregator.Exit()

	log.Printf("[I:%s]: Exit 3", c.name)

}
