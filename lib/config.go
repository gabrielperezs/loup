package lib

import (
	"os"
	"sync"
)

/*
 Config
*/

type Config struct {
	Output     map[string]interface{} `toml:"Output"`
	Input      map[string]interface{} `toml:"Input"`
	Aggregator map[string]interface{} `toml:"Aggregator"`
}

/*
 Loaded
*/

type Output interface {
	SendBytes(b []byte) error
	SendFile(f *os.File) error
	Exit()
}

type Input interface {
	Exit()
}

type Aggregator interface {
	Write(b []byte) (int, error)
	Exit()
}

type Loaded struct {
	Output     sync.Map
	Input      sync.Map
	Aggregator sync.Map
}
