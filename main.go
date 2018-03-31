package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/gabrielperezs/loup/lib"
	"github.com/gabrielperezs/loup/plugins/aggregator/file"
	"github.com/gabrielperezs/loup/plugins/input/cmd"
	"github.com/gabrielperezs/loup/plugins/output/awsS3"
)

const (
	defaultConfigFile = "config.toml"
)

var (
	loaded     lib.Loaded
	conf       lib.Config
	configFile string
	done       chan struct{}
)

func main() {
	flag.StringVar(&configFile, "config", defaultConfigFile, "Path to the config file")
	flag.Parse()
	done = make(chan struct{})
	signals()
	reload()
	<-done
}

func signals() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGTERM, os.Interrupt)
	go func() {
		for {
			switch <-sigs {
			case syscall.SIGHUP:
				log.Printf("Reload")
				reload()
			default:
				finishLoaded()
				log.Printf("Exit")
				done <- struct{}{}
				return
			}
		}
	}()
}

func finishLoaded() {

	wg := &sync.WaitGroup{}

	// Finish running Inputs
	loaded.Input.Range(func(k, v interface{}) bool {
		wg.Add(1)
		go func(in lib.Input) {
			defer wg.Done()
			in.Exit()
		}(v.(lib.Input))
		loaded.Input.Delete(k)
		return true
	})
	wg.Wait()

	loaded.Aggregator.Range(func(k, v interface{}) bool {
		loaded.Input.Delete(k)
		return true
	})

	// Finish running Outputs
	loaded.Output.Range(func(k, v interface{}) bool {
		wg.Add(1)
		go func(o lib.Output) {
			defer wg.Done()
			o.Exit()
		}(v.(lib.Output))
		loaded.Input.Delete(k)
		return true
	})
	wg.Wait()
}

func reload() {
	log.Printf("Loading %s", configFile)

	if _, err := os.Stat(configFile); err != nil {
		log.Fatal("Config file is missing: ", configFile)
	}

	finishLoaded()

	var localConf lib.Config
	if _, err := toml.DecodeFile(configFile, &localConf); err != nil {
		log.Fatalf("Invalid configuration file: %s", err)
	}

	conf = localConf

	wg := &sync.WaitGroup{}

	log.Printf("Loading output plugins")
	for name, c := range conf.Output {
		wg.Add(1)
		go func(name string, c interface{}) {
			defer wg.Done()
			switch strings.ToLower(c.(map[string]interface{})["plugin"].(string)) {
			case "awss3":
				loaded.Output.Store(name, awsS3.New(name, c))
			}
		}(name, c)
	}
	wg.Wait()

	log.Printf("Loading aggregators plugins")
	for name, in := range conf.Aggregator {
		loaded.Aggregator.Store(name, in)
	}

	log.Printf("Loading input plugins")
	for name, in := range conf.Input {
		wg.Add(1)
		go func(name string, in interface{}) {
			defer wg.Done()

			var plugin string
			var aggregatorName string
			var aggregatorCfg map[string]interface{}
			var o lib.Output

			for k, v := range in.(map[string]interface{}) {
				k = strings.ToLower(k)
				switch k {
				case "output":
					var oi interface{}
					var ok bool
					if oi, ok = loaded.Output.Load(v.(string)); !ok {
						log.Printf("[I:%s] ERROR: Output don't exits %s", name, k)
						return
					}
					o = oi.(lib.Output)
				case "aggregator":
					aggregatorName = v.(string)
					var oi interface{}
					var ok bool
					if oi, ok = loaded.Aggregator.Load(aggregatorName); !ok {
						log.Printf("[I:%s] ERROR: Aggregator don't exits %s", name, k)
						return
					}
					aggregatorCfg = oi.(map[string]interface{})
				case "plugin":
					plugin = v.(string)
				}
			}

			switch plugin {
			case "cmd":
				p := cmd.New(name, in.(map[string]interface{}), newAggregator(aggregatorName, aggregatorCfg, o))
				loaded.Input.Store(name, p)
			}
		}(name, in)
	}
	wg.Wait()
}

func newAggregator(k string, cfg map[string]interface{}, o lib.Output) lib.Aggregator {
	switch k {
	case strings.ToLower(cfg["plugin"].(string)):
		return file.New(k, cfg, o)
	}
	return nil
}
