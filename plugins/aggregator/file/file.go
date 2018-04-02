package file

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/gabrielperezs/loup/lib"
	"github.com/gallir/bytebufferpool"
)

var (
	tmpFilePrefix     = "loup-cmd-"
	newLine           = []byte("\n")
	defaultLimitLines = uint64(99999)
	defaultBufSize    = 4096
	pool              = &bytebufferpool.Pool{}
)

type File struct {
	m     sync.Mutex
	name  string
	bufCh chan *bytebufferpool.ByteBuffer
	tmp   *os.File
	gz    *gzip.Writer
	done  chan struct{}

	compress      bool
	compressLevel int
	output        lib.Output

	size       uint64
	lines      uint64
	linesLimit uint64
	flushing   uint32
}

func New(name string, cfg map[string]interface{}, output lib.Output) *File {

	a := &File{
		name:   name,
		bufCh:  make(chan *bytebufferpool.ByteBuffer, defaultBufSize),
		done:   make(chan struct{}),
		output: output,
	}

	for k, v := range cfg {
		k = strings.ToLower(k)
		switch k {
		case "compress":
			a.compress = v.(bool)
		case "compresslevel":
			a.compressLevel = int(v.(int64))
		case "lineslimit":
			a.linesLimit = uint64(v.(int64))
		}
	}

	if a.linesLimit == 0 {
		a.linesLimit = defaultLimitLines
	}

	go a.reader()

	return a
}

func (a *File) GetFile() *os.File {
	return a.tmp
}

func (a *File) reader() {
	a.reset()

	for b := range a.bufCh {
		if a.compress {
			a.gz.Write(b.B)
		} else {
			a.tmp.Write(b.B)
		}

		a.lines += uint64(bytes.Count(b.B, newLine))
		a.size += uint64(len(b.B))

		pool.Put(b)

		if a.lines >= a.linesLimit {
			a.flush()
			a.reset()
		}
	}

	a.done <- struct{}{}
}

func (a *File) flush() (err error) {

	if a.gz != nil {
		// Be sure that the GZ buffer write everything to the tmp file
		// It does not close the underlying io.Writer. This will be done
		// by the output plugin
		a.gz.Close()
	}

	if a.tmp != nil {
		// Send tmp file to the output plugin
		lastTmp := a.tmp
		return a.output.SendFile(lastTmp)
	}

	return nil
}

func (a *File) reset() (err error) {

	a.m.Lock()
	defer a.m.Unlock()

	// Create new tmp file
	a.tmp, err = ioutil.TempFile(os.TempDir(), tmpFilePrefix)
	if err != nil {
		log.Printf("[A:%s] ERROR creating tmp file: %s", a.name, err)
		return err
	}

	// Create new zip writer
	if a.compress {
		a.gz, err = gzip.NewWriterLevel(a.tmp, a.compressLevel)
		if err != nil {
			log.Printf("[A:%s] ERROR creating gz writer: %s", a.name, err)
			return err
		}
	}

	// Reset counters
	a.size = 0
	a.lines = 0

	return
}

func (a *File) Write(b []byte) (i int, err error) {
	log.Printf("[A:%s] write", a.name)
	n := pool.GetLen(len(b))
	n.Set(b)
	a.bufCh <- n
	return len(b), nil
}

func (a *File) Exit() {

	log.Printf("[A:%s]: Flush buffer", a.name)

	close(a.bufCh)
	<-a.done
	a.flush()

	log.Printf("[A:%s]: Exit", a.name)
}
