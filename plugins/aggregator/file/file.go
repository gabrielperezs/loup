package file

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gabrielperezs/loup/lib"
)

var (
	tmpFilePrefix     = "loup-cmd-"
	newLine           = []byte("\n")
	defaultLimitLines = 99999
)

type File struct {
	m    sync.Mutex
	name string
	tmp  *os.File
	buf  *bufio.Writer
	gz   *gzip.Writer

	compress      bool
	compressLevel int
	output        lib.Output

	size       int
	lines      int
	linesLimit int
	flushing   uint32
}

func New(name string, cfg map[string]interface{}, output lib.Output) *File {

	a := &File{
		name:   name,
		buf:    bufio.NewWriter(ioutil.Discard),
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
			a.linesLimit = int(v.(int64))
		}
	}

	if a.linesLimit == 0 {
		a.linesLimit = defaultLimitLines
	}

	return a
}

func (a *File) GetFile() *os.File {
	return a.tmp
}

func (a *File) Flush() (err error) {

	if atomic.LoadUint32(&a.flushing) == 1 {
		return nil
	}

	atomic.StoreUint32(&a.flushing, 1)
	defer func() {
		atomic.StoreUint32(&a.flushing, 0)
	}()

	var tmp *os.File
	tmp, err = ioutil.TempFile(os.TempDir(), tmpFilePrefix)
	if err != nil {
		log.Printf("[A:%s] ERROR creating tmp file: %s", a.name, err)
		return err
	}

	var gz *gzip.Writer
	if a.compress {
		gz, err = gzip.NewWriterLevel(tmp, a.compressLevel)
		if err != nil {
			log.Printf("[A:%s] ERROR creating gz writer: %s", a.name, err)
			return err
		}
	}

	// Flush memory buffer to the tmp file
	a.buf.Flush()

	// Mutex to reset all variable, from this point the buffer will start to write
	// in the new files
	a.m.Lock()

	// Keep pointes to OLD writers
	oldTmp := a.tmp
	oldGz := a.gz
	// Reassined new pointers to the writers
	a.tmp = tmp
	if a.compress {
		a.gz = gz
		a.buf.Reset(a.gz)
	} else {
		a.gz = nil
		a.buf.Reset(a.tmp)
	}
	a.size = 0
	a.lines = 0

	a.m.Unlock()

	if oldTmp == nil {
		return
	}

	if oldGz != nil {
		// Be sure that the GZ buffer write everything to the tmp file
		// It does not close the underlying io.Writer. This will be done
		// by the output plugin
		oldGz.Close()
	}

	// Send tmp file to the output plugin
	a.output.SendFile(oldTmp)

	return
}

func (a *File) Write(b []byte) (i int, err error) {
	if atomic.LoadUint32(&a.flushing) == 1 {
		return 0, fmt.Errorf("Flushing...")
	}

	i, err = a.buf.Write(b)
	if err != nil {
		log.Printf("[A:%s] ERROR WRITE: %s ", a.name, err)
		return
	}

	a.lines += bytes.Count(b, newLine)

	if a.lines >= a.linesLimit {
		log.Printf("[A:%s]: Lines %d / Len %d", a.name, a.lines, a.buf.Buffered())
		a.Flush()
	}
	return
}

func (a *File) Exit() {

	log.Printf("[A:%s]: Flush buffer", a.name)
	a.Flush()

	log.Printf("[A:%s]: Exit", a.name)
}
