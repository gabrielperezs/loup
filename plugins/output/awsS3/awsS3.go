package awsS3

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	maxRetry    = 10
	sleepRetry  = 5 * time.Second
	letterBytes = "abcdefghijklmnopqrstuvwxyz0123456789"
	fileChSize  = 1000
	bytesChSize = 1000
)

type AwsS3 struct {
	name    string
	svc     *s3.S3
	region  string
	bucket  *string
	profile string
	prefix  string

	fileCh  chan *os.File
	bytesCh chan []byte
	done    chan struct{}
}

func New(name string, in interface{}) *AwsS3 {

	cfg := in.(map[string]interface{})

	p := &AwsS3{
		name:    name,
		profile: cfg["profile"].(string),
		region:  cfg["region"].(string),
		bucket:  aws.String(cfg["bucket"].(string)),
		prefix:  cfg["prefix"].(string),

		fileCh:  make(chan *os.File, fileChSize),
		bytesCh: make(chan []byte, bytesChSize),
		done:    make(chan struct{}),
	}

	awsCfg, err := external.LoadDefaultAWSConfig(
		external.WithSharedConfigProfile(p.profile),
		external.WithRegion(p.region),
	)
	if err != nil {
		log.Printf("[O:%s] failed to load config: %s", p.name, err)
		return p
	}

	p.svc = s3.New(awsCfg)

	go p.listen()

	return p
}

func (p *AwsS3) listen() {
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for f := range p.fileCh {
			log.Printf("[O:%s] Debug read file from ch")
			p.sendFile(f)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := range p.bytesCh {
			p.sendBytes(b)
		}
	}()

	wg.Wait()

	p.done <- struct{}{}
}

func (p *AwsS3) SendFile(f *os.File) error {
	select {
	case p.fileCh <- f:
		log.Printf("[O:%s] Debug sending file to ch")
		return nil
	default:
		return fmt.Errorf("Files channel full")
	}
}

func (p *AwsS3) sendFile(f *os.File) error {

	log.Printf("[O:%s] Debug sending file to S3")

	t := time.Now().UTC()
	hash := murmur3.Sum64WithSeed([]byte(f.Name()), uint32(time.Now().Nanosecond()))

	input := &s3.PutObjectInput{
		ACL:    s3.ObjectCannedACLAuthenticatedRead,
		Body:   aws.ReadSeekCloser(f),
		Bucket: p.bucket,

		Key: aws.String(fmt.Sprintf("%s/%04d/%02d/%02d/%02d/%02d/%s-%2x-%s.gz",
			p.prefix, t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), p.name, hash, p.randStringBytes(5))),
	}

	retry := 0
	for {
		err := p.putObject(input, f.Name())
		if err == nil {
			if fInfo, err := f.Stat(); err == nil {
				log.Printf("[O:%s] sent %s (%d bytes)", p.name, *input.Key, fInfo.Size())
			}
			break
		}

		log.Printf("[O:%s] ERROR SendFile failed: %s - %s", p.name, f.Name(), err)
		retry++

		if retry >= maxRetry {
			log.Printf("[O:%s] ERROR SendFile max retry: %s %d/%d", p.name, f.Name(), retry, maxRetry)
			return fmt.Errorf("Maximum errors uploading")
		}

		// Waiting for the next try
		time.Sleep(sleepRetry)
	}

	os.Remove(f.Name())

	return nil
}

func (p *AwsS3) putObject(input *s3.PutObjectInput, fileName string) error {
	req := p.svc.PutObjectRequest(input)
	_, err := req.Send()
	if err == nil {
		return nil
	}

	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		default:
			log.Printf("[O:%s] ERROR SendFile: %s - %s", p.name, fileName, aerr)
		}
	} else {
		log.Printf("[O:%s] ERROR SendFile: %s - %s", p.name, fileName, err)
	}
	return err
}

func (p *AwsS3) sendBytes(b []byte) error {
	select {
	case p.bytesCh <- b:
		return nil
	default:
		return fmt.Errorf("Bytes channel full")
	}
}

func (p *AwsS3) SendBytes(b []byte) error {

	return nil
}

func (p *AwsS3) Exit() {

	if p.bytesCh != nil {
		close(p.fileCh)
		if d := len(p.bytesCh); d > 0 {
			log.Printf("[O:%s] pending %d bytes", p.name, d)
		}
	}

	if p.fileCh != nil {
		close(p.bytesCh)
		if d := len(p.fileCh); d > 0 {
			log.Printf("[O:%s] pending %d files", p.name, d)
		}
	}

	<-p.done

	log.Printf("[O:%s] Exit", p.name)
}

func (p *AwsS3) randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
