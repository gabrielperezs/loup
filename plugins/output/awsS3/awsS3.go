package awsS3

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
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

	errAccessDenied          = "AccessDenied"
	errSignatureDoesNotMatch = "SignatureDoesNotMatch"
)

type AwsS3 struct {
	name    string
	svc     *s3.S3
	region  string
	bucket  *string
	profile string
	prefix  string
	key     string
	secret  string

	fileCh  chan *os.File
	bytesCh chan []byte
	done    chan struct{}
}

func New(name string, in interface{}) *AwsS3 {

	cfg := in.(map[string]interface{})

	p := &AwsS3{
		name:    name,
		fileCh:  make(chan *os.File, fileChSize),
		bytesCh: make(chan []byte, bytesChSize),
		done:    make(chan struct{}),
	}

	for k, v := range cfg {
		k = strings.ToLower(k)
		switch k {
		case "profile":
			p.profile = v.(string)
		case "region":
			p.region = v.(string)
		case "bucket":
			p.bucket = aws.String(v.(string))
		case "prefix":
			p.prefix = v.(string)
		case "key":
			p.key = v.(string)
		case "secret":
			p.secret = v.(string)
		}
	}

	var c aws.Credentials
	if p.key != "" && p.secret != "" {
		key := p.key
		if strings.HasPrefix(p.key, "ENV.") {
			key = os.Getenv(strings.Replace(p.key, "ENV.", "", 1))
		}
		secret := p.secret
		if strings.HasPrefix(p.secret, "ENV.") {
			secret = os.Getenv(strings.Replace(p.secret, "ENV.", "", 1))
		}
		c = aws.Credentials{
			AccessKeyID:     key,
			SecretAccessKey: secret,
		}
	}

	awsCfg, err := external.LoadDefaultAWSConfig(
		external.WithCredentialsValue(c),
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
			log.Printf("[O:%s] Debug read file from ch", p.name)
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
		log.Printf("[O:%s] Debug sending file to ch", p.name)
		return nil
	default:
		return fmt.Errorf("Files channel full")
	}
}

func (p *AwsS3) sendFile(f *os.File) error {

	log.Printf("[O:%s] Debug sending file to S3", p.name)

	t := time.Now().UTC()
	hash := murmur3.Sum64WithSeed([]byte(f.Name()), uint32(time.Now().Nanosecond()))

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "localhost"
	}

	prefix := p.prefix
	prefix = strings.Replace(prefix, "%y", fmt.Sprintf("%04d", t.Year()), -1)
	prefix = strings.Replace(prefix, "%m", fmt.Sprintf("%02d", t.Month()), -1)
	prefix = strings.Replace(prefix, "%d", fmt.Sprintf("%02d", t.Day()), -1)
	prefix = strings.Replace(prefix, "%h", fmt.Sprintf("%02d", t.Hour()), -1)
	prefix = strings.Replace(prefix, "%i", fmt.Sprintf("%02d", t.Minute()), -1)

	input := &s3.PutObjectInput{
		ACL:    s3.ObjectCannedACLAuthenticatedRead,
		Body:   aws.ReadSeekCloser(f),
		Bucket: p.bucket,
		Key:    aws.String(fmt.Sprintf("%s/%s-%s-%s-%2x-%s.log.gz", prefix, t.Format(time.RFC3339Nano), hostname, p.name, hash, p.randStringBytes(5))),
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

		switch err.Error() {
		case errAccessDenied, errSignatureDoesNotMatch:
			// Don't try more
			return err
		default:
		}

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
		log.Printf("[O:%s] ERROR SendFile: %s - %s (%s)", p.name, fileName, aerr, aerr.Code())
		return fmt.Errorf(aerr.Code())
	} else {
		log.Printf("[O:%s] ERROR SendFile 2: %s - %s", p.name, fileName, err)
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
