package fetcher

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"io"
	"net/url"
)

type S3Fetcher struct {
	url        *url.URL
	BucketName string
	Client     *s3.S3
}

func NewS3Fetcher(u *url.URL) (*S3BinaryCacheStore, error) {
	scheme := u.Query().Get("scheme")
	profile := u.Query().Get("profile")
	region := u.Query().Get("region")
	endpoint := u.Query().Get("endpoint")
	bucketName := u.Host
	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvProvider{},
			&credentials.SharedCredentialsProvider{},
		})

	var disableSSL bool
	switch scheme {
	case "http":
		disableSSL = true
	case "https", "":
		disableSSL = false
	default:
		return &S3Fetcher{}, fmt.Errorf("Unsupported scheme %s", scheme)
	}

	var sess = session.Must(session.NewSessionWithOptions(session.Options{
		// Specify profile to load for the session's config
		Profile: profile,

		// Provide SDK Config options, such as Region.
		Config: aws.Config{
			Region:           aws.String(region),
			Endpoint:         &endpoint,
			Credentials:      creds,
			DisableSSL:       aws.Bool(disableSSL),
			S3ForcePathStyle: aws.Bool(true),
		},
	}))

	svc := s3.New(sess)
	return &S3Fetcher{
		url:        u,
		BucketName: bucketName,
		Client:     svc,
	}, nil
}

func (c *S3Fetcher) FileExists(ctx context.Context, path string) (bool, error) {
	_, err := c.GetFile(ctx, path)
	aerr, ok := err.(awserr.Error)
	if ok {
		switch aerr.Code() {
		case s3.ErrCodeNoSuchKey:
			return false, aerr
		default:
			return true, aerr
		}
	} else {
		return true, nil
	}
}

func (c *S3Fetcher) GetFile(ctx context.Context, path string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.BucketName),
		Key:    aws.String(path),
	}

	obj, err := c.Client.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, err
	}

	return obj.Body, nil // for now we return Object data with type blob
}

// URL returns the fetcher URI
func (c S3Fetcher) URL() string {
	return c.url.String()
}
