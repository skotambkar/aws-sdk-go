package s3_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var region = "us-east-1"

// Put Object
func TestPutObjectS3NewEndpoint(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			"AKID",
			"SECRET",
			"")),
	))

	svc := s3.New(sess, &aws.Config{
		Retryer:                        client.DefaultRetryer{NumMaxRetries: 3},
		Region:                         aws.String(region),
		DisableRestProtocolURICleaning: aws.Bool(true),
		LogLevel: aws.LogLevel(aws.LogDebugWithSigning),

	})

	input := &s3.PutObjectInput{
		Bucket: aws.String("aws-sdk-go-endpoint-test-2"),
		Body:   aws.ReadSeekCloser(strings.NewReader("filetoupload")),
		Key:    aws.String("exampleobject"),
	}

	result, err := svc.PutObject(input)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				t.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			t.Error(err.Error())
		}
		return
	}

	fmt.Println(result)

}

// Get Object
func TestGetObjectS3NewEndpoint(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			"AKID",
			"SECRET",
			""))))

	svc := s3.New(sess, &aws.Config{
		Retryer:                        client.DefaultRetryer{NumMaxRetries: 3},
		Region:                         aws.String(region),
		DisableRestProtocolURICleaning: aws.Bool(true),
		LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	})

	input := &s3.GetObjectInput{
		Bucket: aws.String("aws-sdk-go-endpoint-test-2"),
		Key:    aws.String("test_endpoint"),
	}

	result, err := svc.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				t.Error(s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				t.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			t.Error(err.Error())
		}
		return
	}

	fmt.Println(result)

}

// Get Object Metadata
func TestGetObjectMetadataS3NewEndpoint(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			"AKID",
			"SECRET",
			""))))

	svc := s3.New(sess, &aws.Config{
		Retryer:                        client.DefaultRetryer{NumMaxRetries: 3},
		Region:                         aws.String(region),
		DisableRestProtocolURICleaning: aws.Bool(true),
	})

	input := &s3.HeadObjectInput{
		Bucket: aws.String("aws-sdk-go-endpoint-test-2"),
		Key:    aws.String("test_endpoint"),
	}

	result, err := svc.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				t.Error(s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				t.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			t.Error(err.Error())
		}
		return
	}
	fmt.Println(result)
}

// Put Bucket tags
func TestPutBucketTaggingS3NewEndpoint(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			"AKID",
			"SECRET",
			""))))
	svc := s3.New(sess, &aws.Config{
		Retryer:                        client.DefaultRetryer{NumMaxRetries: 3},
		Region:                         aws.String(region),
		DisableRestProtocolURICleaning: aws.Bool(true),
	})

	input := &s3.PutBucketTaggingInput{
		Bucket: aws.String("aws-sdk-go-endpoint-test-2"),
		Tagging: &s3.Tagging{TagSet: []*s3.Tag{
			{
				Key:   aws.String("key2"),
				Value: aws.String("value2"),
			},
		}},
	}

	result, err := svc.PutBucketTagging(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				t.Error(s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				t.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			t.Error(err.Error())
		}
		return
	}
	fmt.Println(result)
}

// Get Bucket tags
func TestGetBucketTaggingS3NewEndpoint(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			"AKID",
			"SECRET",
			""))))
	svc := s3.New(sess, &aws.Config{
		Retryer:                        client.DefaultRetryer{NumMaxRetries: 3},
		Region:                         aws.String(region),
		DisableRestProtocolURICleaning: aws.Bool(true),
	})

	input := &s3.GetBucketTaggingInput{
		Bucket: aws.String("aws-sdk-go-endpoint-test-2"),
	}

	result, err := svc.GetBucketTagging(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				t.Error(s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				t.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			t.Error(err.Error())
		}
		return
	}

	fmt.Println(result)
}

// Multipart upload object
func TestMultiPartUploadS3NewEndpoint(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			"AKID",
			"SECRET",
			"Session")),
	))
	svc := s3.New(sess, &aws.Config{
		Retryer:                        client.DefaultRetryer{NumMaxRetries: 3},
		Region:                         aws.String(region),
		DisableRestProtocolURICleaning: aws.Bool(true),
	})

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String("aws-sdk-go-endpoint-test-2"),
		Key:    aws.String("largeobject"),
	}

	result, err := svc.CreateMultipartUpload(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				t.Error(s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				t.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			t.Error(err.Error())
		}
		return
	}

	fmt.Println(result)
}

// Create Bucket in us-west-2
func TestCreateBucketS3NewEndpoint(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			"AKID",
			"SECRET",
			"")),
	))
	svc := s3.New(sess, &aws.Config{
		Retryer:                        client.DefaultRetryer{NumMaxRetries: 3},
		Region:                         aws.String(region),
		DisableRestProtocolURICleaning: aws.Bool(true),
	})

	input := &s3.CreateBucketInput{
		Bucket: aws.String("aws-sdk-go-endpoint-test-third"),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String("us-west-2"),
		}}

	result, err := svc.CreateBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				t.Error(s3.ErrCodeBucketAlreadyExists, aerr.Error())
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				t.Error(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
			default:
				t.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			t.Error(err.Error())
		}
		return
	}

	fmt.Println(result)
}

// Delete a bucket in us-west-2
func TestDeleteBucketS3NewEndpoint(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			"AKID",
			"SECRET",
			"")),
	))
	svc := s3.New(sess, &aws.Config{
		Retryer:                        client.DefaultRetryer{NumMaxRetries: 3},
		Region:                         aws.String(region),
		DisableRestProtocolURICleaning: aws.Bool(true),
	})

	input := &s3.DeleteBucketInput{
		Bucket: aws.String("aws-sdk-go-endpoint-test-third"),
	}

	result, err := svc.DeleteBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				t.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			t.Error(err.Error())
		}
		return
	}

	fmt.Println(result)
}

// List all objects in the bucket
func TestBucketObjectExist(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			"AKID",
			"SECRET",
			""))))

	svc := s3.New(sess, &aws.Config{
		Retryer:                        client.DefaultRetryer{NumMaxRetries: 3},
		Region:                         aws.String(region),
		DisableRestProtocolURICleaning: aws.Bool(true),
	})

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String("aws-sdk-go-endpoint-test-2"),
	}
	result, err := svc.ListObjectsV2(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				t.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			t.Error(err.Error())
		}
		return
	}

	fmt.Println(result)
}

// All the buckets for the account
func TestBucketExist(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			"AKID",
			"SECRET",
			""))))

	svc := s3.New(sess, &aws.Config{
		Retryer:                        client.DefaultRetryer{NumMaxRetries: 3},
		Region:                         aws.String(region),
		DisableRestProtocolURICleaning: aws.Bool(true),
	})

	input := &s3.ListBucketsInput{}
	result, err := svc.ListBuckets(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				t.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			t.Error(err.Error())
		}
		return
	}

	fmt.Println(result)
}
