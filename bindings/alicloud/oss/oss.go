// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package oss

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	metadataKey = "key"

	maxKeys = 1000
)

// AliCloudOSS is a binding for an AliCloud OSS storage bucket.
type AliCloudOSS struct {
	metadata *ossMetadata
	client   *oss.Client
	logger   logger.Logger
}

type ossMetadata struct {
	Endpoint    string `json:"endpoint"`
	AccessKeyID string `json:"accessKeyID"`
	AccessKey   string `json:"accessKey"`
	Bucket      string `json:"bucket"`
}

type listPayload struct {
	Marker    string `json:"marker"`
	Prefix    string `json:"prefix"`
	MaxKeys   int32  `json:"maxkeys"`
	Delimiter string `json:"delimiter"`
}

// NewAliCloudOSS returns a new  instance.
func NewAliCloudOSS(logger logger.Logger) *AliCloudOSS {
	return &AliCloudOSS{logger: logger}
}

// Init does metadata parsing and connection creation.
func (s *AliCloudOSS) Init(metadata bindings.Metadata) error {
	m, err := s.parseMetadata(metadata)
	if err != nil {
		return err
	}
	client, err := s.getClient(m)
	if err != nil {
		return err
	}
	s.metadata = m
	s.client = client

	return nil
}

func (s *AliCloudOSS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
	}
}

func (s *AliCloudOSS) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return s.create(req)
	case bindings.GetOperation:
		return s.get(req)
	case bindings.DeleteOperation:
		return s.delete(req)
	case bindings.ListOperation:
		return s.list(req)
	default:
		return nil, fmt.Errorf("aliyun oss binding error. unsupported operation %s", req.Operation)
	}
}

func (s *AliCloudOSS) create(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	key := ""
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		key = uuid.New().String()
		s.logger.Debugf("key not found. generating key %s", key)
	}

	bucket, err := s.client.Bucket(s.metadata.Bucket)
	if err != nil {
		return nil, fmt.Errorf("alicloud oss binding error: error getting bucket failed : %w", err)
	}

	options := []oss.Option{}
	for k, v := range req.Metadata {
		if k == "key" {
			continue
		}
		options = append(options, oss.Meta(k, v))
	}

	err = bucket.PutObject(key, bytes.NewReader(req.Data), options...)
	if err != nil {
		return nil, fmt.Errorf("alicloud oss binding error: error putting object %w", err)
	}

	return &bindings.InvokeResponse{}, nil
}

func (s *AliCloudOSS) get(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, fmt.Errorf("alicloud oss binding error: can't read key value")
	}

	bucket, err := s.client.Bucket(s.metadata.Bucket)
	if err != nil {
		return nil, fmt.Errorf("alicloud oss binding error: error getting bucket : %w", err)
	}

	body, err := bucket.GetObject(key)
	if err != nil {
		serviceErr, ok := err.(oss.ServiceError)
		if !ok {
			return nil, fmt.Errorf("alicloud oss binding error: error getting object : %w", err)
		}
		if serviceErr.StatusCode == 404 && serviceErr.Code == "NoSuchKey" {
			return &bindings.InvokeResponse{}, nil
		}
	}
	defer body.Close()

	data, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("alicloud oss binding error: error reading object : %w", err)
	}

	meta, err := bucket.GetObjectDetailedMeta(key)
	if err != nil {
		return nil, fmt.Errorf("alicloud oss binding error: error reading metadata : %w", err)
	}

	m := map[string]string{}
	for k, v := range meta {
		m[k] = strings.Join(v, " ")
	}

	return &bindings.InvokeResponse{
		Data:     data,
		Metadata: m,
	}, nil
}

func (s *AliCloudOSS) delete(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, fmt.Errorf("alicloud oss binding error: can't read key value")
	}

	bucket, err := s.client.Bucket(s.metadata.Bucket)
	if err != nil {
		return nil, fmt.Errorf("alicloud oss binding error: error getting bucket : %w", err)
	}

	err = bucket.DeleteObject(key)
	if err != nil {
		return nil, fmt.Errorf("alicloud oss binding error: error deleting : %w", err)
	}
	return &bindings.InvokeResponse{}, nil
}

func (s *AliCloudOSS) list(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	bucket, err := s.client.Bucket(s.metadata.Bucket)
	if err != nil {
		return nil, fmt.Errorf("alicloud oss binding error: error getting bucket : %w", err)
	}

	var payload listPayload
	err = json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, fmt.Errorf("aliyun oss binding error. list operation. cannot unmarshal json to blobs: %w", err)
	}

	if payload.MaxKeys == int32(0) {
		payload.MaxKeys = maxKeys
	}

	result, err := bucket.ListObjects(
		oss.Prefix(payload.Prefix),
		oss.Marker(payload.Marker),
		oss.MaxKeys(int(payload.MaxKeys)),
		oss.Delimiter(payload.Delimiter),
	)
	if err != nil {
		return nil, fmt.Errorf("aliyun oss binding error. error listing objects: %w", err)
	}

	jsonResponse, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("aliyun oss binding error. list operation. cannot marshal blobs to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (s *AliCloudOSS) parseMetadata(metadata bindings.Metadata) (*ossMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m ossMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *AliCloudOSS) getClient(metadata *ossMetadata) (*oss.Client, error) {
	client, err := oss.New(metadata.Endpoint, metadata.AccessKeyID, metadata.AccessKey)
	if err != nil {
		return nil, err
	}

	return client, nil
}
