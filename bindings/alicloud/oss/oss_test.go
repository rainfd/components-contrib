// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package oss

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"AccessKey": "key", "Endpoint": "endpoint", "AccessKeyID": "accessKeyID", "Bucket": "test"}
	aliCloudOSS := AliCloudOSS{}
	meta, err := aliCloudOSS.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "key", meta.AccessKey)
	assert.Equal(t, "endpoint", meta.Endpoint)
	assert.Equal(t, "accessKeyID", meta.AccessKeyID)
	assert.Equal(t, "test", meta.Bucket)
}

func TestOption(t *testing.T) {
	oss := NewAliCloudOSS(logger.NewLogger("alicloudoss"))
	oss.metadata = &ossMetadata{}

	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := oss.get(&r)
		assert.Error(t, err)
		_, err = oss.delete(&r)
		assert.Error(t, err)
	})
}
