//go:build !wasm

package wasman

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// S3SnapshotStore implements SnapshotStore using an S3-compatible object store.
type S3SnapshotStore struct {
	Client      *s3.Client
	bucket      string
	Compression bool
}

var _ SnapshotStore = (*S3SnapshotStore)(nil)

// NewS3SnapshotStore initializes a new S3 snapshot store.
func NewS3SnapshotStore(ctx context.Context, bucket string, opts ...func(*s3.Options)) (*S3SnapshotStore, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %w", err)
	}

	client := s3.NewFromConfig(cfg, opts...)
	return &S3SnapshotStore{
		Client: client,
		bucket: bucket,
	}, nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if code == "NoSuchKey" || code == "NotFound" {
			return true
		}
	}
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "404") || strings.Contains(errStr, "nosuchkey") || strings.Contains(errStr, "notfound") || strings.Contains(errStr, "not found") {
		return true
	}
	return false
}

func isPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	var smithyErr smithy.APIError
	if errors.As(err, &smithyErr) {
		code := smithyErr.ErrorCode()
		if code == "PreconditionFailed" || strings.Contains(strings.ToLower(code), "precondition") {
			return true
		}
	}
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "412") || strings.Contains(errStr, "preconditionfailed") || strings.Contains(errStr, "precondition failed") {
		return true
	}
	return false
}

func (s *S3SnapshotStore) readObject(key string) ([]byte, string, error) {
	out, err := s.Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}

	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return data, etag, nil
}

func (s *S3SnapshotStore) writeObject(key string, data []byte) (string, error) {
	out, err := s.Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return "", err
	}
	if out.ETag != nil {
		return *out.ETag, nil
	}
	return "", nil
}

// Save writes a full memory snapshot to S3.
func (s *S3SnapshotStore) Save(id string, snapshot []byte) error {
	key := fmt.Sprintf("instances/%s/snapshot.bin", id)
	data := snapshot
	if s.Compression {
		var err error
		data, err = compressData(snapshot)
		if err != nil {
			return fmt.Errorf("failed to compress snapshot for '%s': %w", id, err)
		}
	}
	_, err := s.writeObject(key, data)
	if err != nil {
		return fmt.Errorf("failed to save snapshot for '%s': %w", id, err)
	}
	return nil
}

// Load reads a full memory snapshot from S3.
func (s *S3SnapshotStore) Load(id string) ([]byte, error) {
	key := fmt.Sprintf("instances/%s/snapshot.bin", id)
	data, _, err := s.readObject(key)
	if err != nil {
		return nil, err
	}
	return decompressData(data)
}

// SaveDeltas saves memory deltas to S3 by reading current, overlaying new ones and writing back.
func (s *S3SnapshotStore) SaveDeltas(id string, deltas map[int][]byte) error {
	key := fmt.Sprintf("instances/%s/deltas.json", id)
	current := make(map[int][]byte)

	data, _, err := s.readObject(key)
	if err == nil {
		decompressed, err := decompressData(data)
		if err == nil {
			_ = json.Unmarshal(decompressed, &current)
		}
	} else if !isNotFound(err) {
		return fmt.Errorf("failed to read deltas from S3: %w", err)
	}

	for k, v := range deltas {
		current[k] = v
	}

	newData, err := json.Marshal(current)
	if err != nil {
		return fmt.Errorf("failed to marshal deltas: %w", err)
	}

	if s.Compression {
		newData, err = compressData(newData)
		if err != nil {
			return fmt.Errorf("failed to compress deltas: %w", err)
		}
	}

	_, err = s.writeObject(key, newData)
	if err != nil {
		return fmt.Errorf("failed to write deltas to S3: %w", err)
	}
	return nil
}

// LoadDeltas retrieves memory deltas from S3.
func (s *S3SnapshotStore) LoadDeltas(id string) (map[int][]byte, error) {
	key := fmt.Sprintf("instances/%s/deltas.json", id)
	data, _, err := s.readObject(key)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load deltas from S3: %w", err)
	}

	decompressed, err := decompressData(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress deltas from S3: %w", err)
	}

	var deltas map[int][]byte
	err = json.Unmarshal(decompressed, &deltas)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal deltas: %w", err)
	}
	return deltas, nil
}

// TruncateDeltas deletes memory deltas for the instance from S3.
func (s *S3SnapshotStore) TruncateDeltas(id string) error {
	key := fmt.Sprintf("instances/%s/deltas.json", id)
	_, err := s.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("failed to truncate deltas on S3: %w", err)
	}
	return nil
}

// SaveOplog appends an API call to the oplog JSON on S3.
func (s *S3SnapshotStore) SaveOplog(id string, callIndex int, apiName string, request []byte, response []byte) error {
	key := fmt.Sprintf("instances/%s/oplog.json", id)
	var list []OplogEntry

	data, _, err := s.readObject(key)
	if err == nil {
		decompressed, err := decompressData(data)
		if err == nil {
			_ = json.Unmarshal(decompressed, &list)
		}
	} else if !isNotFound(err) {
		return fmt.Errorf("failed to read oplog from S3: %w", err)
	}

	list = append(list, OplogEntry{
		CallIndex:       callIndex,
		ApiName:         apiName,
		RequestPayload:  request,
		ResponsePayload: response,
	})

	newData, err := json.Marshal(list)
	if err != nil {
		return fmt.Errorf("failed to marshal oplog: %w", err)
	}

	if s.Compression {
		newData, err = compressData(newData)
		if err != nil {
			return fmt.Errorf("failed to compress oplog: %w", err)
		}
	}

	_, err = s.writeObject(key, newData)
	if err != nil {
		return fmt.Errorf("failed to write oplog to S3: %w", err)
	}
	return nil
}

// LoadOplog retrieves the oplog entries from S3.
func (s *S3SnapshotStore) LoadOplog(id string) ([]OplogEntry, error) {
	key := fmt.Sprintf("instances/%s/oplog.json", id)
	data, _, err := s.readObject(key)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load oplog from S3: %w", err)
	}

	decompressed, err := decompressData(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress oplog from S3: %w", err)
	}

	var list []OplogEntry
	err = json.Unmarshal(decompressed, &list)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal oplog: %w", err)
	}
	return list, nil
}

// TruncateOplog deletes oplog entries at or below the given call index.
func (s *S3SnapshotStore) TruncateOplog(id string, beforeCallIndex int) error {
	key := fmt.Sprintf("instances/%s/oplog.json", id)
	data, _, err := s.readObject(key)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to read oplog for truncate from S3: %w", err)
	}

	decompressed, err := decompressData(data)
	if err != nil {
		return fmt.Errorf("failed to decompress oplog for truncate from S3: %w", err)
	}

	var list []OplogEntry
	if err := json.Unmarshal(decompressed, &list); err != nil {
		return fmt.Errorf("failed to unmarshal oplog for truncate: %w", err)
	}

	var filtered []OplogEntry
	for _, entry := range list {
		if entry.CallIndex > beforeCallIndex {
			filtered = append(filtered, entry)
		}
	}

	newData, err := json.Marshal(filtered)
	if err != nil {
		return fmt.Errorf("failed to marshal truncated oplog: %w", err)
	}

	if s.Compression {
		newData, err = compressData(newData)
		if err != nil {
			return fmt.Errorf("failed to compress truncated oplog: %w", err)
		}
	}

	_, err = s.writeObject(key, newData)
	if err != nil {
		return fmt.Errorf("failed to write truncated oplog to S3: %w", err)
	}
	return nil
}

// SaveMetadata saves metadata or atomically updates version via CAS using ETag.
func (s *S3SnapshotStore) SaveMetadata(meta *InstanceMeta) (bool, error) {
	key := fmt.Sprintf("instances/%s/meta.json", meta.InstanceID)

	nextVersion := meta.Version + 1
	if meta.Version == 0 {
		nextVersion = 1
	}

	// Create a copy for serialization to exclude the ETag field from the JSON itself
	tempMeta := *meta
	tempMeta.Version = nextVersion
	tempMeta.ETag = ""

	jsonData, err := json.Marshal(tempMeta)
	if err != nil {
		return false, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(jsonData),
	}

	if meta.Version == 0 {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(meta.ETag)
	}

	out, err := s.Client.PutObject(context.Background(), input)
	if err != nil {
		if isPreconditionFailed(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to save metadata to S3: %w", err)
	}

	meta.Version = nextVersion
	if out.ETag != nil {
		meta.ETag = *out.ETag
	}
	return true, nil
}

// LoadMetadata retrieves the instance metadata from S3.
func (s *S3SnapshotStore) LoadMetadata(id string) (*InstanceMeta, error) {
	key := fmt.Sprintf("instances/%s/meta.json", id)
	data, etag, err := s.readObject(key)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load metadata from S3: %w", err)
	}

	var meta InstanceMeta
	err = json.Unmarshal(data, &meta)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	meta.ETag = etag
	return &meta, nil
}

// SaveWasm saves a WASM module binary by its SHA256 hash.
func (s *S3SnapshotStore) SaveWasm(hash string, wasmBytes []byte) error {
	key := fmt.Sprintf("wasm/%s.wasm", hash)
	data := wasmBytes
	if s.Compression {
		var err error
		data, err = compressData(wasmBytes)
		if err != nil {
			return fmt.Errorf("failed to compress WASM module %s: %w", hash, err)
		}
	}
	_, err := s.writeObject(key, data)
	if err != nil {
		return fmt.Errorf("failed to save WASM module %s to S3: %w", hash, err)
	}
	return nil
}

// LoadWasm loads a WASM module binary by its SHA256 hash.
func (s *S3SnapshotStore) LoadWasm(hash string) ([]byte, error) {
	key := fmt.Sprintf("wasm/%s.wasm", hash)
	data, _, err := s.readObject(key)
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("wasm module not found: %s", hash)
		}
		return nil, fmt.Errorf("failed to load WASM module %s from S3: %w", hash, err)
	}
	return decompressData(data)
}

// Delete removes all data associated with the instance from S3.
func (s *S3SnapshotStore) Delete(id string) error {
	keys := []string{
		fmt.Sprintf("instances/%s/snapshot.bin", id),
		fmt.Sprintf("instances/%s/deltas.json", id),
		fmt.Sprintf("instances/%s/oplog.json", id),
		fmt.Sprintf("instances/%s/meta.json", id),
		fmt.Sprintf("instances/%s/active.json", id),
	}

	for _, key := range keys {
		_, err := s.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil && !isNotFound(err) {
			return fmt.Errorf("failed to delete key %s from S3: %w", key, err)
		}
	}
	return nil
}

// UpdateActiveIndex updates the active instance index status.
func (s *S3SnapshotStore) UpdateActiveIndex(id string, info []byte, completed bool) error {
	key := fmt.Sprintf("instances/%s/active.json", id)
	if completed {
		_, err := s.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil && !isNotFound(err) {
			return fmt.Errorf("failed to delete active status for %s from S3: %w", id, err)
		}
		return nil
	}

	_, err := s.Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(info),
	})
	if err != nil {
		return fmt.Errorf("failed to write active status for %s to S3: %w", id, err)
	}
	return nil
}

// LoadActiveIndex loads the compiled active index list from S3.
func (s *S3SnapshotStore) LoadActiveIndex() ([]byte, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String("instances/"),
	}

	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list instances prefix: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil && strings.HasSuffix(*obj.Key, "/active.json") {
				keys = append(keys, *obj.Key)
			}
		}
	}

	if len(keys) == 0 {
		return []byte("[]"), nil
	}

	// Fetch active.json objects in parallel
	type result struct {
		data []byte
		err  error
	}

	resChan := make(chan result, len(keys))
	for _, key := range keys {
		go func(k string) {
			data, _, err := s.readObject(k)
			resChan <- result{data: data, err: err}
		}(key)
	}

	var activeList []map[string]interface{}
	for i := 0; i < len(keys); i++ {
		res := <-resChan
		if res.err != nil {
			return nil, fmt.Errorf("failed to read active instance info: %w", res.err)
		}
		var item map[string]interface{}
		if err := json.Unmarshal(res.data, &item); err != nil {
			return nil, fmt.Errorf("failed to unmarshal active instance info: %w", err)
		}
		activeList = append(activeList, item)
	}

	resultBytes, err := json.Marshal(activeList)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal compiled active index: %w", err)
	}
	return resultBytes, nil
}
