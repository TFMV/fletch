package tests

import (
	"context"
	"testing"
	"time"

	"github.com/TFMV/fletch/internal/qdrant"
	"github.com/TFMV/fletch/internal/schema"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	qdrantpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// createLargeTestRecord creates a test record batch with many rows for batch processing tests
func createLargeTestRecord(t *testing.T, allocator memory.Allocator, numRows int) arrow.Record {
	// Create schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
		{Name: "metadata", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Create builders
	idBuilder := array.NewStringBuilder(allocator)
	defer idBuilder.Release()

	vectorBuilder := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Float32)
	defer vectorBuilder.Release()
	valueBuilder := vectorBuilder.ValueBuilder().(*array.Float32Builder)

	metadataBuilder := array.NewStringBuilder(allocator)
	defer metadataBuilder.Release()

	// Add data
	ids := make([]string, numRows)
	metadatas := make([]string, numRows)

	for i := 0; i < numRows; i++ {
		ids[i] = "id" + string(rune(i+48)) // id0, id1, etc.
		metadatas[i] = "meta" + string(rune(i+48))
	}

	idBuilder.AppendValues(ids, nil)
	metadataBuilder.AppendValues(metadatas, nil)

	// Sample vector dimension
	vectorDim := 3

	// Generate vectors
	for i := 0; i < numRows; i++ {
		vectorBuilder.Append(true)

		vector := make([]float32, vectorDim)
		for j := 0; j < vectorDim; j++ {
			vector[j] = float32(i + j)
		}

		valueBuilder.AppendValues(vector, nil)
	}

	// Create arrays
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	vectorArray := vectorBuilder.NewArray()
	defer vectorArray.Release()

	metadataArray := metadataBuilder.NewArray()
	defer metadataArray.Release()

	// Create record batch
	record := array.NewRecord(schema, []arrow.Array{idArray, vectorArray, metadataArray}, int64(numRows))
	return record
}

// BatchProcessingMockClient is a mock implementation that tracks batch inserts
type BatchProcessingMockClient struct {
	mock.Mock
	batches       int
	processedRows int
	lastBatchTime time.Time
	progressCalls int
}

func (m *BatchProcessingMockClient) CreateCollection(ctx context.Context, name string, vectorDimension uint64) error {
	args := m.Called(ctx, name, vectorDimension)
	return args.Error(0)
}

func (m *BatchProcessingMockClient) CollectionExists(ctx context.Context, name string) (bool, error) {
	args := m.Called(ctx, name)
	return args.Bool(0), args.Error(1)
}

func (m *BatchProcessingMockClient) GetCollectionInfo(ctx context.Context, name string) (*qdrantpb.CollectionInfo, error) {
	args := m.Called(ctx, name)
	return args.Get(0).(*qdrantpb.CollectionInfo), args.Error(1)
}

func (m *BatchProcessingMockClient) UpsertVectors(ctx context.Context, collection string, points []*qdrantpb.PointStruct, wait bool) error {
	args := m.Called(ctx, collection, points, wait)

	// Track batch metrics
	m.batches++
	m.processedRows += len(points)
	m.lastBatchTime = time.Now()

	return args.Error(0)
}

func (m *BatchProcessingMockClient) Search(ctx context.Context, collection string, vector []float32, limit uint64, filter *qdrantpb.Filter, withPayload *qdrantpb.WithPayloadSelector) ([]*qdrantpb.ScoredPoint, error) {
	args := m.Called(ctx, collection, vector, limit, filter, withPayload)
	return args.Get(0).([]*qdrantpb.ScoredPoint), args.Error(1)
}

func (m *BatchProcessingMockClient) DeleteCollection(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *BatchProcessingMockClient) ListCollections(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *BatchProcessingMockClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *BatchProcessingMockClient) ReportProgress(processed, total int) {
	m.progressCalls++
}

// TestBatchProcessing tests that large record batches are processed in smaller chunks
// with progress reporting
func TestBatchProcessing(t *testing.T) {
	// Create allocator
	allocator := memory.NewGoAllocator()

	// Create schema manager
	schemaManager := schema.NewManager(allocator)

	// We don't use the config or logger in this test, so we can skip creating them
	// and focus on the mock client directly

	// Create special mock client for batch processing
	mockClient := new(BatchProcessingMockClient)

	// Test context
	ctx := context.Background()

	// Collection name
	collectionName := "test_batch_collection"

	// Set up expectations
	mockClient.On("CollectionExists", ctx, collectionName).Return(false, nil)
	mockClient.On("CreateCollection", ctx, collectionName, uint64(3)).Return(nil)

	// For upsert, allow any number of points
	mockClient.On("UpsertVectors", ctx, collectionName, mock.Anything, true).Return(nil)

	// Create large test record - 1000 rows
	record := createLargeTestRecord(t, allocator, 1000)
	defer record.Release()

	// Extract data for assertions
	ids, err := schema.ExtractIDs(record)
	require.NoError(t, err)

	vectors, err := schema.ExtractVectors(record)
	require.NoError(t, err)

	payloads, err := schema.ExtractPayloads(record)
	require.NoError(t, err)

	// Process the batch with custom batch size
	batchSize := 100
	totalBatches := (len(ids) + batchSize - 1) / batchSize // Ceiling division

	// Create collection if it doesn't exist
	exists, err := mockClient.CollectionExists(ctx, collectionName)
	require.NoError(t, err)

	if !exists {
		err = mockClient.CreateCollection(ctx, collectionName, 3)
		require.NoError(t, err)

		// Register schema
		err = schemaManager.RegisterSchema(collectionName, record.Schema(), 3)
		require.NoError(t, err)
	}

	// Process in batches
	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}

		batchIDs := ids[i:end]
		batchVectors := vectors[i:end]
		batchPayloads := payloads[i:end]

		points, err := qdrant.CreatePointStructs(batchIDs, batchVectors, batchPayloads)
		require.NoError(t, err)

		err = mockClient.UpsertVectors(ctx, collectionName, points, true)
		require.NoError(t, err)

		// Report progress
		mockClient.ReportProgress(end, len(ids))
	}

	// Assert expectations
	mockClient.AssertExpectations(t)

	// Verify batch processing
	assert.Equal(t, totalBatches, mockClient.batches, "Should have processed the correct number of batches")
	assert.Equal(t, len(ids), mockClient.processedRows, "Should have processed all rows")
	assert.Greater(t, mockClient.progressCalls, 0, "Progress should have been reported")
}
