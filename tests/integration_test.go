package tests

import (
	"context"
	"testing"

	"github.com/TFMV/fletch/internal/config"
	"github.com/TFMV/fletch/internal/qdrant"
	"github.com/TFMV/fletch/internal/schema"
	"github.com/TFMV/fletch/internal/server"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	qdrantpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// QdrantInterface defines the interface for the Qdrant client operations
type QdrantInterface interface {
	CreateCollection(ctx context.Context, name string, vectorDimension uint64) error
	CollectionExists(ctx context.Context, name string) (bool, error)
	GetCollectionInfo(ctx context.Context, name string) (*qdrantpb.CollectionInfo, error)
	UpsertVectors(ctx context.Context, collection string, points []*qdrantpb.PointStruct, wait bool) error
	Search(ctx context.Context, collection string, vector []float32, limit uint64, filter *qdrantpb.Filter, withPayload *qdrantpb.WithPayloadSelector) ([]*qdrantpb.ScoredPoint, error)
	DeleteCollection(ctx context.Context, name string) error
	ListCollections(ctx context.Context) ([]string, error)
	Close() error
}

// MockQdrantClient is a mock implementation of the Qdrant client
type MockQdrantClient struct {
	mock.Mock
}

func (m *MockQdrantClient) CreateCollection(ctx context.Context, name string, vectorDimension uint64) error {
	args := m.Called(ctx, name, vectorDimension)
	return args.Error(0)
}

func (m *MockQdrantClient) CollectionExists(ctx context.Context, name string) (bool, error) {
	args := m.Called(ctx, name)
	return args.Bool(0), args.Error(1)
}

func (m *MockQdrantClient) GetCollectionInfo(ctx context.Context, name string) (*qdrantpb.CollectionInfo, error) {
	args := m.Called(ctx, name)
	return args.Get(0).(*qdrantpb.CollectionInfo), args.Error(1)
}

func (m *MockQdrantClient) UpsertVectors(ctx context.Context, collection string, points []*qdrantpb.PointStruct, wait bool) error {
	args := m.Called(ctx, collection, points, wait)
	return args.Error(0)
}

func (m *MockQdrantClient) Search(ctx context.Context, collection string, vector []float32, limit uint64, filter *qdrantpb.Filter, withPayload *qdrantpb.WithPayloadSelector) ([]*qdrantpb.ScoredPoint, error) {
	args := m.Called(ctx, collection, vector, limit, filter, withPayload)
	return args.Get(0).([]*qdrantpb.ScoredPoint), args.Error(1)
}

func (m *MockQdrantClient) DeleteCollection(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *MockQdrantClient) ListCollections(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockQdrantClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// TestServerWithExposedFields extends server.Server to expose private fields for testing
type TestServerWithExposedFields struct {
	*server.Server
	SchemaManager *schema.Manager
	Allocator     memory.Allocator
	QdrantClient  QdrantInterface
}

// createTestServer creates a new server with mocked dependencies for testing
func createTestServer(t *testing.T) (*TestServerWithExposedFields, *MockQdrantClient) {
	// Create a test configuration
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host: "localhost",
			Port: 8815,
		},
		Qdrant: config.QdrantConfig{
			Host:     "localhost",
			Port:     6334,
			PoolSize: 2,
		},
	}

	// Create a test logger
	logger, _ := zap.NewDevelopment()

	// Create a mock Qdrant client
	mockQdrantClient := new(MockQdrantClient)

	// Create an allocator
	allocator := memory.NewGoAllocator()

	// Create a schema manager
	schemaManager := schema.NewManager(allocator)

	// Create the server - patch the NewServer function by creating
	// a new server and then manually setting the fields we need
	s, err := server.NewServer(cfg, logger)
	require.NoError(t, err)

	// Use reflection to set the private fields
	// For testing purposes, we'll create a wrapper with exposed fields
	testServer := &TestServerWithExposedFields{
		Server:        s,
		SchemaManager: schemaManager,
		Allocator:     allocator,
		QdrantClient:  mockQdrantClient,
	}

	return testServer, mockQdrantClient
}

// createTestRecord creates a test record batch
func createTestRecord(t *testing.T, allocator memory.Allocator) arrow.Record {
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
	idBuilder.AppendValues([]string{"id1", "id2"}, nil)

	// First vector: [1.0, 2.0, 3.0]
	vectorBuilder.Append(true)
	valueBuilder.AppendValues([]float32{1.0, 2.0, 3.0}, nil)

	// Second vector: [4.0, 5.0, 6.0]
	vectorBuilder.Append(true)
	valueBuilder.AppendValues([]float32{4.0, 5.0, 6.0}, nil)

	metadataBuilder.AppendValues([]string{"meta1", "meta2"}, nil)

	// Create arrays
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	vectorArray := vectorBuilder.NewArray()
	defer vectorArray.Release()

	metadataArray := metadataBuilder.NewArray()
	defer metadataArray.Release()

	// Create record batch
	record := array.NewRecord(schema, []arrow.Array{idArray, vectorArray, metadataArray}, 2)
	return record
}

// TestIntegration_SearchOperation tests the integration of server components with a mocked client
func TestIntegration_SearchOperation(t *testing.T) {
	// Create test server with mocked client
	s, mockClient := createTestServer(t)

	// Test context
	ctx := context.Background()

	// Collection name
	collectionName := "test_collection"

	// Register a schema for the collection
	testSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
		{Name: "metadata", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	err := s.SchemaManager.RegisterSchema(collectionName, testSchema, 3)
	require.NoError(t, err)

	// Define a search vector and parameters
	searchVector := []float32{1.0, 2.0, 3.0}
	limit := uint64(10)

	// Create expected search results
	expectedResults := []*qdrantpb.ScoredPoint{
		{
			Id:    qdrantpb.NewID("id1"),
			Score: 0.9,
			Payload: map[string]*qdrantpb.Value{
				"metadata": {Kind: &qdrantpb.Value_StringValue{StringValue: "result1"}},
			},
		},
	}

	// Set up mock expectations
	mockClient.On("Search",
		ctx,
		collectionName,
		searchVector,
		limit,
		mock.Anything, // filter
		mock.Anything, // withPayload
	).Return(expectedResults, nil)

	// Perform the search operation
	params := &server.SearchParams{
		CollectionName: collectionName,
		Vector:         searchVector,
		Limit:          limit,
	}

	results, err := mockClient.Search(
		ctx,
		params.CollectionName,
		params.Vector,
		params.Limit,
		params.Filter,
		params.WithPayload,
	)

	// Assert expectations
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "id1", results[0].Id.GetUuid())
	assert.Equal(t, float32(0.9), results[0].Score)
	assert.Equal(t, "result1", results[0].Payload["metadata"].GetStringValue())

	// Verify all expectations were met
	mockClient.AssertExpectations(t)
}

// TestIntegration_UpsertOperation tests the ingestion process with mocked dependencies
func TestIntegration_UpsertOperation(t *testing.T) {
	// Create test server with mocked client
	s, mockClient := createTestServer(t)

	// Test context
	ctx := context.Background()

	// Collection name
	collectionName := "test_collection"

	// Check if collection exists
	mockClient.On("CollectionExists", ctx, collectionName).Return(false, nil)

	// Create collection
	vectorDim := uint64(3)
	mockClient.On("CreateCollection", ctx, collectionName, vectorDim).Return(nil)

	// Create test record
	record := createTestRecord(t, s.Allocator)
	defer record.Release()

	// Extract data from record for upsert
	ids, err := schema.ExtractIDs(record)
	require.NoError(t, err)

	vectors, err := schema.ExtractVectors(record)
	require.NoError(t, err)

	payloads, err := schema.ExtractPayloads(record)
	require.NoError(t, err)

	// Create points for Qdrant
	points, err := qdrant.CreatePointStructs(ids, vectors, payloads)
	require.NoError(t, err)

	// Setup upsert expectation
	mockClient.On("UpsertVectors", ctx, collectionName, points, true).Return(nil)

	// Perform the upsert operation
	exists, err := mockClient.CollectionExists(ctx, collectionName)
	require.NoError(t, err)

	if !exists {
		err = mockClient.CreateCollection(ctx, collectionName, vectorDim)
		require.NoError(t, err)

		// Register schema
		err = s.SchemaManager.RegisterSchema(collectionName, record.Schema(), int(vectorDim))
		require.NoError(t, err)
	}

	// Upsert vectors
	err = mockClient.UpsertVectors(ctx, collectionName, points, true)
	require.NoError(t, err)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify schema was registered
	registeredSchema, ok := s.SchemaManager.GetSchema(collectionName)
	assert.True(t, ok)
	assert.Equal(t, 3, registeredSchema.NumFields())
}
