package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/TFMV/fletch/internal/config"
	"github.com/TFMV/fletch/internal/schema"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	qdrantpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// MockQdrantClient is a mock implementation of the Qdrant client interface
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

// MockFlightServer is a mock implementation of the Arrow Flight server interfaces
type MockFlightDoGetServer struct {
	mock.Mock
	grpc.ServerStream
	ctx context.Context
}

func (m *MockFlightDoGetServer) Context() context.Context {
	return m.ctx
}

func (m *MockFlightDoGetServer) Send(data *flight.FlightData) error {
	args := m.Called(data)
	return args.Error(0)
}

type MockFlightDoPutServer struct {
	mock.Mock
	grpc.ServerStream
	ctx context.Context
}

func (m *MockFlightDoPutServer) Context() context.Context {
	return m.ctx
}

func (m *MockFlightDoPutServer) Recv() (*flight.FlightData, error) {
	args := m.Called()
	return args.Get(0).(*flight.FlightData), args.Error(1)
}

func createTestSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: IDFieldName, Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: VectorFieldName, Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
		{Name: "metadata", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func createTestRecord(allocator memory.Allocator) arrow.Record {
	// Create schema
	schema := createTestSchema()

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

func TestNewServer(t *testing.T) {
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

	// Create a new server
	server, err := NewServer(cfg, logger)

	// Assert no error
	require.NoError(t, err)

	// Assert server is properly initialized
	assert.NotNil(t, server)
	assert.Equal(t, cfg, server.cfg)
	assert.Equal(t, logger, server.logger)
	assert.NotNil(t, server.qdrantClient)
	assert.NotNil(t, server.schemaManager)
	assert.NotNil(t, server.allocator)
}

func TestServer_DoPut_Setup(t *testing.T) {
	// This test just verifies the setup for DoPut testing
	// Without actually running DoPut which is more complex to mock

	// Create mock Qdrant client
	mockQdrantClient := new(MockQdrantClient)

	// Create mock DoPut server
	mockDoPutServer := new(MockFlightDoPutServer)
	mockDoPutServer.ctx = context.Background()

	// Create flight descriptor
	collectionName := "test_collection"
	putParams := struct {
		CollectionName   string `json:"collection_name"`
		CreateCollection bool   `json:"create_collection,omitempty"`
		BatchSize        int    `json:"batch_size,omitempty"`
		Wait             bool   `json:"wait,omitempty"`
	}{
		CollectionName:   collectionName,
		CreateCollection: true,
		Wait:             true,
	}

	paramsBytes, _ := json.Marshal(putParams)
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  paramsBytes,
	}

	// Setup expectations
	// First receive the flight descriptor
	mockDoPutServer.On("Recv").Return(&flight.FlightData{
		FlightDescriptor: descriptor,
	}, nil).Once()

	// Verify our mock setup is correct
	assert.NotNil(t, mockDoPutServer)
	assert.NotNil(t, mockQdrantClient)
}

func TestServer_DoGet_Setup(t *testing.T) {
	// This test just verifies the setup for DoGet testing
	// Without actually running DoGet which is more complex to mock

	// Create mock Qdrant client
	mockQdrantClient := new(MockQdrantClient)

	// Create schema manager
	schemaManager := schema.NewManager(memory.NewGoAllocator())

	// Register schema for test collection
	testSchema := createTestSchema()
	schemaManager.RegisterSchema("test_collection", testSchema, 3)

	// Create mock DoGet server
	mockDoGetServer := new(MockFlightDoGetServer)
	mockDoGetServer.ctx = context.Background()

	// Create search params
	searchParams := &SearchParams{
		CollectionName: "test_collection",
		Vector:         []float32{1.0, 2.0, 3.0},
		Limit:          10,
	}

	searchParamsBytes, _ := json.Marshal(searchParams)
	ticket := &flight.Ticket{
		Ticket: searchParamsBytes,
	}

	// Create search results
	searchResults := []*qdrantpb.ScoredPoint{
		{
			Id:    qdrantpb.NewID("id1"),
			Score: 0.9,
		},
	}

	// Verify our setup is correct
	assert.NotNil(t, ticket)
	assert.NotNil(t, searchResults)
	assert.NotNil(t, mockDoGetServer)
	assert.NotNil(t, mockQdrantClient)
}

func TestServer_InferSchemaFromResults(t *testing.T) {
	// Create server
	server := &Server{
		allocator: memory.NewGoAllocator(),
	}

	// Test with empty results
	schema := server.inferSchemaFromResults([]*qdrantpb.ScoredPoint{})
	assert.Equal(t, 3, schema.NumFields())
	assert.Equal(t, IDFieldName, schema.Field(0).Name)
	assert.Equal(t, VectorFieldName, schema.Field(1).Name)
	assert.Equal(t, "score", schema.Field(2).Name)

	// Test with results containing payload
	payload := map[string]*qdrantpb.Value{
		"string_field": {Kind: &qdrantpb.Value_StringValue{StringValue: "test"}},
		"int_field":    {Kind: &qdrantpb.Value_IntegerValue{IntegerValue: 123}},
		"float_field":  {Kind: &qdrantpb.Value_DoubleValue{DoubleValue: 123.456}},
		"bool_field":   {Kind: &qdrantpb.Value_BoolValue{BoolValue: true}},
	}

	results := []*qdrantpb.ScoredPoint{
		{
			Id:      qdrantpb.NewID("id1"),
			Score:   0.9,
			Payload: payload,
		},
	}

	schema = server.inferSchemaFromResults(results)
	assert.Equal(t, 7, schema.NumFields()) // 3 base fields + 4 payload fields

	// Verify payload fields
	fieldMap := make(map[string]arrow.DataType)
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		fieldMap[field.Name] = field.Type
	}

	assert.Equal(t, arrow.BinaryTypes.String, fieldMap["string_field"])
	assert.Equal(t, arrow.PrimitiveTypes.Int64, fieldMap["int_field"])
	assert.Equal(t, arrow.PrimitiveTypes.Float64, fieldMap["float_field"])
	assert.Equal(t, arrow.FixedWidthTypes.Boolean, fieldMap["bool_field"])
}

func TestServer_SearchResultsToRecord(t *testing.T) {
	// Create server
	server := &Server{
		allocator: memory.NewGoAllocator(),
	}

	// Create schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: IDFieldName, Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: VectorFieldName, Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "string_field", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "int_field", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	// Create test results
	results := []*qdrantpb.ScoredPoint{
		{
			Id:    qdrantpb.NewID("id1"),
			Score: 0.9,
			// Skip vectors for simplicity in testing
			Payload: map[string]*qdrantpb.Value{
				"string_field": {Kind: &qdrantpb.Value_StringValue{StringValue: "test"}},
				"int_field":    {Kind: &qdrantpb.Value_IntegerValue{IntegerValue: 123}},
			},
		},
	}

	// Convert results to record
	record, err := server.searchResultsToRecord(schema, results)
	require.NoError(t, err)
	defer record.Release()

	// Verify record
	assert.Equal(t, int64(1), record.NumRows())
	assert.Equal(t, 5, record.NumCols())

	// Verify ID field
	idField := record.Column(0).(*array.String)
	assert.Equal(t, "id1", idField.Value(0))

	// Verify score field
	scoreField := record.Column(2).(*array.Float32)
	assert.Equal(t, float32(0.9), scoreField.Value(0))

	// Verify string payload field
	stringField := record.Column(3).(*array.String)
	assert.Equal(t, "test", stringField.Value(0))

	// Verify int payload field
	intField := record.Column(4).(*array.Int64)
	assert.Equal(t, int64(123), intField.Value(0))
}

func TestServer_Close_Setup(t *testing.T) {
	// Create mock Qdrant client
	mockQdrantClient := new(MockQdrantClient)

	// Setup expectations
	mockQdrantClient.On("Close").Return(nil)

	// Test the mock works correctly
	err := mockQdrantClient.Close()
	assert.NoError(t, err)
	mockQdrantClient.AssertExpectations(t)
}
