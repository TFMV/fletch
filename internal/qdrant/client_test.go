package qdrant

import (
	"context"
	"testing"

	"github.com/TFMV/fletch/internal/config"
	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// MockQdrantClient is a mock implementation of the Qdrant client for testing
type MockQdrantClient struct {
	mock.Mock
}

func (m *MockQdrantClient) CreateCollection(ctx context.Context, req *qdrant.CreateCollection) error {
	args := m.Called(ctx, req)
	return args.Error(0)
}

func (m *MockQdrantClient) CollectionExists(ctx context.Context, name string) (bool, error) {
	args := m.Called(ctx, name)
	return args.Bool(0), args.Error(1)
}

func (m *MockQdrantClient) GetCollectionInfo(ctx context.Context, name string) (*qdrant.CollectionInfo, error) {
	args := m.Called(ctx, name)
	return args.Get(0).(*qdrant.CollectionInfo), args.Error(1)
}

func (m *MockQdrantClient) Upsert(ctx context.Context, req *qdrant.UpsertPoints) (*qdrant.PointsOperationResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*qdrant.PointsOperationResponse), args.Error(1)
}

func (m *MockQdrantClient) Query(ctx context.Context, req *qdrant.QueryPoints) ([]*qdrant.ScoredPoint, error) {
	args := m.Called(ctx, req)
	return args.Get(0).([]*qdrant.ScoredPoint), args.Error(1)
}

func (m *MockQdrantClient) DeleteCollection(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *MockQdrantClient) ListCollections(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockQdrantClient) Close() {
	m.Called()
}

// clientWithMocks creates a Client with mocked Qdrant clients
func clientWithMocks(t *testing.T, poolSize int) (*Client, []*MockQdrantClient) {
	cfg := &config.QdrantConfig{
		Host:     "localhost",
		Port:     6334,
		UseTLS:   false,
		PoolSize: poolSize,
	}

	logger, _ := zap.NewDevelopment()

	client := &Client{
		pool:   make([]*qdrant.Client, poolSize),
		cfg:    cfg,
		logger: logger,
	}

	mocks := make([]*MockQdrantClient, poolSize)
	for i := 0; i < poolSize; i++ {
		mockClient := new(MockQdrantClient)
		mocks[i] = mockClient
		client.pool[i] = &qdrant.Client{}
	}

	return client, mocks
}

func TestClient_CreateConnection(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	testCases := []struct {
		name     string
		config   *config.QdrantConfig
		wantHost string
		wantPort int
		wantTLS  bool
		wantKey  string
	}{
		{
			name: "default config",
			config: &config.QdrantConfig{
				Host:     "localhost",
				Port:     6334,
				UseTLS:   false,
				PoolSize: 2,
			},
			wantHost: "localhost",
			wantPort: 6334,
			wantTLS:  false,
			wantKey:  "",
		},
		{
			name: "with API key",
			config: &config.QdrantConfig{
				Host:     "qdrant.example.com",
				Port:     8080,
				UseTLS:   true,
				APIKey:   "test-api-key",
				PoolSize: 3,
			},
			wantHost: "qdrant.example.com",
			wantPort: 8080,
			wantTLS:  true,
			wantKey:  "test-api-key",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &Client{
				cfg:    tc.config,
				logger: logger,
			}

			// This only tests that createConnection doesn't crash
			// since we can't verify the internal state of the generated client
			conn, err := client.createConnection()
			require.NoError(t, err)
			assert.NotNil(t, conn)
		})
	}
}

func TestClient_GetClient(t *testing.T) {
	client, _ := clientWithMocks(t, 3)

	// Test round-robin behavior
	firstClient := client.getClient()
	secondClient := client.getClient()
	thirdClient := client.getClient()
	fourthClient := client.getClient() // Should wrap around to the first

	assert.Equal(t, client.pool[0], firstClient)
	assert.Equal(t, client.pool[1], secondClient)
	assert.Equal(t, client.pool[2], thirdClient)
	assert.Equal(t, client.pool[0], fourthClient) // Should have wrapped around
}

func TestClient_CollectionExists(t *testing.T) {
	client, mocks := clientWithMocks(t, 1)
	ctx := context.Background()

	// Set up expectations
	mocks[0].On("CollectionExists", ctx, "test_collection").Return(true, nil)

	// Call the method
	exists, err := client.CollectionExists(ctx, "test_collection")

	// Verify
	assert.NoError(t, err)
	assert.True(t, exists)
	mocks[0].AssertExpectations(t)
}

func TestClient_CreateCollection(t *testing.T) {
	client, mocks := clientWithMocks(t, 1)
	ctx := context.Background()

	vectorSize := uint64(128)

	// Set up expectations for the first client in the pool
	mocks[0].On("CreateCollection", ctx, mock.MatchedBy(func(req *qdrant.CreateCollection) bool {
		return req.CollectionName == "test_collection" &&
			req.VectorsConfig != nil &&
			req.VectorsConfig.GetParams().Size == vectorSize
	})).Return(nil)

	// Call the method
	err := client.CreateCollection(ctx, "test_collection", vectorSize)

	// Verify
	assert.NoError(t, err)
	mocks[0].AssertExpectations(t)
}

func TestClient_UpsertVectors(t *testing.T) {
	client, mocks := clientWithMocks(t, 1)
	ctx := context.Background()
	collectionName := "test_collection"

	// Create test points
	points := []*qdrant.PointStruct{
		{
			Id:      qdrant.NewID("id1"),
			Vectors: qdrant.NewVectorsDense([]float32{1.0, 2.0, 3.0}),
		},
		{
			Id:      qdrant.NewID("id2"),
			Vectors: qdrant.NewVectorsDense([]float32{4.0, 5.0, 6.0}),
		},
	}

	// Set up expectations
	waitTrue := true
	mocks[0].On("Upsert", ctx, mock.MatchedBy(func(req *qdrant.UpsertPoints) bool {
		return req.CollectionName == collectionName &&
			req.Wait != nil && *req.Wait == true &&
			len(req.Points) == 2
	})).Return(&qdrant.PointsOperationResponse{}, nil)

	// Call the method
	err := client.UpsertVectors(ctx, collectionName, points, waitTrue)

	// Verify
	assert.NoError(t, err)
	mocks[0].AssertExpectations(t)
}

func TestClient_Search(t *testing.T) {
	client, mocks := clientWithMocks(t, 1)
	ctx := context.Background()
	collectionName := "test_collection"
	vector := []float32{1.0, 2.0, 3.0}
	limit := uint64(10)

	// Expected results
	expectedResults := []*qdrant.ScoredPoint{
		{
			Id:    qdrant.NewID("id1"),
			Score: 0.9,
			// We don't need to test vectors in the result for this test
			// Avoid constructing complex objects that can cause linter issues
		},
	}

	// Set up expectations
	mocks[0].On("Query", ctx, mock.MatchedBy(func(req *qdrant.QueryPoints) bool {
		return req.CollectionName == collectionName &&
			req.Limit != nil && *req.Limit == limit
	})).Return(expectedResults, nil)

	// Call the method
	results, err := client.Search(ctx, collectionName, vector, limit, nil, nil)

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, expectedResults, results)
	mocks[0].AssertExpectations(t)
}

func TestClient_DeleteCollection(t *testing.T) {
	client, mocks := clientWithMocks(t, 1)
	ctx := context.Background()
	collectionName := "test_collection"

	// Set up expectations
	mocks[0].On("DeleteCollection", ctx, collectionName).Return(nil)

	// Call the method
	err := client.DeleteCollection(ctx, collectionName)

	// Verify
	assert.NoError(t, err)
	mocks[0].AssertExpectations(t)
}

func TestClient_ListCollections(t *testing.T) {
	client, mocks := clientWithMocks(t, 1)
	ctx := context.Background()
	expectedCollections := []string{"collection1", "collection2"}

	// Set up expectations
	mocks[0].On("ListCollections", ctx).Return(expectedCollections, nil)

	// Call the method
	collections, err := client.ListCollections(ctx)

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, expectedCollections, collections)
	mocks[0].AssertExpectations(t)
}

func TestClient_Close(t *testing.T) {
	client, mocks := clientWithMocks(t, 3)

	// Set up expectations for each mock
	for _, m := range mocks {
		m.On("Close").Return()
	}

	// Call the method
	client.Close()

	// Verify that Close was called on all the mocks
	for _, m := range mocks {
		m.AssertExpectations(t)
	}
}

func TestCreatePointStructs(t *testing.T) {
	// Test data
	ids := []interface{}{"id1", int64(2)}
	vectors := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
	}
	payloads := []map[string]interface{}{
		{"name": "Alice", "age": int64(30)},
		{"name": "Bob", "age": int64(40)},
	}

	// Create points
	points, err := CreatePointStructs(ids, vectors, payloads)
	require.NoError(t, err)
	require.Len(t, points, 2)

	// Verify first point
	assert.Equal(t, "id1", points[0].Id.GetUuid())
	assert.Equal(t, []float32{1.0, 2.0, 3.0}, points[0].Vectors.GetVector().GetData())
	assert.Equal(t, "Alice", points[0].Payload["name"].GetStringValue())
	assert.Equal(t, int64(30), points[0].Payload["age"].GetIntegerValue())

	// Verify second point
	assert.Equal(t, uint64(2), points[1].Id.GetNum())
	assert.Equal(t, []float32{4.0, 5.0, 6.0}, points[1].Vectors.GetVector().GetData())
	assert.Equal(t, "Bob", points[1].Payload["name"].GetStringValue())
	assert.Equal(t, int64(40), points[1].Payload["age"].GetIntegerValue())

	// Test validation errors
	_, err = CreatePointStructs([]interface{}{"id1"}, vectors, payloads)
	assert.Error(t, err, "should fail when ids length doesn't match vectors length")

	_, err = CreatePointStructs(ids, [][]float32{{1.0}}, payloads)
	assert.Error(t, err, "should fail when vectors length doesn't match ids length")

	_, err = CreatePointStructs([]interface{}{1.0, "id2"}, vectors, payloads)
	assert.Error(t, err, "should fail with unsupported ID type")
}
