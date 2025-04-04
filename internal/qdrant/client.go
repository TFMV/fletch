package qdrant

import (
	"context"
	"fmt"
	"sync"

	"github.com/TFMV/fletch/internal/config"
	"github.com/qdrant/go-client/qdrant"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a wrapper around Qdrant's client that adds connection pooling
type Client struct {
	pool    []*qdrant.Client
	poolMu  sync.Mutex
	cfg     *config.QdrantConfig
	logger  *zap.Logger
	nextIdx int
}

// NewClient creates a new Qdrant client with connection pooling
func NewClient(cfg *config.QdrantConfig, logger *zap.Logger) (*Client, error) {
	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
	}

	client := &Client{
		pool:   make([]*qdrant.Client, cfg.PoolSize),
		cfg:    cfg,
		logger: logger,
	}

	// Initialize the connection pool
	for i := 0; i < cfg.PoolSize; i++ {
		conn, err := client.createConnection()
		if err != nil {
			return nil, err
		}
		client.pool[i] = conn
	}

	return client, nil
}

// createConnection creates a new connection to Qdrant
func (c *Client) createConnection() (*qdrant.Client, error) {
	var opts []grpc.DialOption

	if c.cfg.UseTLS {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		// TODO: Replace with proper TLS credentials when needed
		// opts = append(opts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Create client config
	clientConfig := &qdrant.Config{
		Host:        c.cfg.Host,
		Port:        int(c.cfg.Port),
		GrpcOptions: opts,
	}

	// Add API key if provided
	if c.cfg.APIKey != "" {
		clientConfig.APIKey = c.cfg.APIKey
	}

	client, err := qdrant.NewClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Qdrant client: %w", err)
	}

	return client, nil
}

// Close closes all connections in the pool
func (c *Client) Close() {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()

	for i, client := range c.pool {
		if client != nil {
			client.Close()
			c.pool[i] = nil
		}
	}
}

// getClient gets a client from the pool in a round-robin fashion
func (c *Client) getClient() *qdrant.Client {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()

	client := c.pool[c.nextIdx]
	c.nextIdx = (c.nextIdx + 1) % len(c.pool)
	return client
}

// CreateCollection creates a new collection in Qdrant
func (c *Client) CreateCollection(ctx context.Context, name string, vectorDimension uint64) error {
	client := c.getClient()
	err := client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: name,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     vectorDimension,
			Distance: qdrant.Distance_Cosine,
		}),
	})
	if err != nil {
		return fmt.Errorf("failed to create collection %s: %w", name, err)
	}
	return nil
}

// CollectionExists checks if a collection exists in Qdrant
func (c *Client) CollectionExists(ctx context.Context, name string) (bool, error) {
	client := c.getClient()
	exists, err := client.CollectionExists(ctx, name)
	if err != nil {
		return false, fmt.Errorf("failed to check if collection exists: %w", err)
	}
	return exists, nil
}

// GetCollectionInfo gets information about a collection
func (c *Client) GetCollectionInfo(ctx context.Context, name string) (*qdrant.CollectionInfo, error) {
	client := c.getClient()
	info, err := client.GetCollectionInfo(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get collection info for %s: %w", name, err)
	}
	return info, nil
}

// UpsertVectors inserts or updates vectors in a collection
func (c *Client) UpsertVectors(ctx context.Context, collection string, points []*qdrant.PointStruct, wait bool) error {
	client := c.getClient()
	_, err := client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collection,
		Points:         points,
		Wait:           &wait,
	})
	if err != nil {
		return fmt.Errorf("failed to upsert vectors to %s: %w", collection, err)
	}
	return nil
}

// Search performs a vector similarity search in Qdrant
func (c *Client) Search(ctx context.Context, collection string, vector []float32, limit uint64, filter *qdrant.Filter, withPayload *qdrant.WithPayloadSelector) ([]*qdrant.ScoredPoint, error) {
	client := c.getClient()
	limitPtr := limit
	resp, err := client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: collection,
		Query:          qdrant.NewQuery(vector...),
		Limit:          &limitPtr,
		Filter:         filter,
		WithPayload:    withPayload,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search vectors in %s: %w", collection, err)
	}
	return resp, nil
}

// DeleteCollection deletes a collection
func (c *Client) DeleteCollection(ctx context.Context, name string) error {
	client := c.getClient()
	err := client.DeleteCollection(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to delete collection %s: %w", name, err)
	}
	return nil
}

// ListCollections lists all collections
func (c *Client) ListCollections(ctx context.Context) ([]string, error) {
	client := c.getClient()
	collections, err := client.ListCollections(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	return collections, nil
}

// CreatePointStructs creates point structs for Qdrant from individual arrays
func CreatePointStructs(ids []interface{}, vectors [][]float32, payloads []map[string]interface{}) ([]*qdrant.PointStruct, error) {
	if len(ids) != len(vectors) || len(ids) != len(payloads) {
		return nil, fmt.Errorf("ids, vectors, and payloads must have the same length")
	}

	points := make([]*qdrant.PointStruct, len(ids))
	for i := 0; i < len(ids); i++ {
		point := &qdrant.PointStruct{}

		// Set ID based on type
		switch id := ids[i].(type) {
		case string:
			point.Id = qdrant.NewID(id)
		case int64:
			point.Id = qdrant.NewIDNum(uint64(id))
		default:
			return nil, fmt.Errorf("unsupported ID type at index %d", i)
		}

		// Set vector
		point.Vectors = qdrant.NewVectorsDense(vectors[i])

		// Set payload
		if len(payloads[i]) > 0 {
			point.Payload = qdrant.NewValueMap(payloads[i])
		}

		points[i] = point
	}

	return points, nil
}
