package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"

	"github.com/TFMV/fletch/internal/config"
	"github.com/TFMV/fletch/internal/qdrant"
	"github.com/TFMV/fletch/internal/schema"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	qdrantpb "github.com/qdrant/go-client/qdrant"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Constants for field names
const (
	IDFieldName      = "id"
	VectorFieldName  = "vector"
	DefaultBatchSize = 100 // Default batch size for processing records
)

// SearchParams represents the parameters for a vector search operation
type SearchParams struct {
	CollectionName string                        `json:"collection_name"`
	Vector         []float32                     `json:"vector,omitempty"`
	Limit          uint64                        `json:"limit"`
	Filter         *qdrantpb.Filter              `json:"filter,omitempty"`
	ScoreThreshold *float32                      `json:"score_threshold,omitempty"`
	WithPayload    *qdrantpb.WithPayloadSelector `json:"with_payload,omitempty"`
}

// ProgressReporter is an interface for reporting progress during batch operations
type ProgressReporter interface {
	// ReportProgress reports progress with the number of processed items and total items
	ReportProgress(processed, total int)
}

// BulkImportParams represents parameters for bulk import operations
type BulkImportParams struct {
	CollectionName   string `json:"collection_name"`
	CreateCollection bool   `json:"create_collection,omitempty"`
	BatchSize        int    `json:"batch_size,omitempty"`
	Wait             bool   `json:"wait,omitempty"`
	ReportProgress   bool   `json:"report_progress,omitempty"`
}

// Server is the Flight server that connects Arrow Flight with Qdrant
type Server struct {
	cfg           *config.Config
	logger        *zap.Logger
	qdrantClient  *qdrant.Client
	schemaManager *schema.Manager
	allocator     memory.Allocator
	server        *grpc.Server
	progressCh    chan struct {
		processed int
		total     int
	}
	flight.BaseFlightServer
}

// NewServer creates a new server instance
func NewServer(cfg *config.Config, logger *zap.Logger) (*Server, error) {
	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
	}

	qdrantClient, err := qdrant.NewClient(&cfg.Qdrant, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Qdrant client: %w", err)
	}

	allocator := memory.NewGoAllocator()
	schemaManager := schema.NewManager(allocator)

	s := &Server{
		cfg:           cfg,
		logger:        logger,
		qdrantClient:  qdrantClient,
		schemaManager: schemaManager,
		allocator:     allocator,
		progressCh:    make(chan struct{ processed, total int }, 100), // Buffer for progress updates
	}

	return s, nil
}

// Serve starts the Flight server
func (s *Server) Serve() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.cfg.Server.Host, s.cfg.Server.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
	)

	flight.RegisterFlightServiceServer(grpcServer, s)
	s.server = grpcServer

	s.logger.Info("Starting Flight server",
		zap.String("host", s.cfg.Server.Host),
		zap.Int("port", s.cfg.Server.Port),
	)

	// Start progress reporting goroutine
	go s.handleProgressUpdates()

	return grpcServer.Serve(listener)
}

// handleProgressUpdates processes progress updates in a separate goroutine
func (s *Server) handleProgressUpdates() {
	for progress := range s.progressCh {
		s.logger.Info("Processing progress",
			zap.Int("processed", progress.processed),
			zap.Int("total", progress.total),
			zap.Float64("percentage", float64(progress.processed*100)/float64(progress.total)),
		)
	}
}

// ReportProgress implements ProgressReporter interface
func (s *Server) ReportProgress(processed, total int) {
	// Non-blocking send to avoid deadlocks if channel buffer is full
	select {
	case s.progressCh <- struct{ processed, total int }{processed, total}:
		// Sent successfully
	default:
		// Channel full, log and continue
		s.logger.Debug("Progress channel full, skipping update",
			zap.Int("processed", processed),
			zap.Int("total", total),
		)
	}
}

// Close closes the server and releases resources
func (s *Server) Close() error {
	// Close progress channel
	close(s.progressCh)

	if s.qdrantClient != nil {
		s.qdrantClient.Close()
	}
	return nil
}

// DoPut handles the DoPut RPC, which ingests Arrow Tables into Qdrant
func (s *Server) DoPut(stream flight.FlightService_DoPutServer) error {
	ctx := stream.Context()

	// Get the FlightDescriptor from the first message
	firstMessage, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("error receiving first message: %w", err)
	}

	// Extract collection name from descriptor
	descriptor := firstMessage.FlightDescriptor
	if descriptor == nil {
		return fmt.Errorf("missing flight descriptor")
	}

	// Parse parameters from descriptor
	var params BulkImportParams

	if err := json.Unmarshal(descriptor.Cmd, &params); err != nil {
		return fmt.Errorf("invalid descriptor command: %w", err)
	}

	if params.CollectionName == "" {
		return fmt.Errorf("collection_name is required")
	}

	if params.BatchSize <= 0 {
		params.BatchSize = DefaultBatchSize // Default batch size
	}

	// Create a reader for the incoming data
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return fmt.Errorf("error creating record reader: %w", err)
	}
	defer reader.Release()

	// Check if collection exists
	exists, err := s.qdrantClient.CollectionExists(ctx, params.CollectionName)
	if err != nil {
		return fmt.Errorf("error checking collection existence: %w", err)
	}

	// To track total processed records
	processedRecords := 0
	var recordsToProcess int

	if !exists {
		if !params.CreateCollection {
			return fmt.Errorf("collection %s does not exist", params.CollectionName)
		}

		// Wait for the first record to get the schema and vector dimension
		rec, err := reader.Read()
		if err != nil {
			return fmt.Errorf("error reading data: %w", err)
		}

		arrowSchema := rec.Schema()

		// Validate the schema
		if err := s.schemaManager.Validate(arrowSchema); err != nil {
			return fmt.Errorf("invalid schema: %w", err)
		}

		// Extract vectors to determine dimension
		vectors, err := schema.ExtractVectors(rec)
		if err != nil {
			return fmt.Errorf("failed to extract vectors: %w", err)
		}

		if len(vectors) == 0 || len(vectors[0]) == 0 {
			return fmt.Errorf("no vector data found")
		}

		vectorDim := uint64(len(vectors[0]))

		// Create the collection
		err = s.qdrantClient.CreateCollection(ctx, params.CollectionName, vectorDim)
		if err != nil {
			return fmt.Errorf("failed to create collection: %w", err)
		}

		// Register the schema
		err = s.schemaManager.RegisterSchema(params.CollectionName, arrowSchema, int(vectorDim))
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}

		// Process the first batch we already read
		recordsToProcess = 1 // At least one record
		err = s.processBatchWithProgress(ctx, params.CollectionName, rec, params.Wait, params.ReportProgress, processedRecords, recordsToProcess)
		if err != nil {
			return fmt.Errorf("failed to process batch: %w", err)
		}
		processedRecords++
	}

	// Process all remaining batches
	for {
		rec, err := reader.Read()
		if err != nil {
			break // End of stream or error
		}

		recordsToProcess++

		err = s.processBatchWithProgress(ctx, params.CollectionName, rec, params.Wait, params.ReportProgress, processedRecords, recordsToProcess)
		if err != nil {
			return fmt.Errorf("failed to process batch %d: %w", processedRecords, err)
		}
		processedRecords++
	}

	return nil
}

// processBatchWithProgress processes a batch of records with optional progress reporting
func (s *Server) processBatchWithProgress(ctx context.Context, collectionName string, record arrow.Record, wait bool, reportProgress bool, processed int, total int) error {
	// For large batches, we'll process in smaller chunks to avoid overwhelming Qdrant
	batchSize := DefaultBatchSize

	// Extract all IDs, vectors, and payloads from the record
	ids, err := schema.ExtractIDs(record)
	if err != nil {
		return fmt.Errorf("failed to extract IDs: %w", err)
	}

	vectors, err := schema.ExtractVectors(record)
	if err != nil {
		return fmt.Errorf("failed to extract vectors: %w", err)
	}

	payloads, err := schema.ExtractPayloads(record)
	if err != nil {
		return fmt.Errorf("failed to extract payloads: %w", err)
	}

	// Calculate number of points in this record
	pointCount := len(ids)

	// Process in smaller batches if needed
	for i := 0; i < pointCount; i += batchSize {
		end := i + batchSize
		if end > pointCount {
			end = len(ids)
		}

		// Extract batch
		batchIDs := ids[i:end]
		batchVectors := vectors[i:end]
		batchPayloads := payloads[i:end]

		// Create point structs for Qdrant
		points, err := qdrant.CreatePointStructs(batchIDs, batchVectors, batchPayloads)
		if err != nil {
			return fmt.Errorf("failed to create point structs: %w", err)
		}

		// Upsert the points to Qdrant
		err = s.qdrantClient.UpsertVectors(ctx, collectionName, points, wait)
		if err != nil {
			return fmt.Errorf("failed to upsert vectors: %w", err)
		}

		// Report progress if requested
		if reportProgress {
			processed := processed*pointCount + i + len(batchIDs)
			total := total * pointCount
			s.ReportProgress(processed, total)
		}
	}

	return nil
}

// processBatch processes a batch of records and inserts them into Qdrant
func (s *Server) processBatch(ctx context.Context, collectionName string, record arrow.Record, wait bool) error {
	// Use our enhanced batch processing method with default parameters
	return s.processBatchWithProgress(ctx, collectionName, record, wait, false, 0, 1)
}

// DoGet handles the DoGet RPC, which performs vector similarity searches in Qdrant
func (s *Server) DoGet(request *flight.Ticket, server flight.FlightService_DoGetServer) error {
	ctx := server.Context()

	// Parse search parameters from ticket
	var params SearchParams
	if err := json.Unmarshal(request.Ticket, &params); err != nil {
		return fmt.Errorf("invalid search parameters: %w", err)
	}

	if params.CollectionName == "" {
		return fmt.Errorf("collection_name is required")
	}

	if params.Vector == nil || len(params.Vector) == 0 {
		return fmt.Errorf("vector is required")
	}

	if params.Limit == 0 {
		params.Limit = 10 // Default limit
	}

	// Perform the search
	results, err := s.qdrantClient.Search(
		ctx,
		params.CollectionName,
		params.Vector,
		params.Limit,
		params.Filter,
		params.WithPayload,
	)
	if err != nil {
		return fmt.Errorf("search failed: %w", err)
	}

	if len(results) == 0 {
		// If no results, return an empty stream
		s.logger.Info("No search results found")
		return nil
	}

	// Auto-infer schema from results if needed
	var resultSchema *arrow.Schema
	registeredSchema, schemaExists := s.schemaManager.GetSchema(params.CollectionName)
	if !schemaExists {
		// Auto-infer schema from the first result
		s.logger.Info("No schema registered for collection, inferring from results")
		resultSchema = s.inferSchemaFromResults(results)

		// Register the inferred schema
		vectorDim, _ := s.schemaManager.GetVectorDimension(params.CollectionName)
		if vectorDim <= 0 {
			// Use the query vector dimension as fallback
			vectorDim = len(params.Vector)
		}

		// Register the inferred schema
		if err := s.schemaManager.RegisterSchema(params.CollectionName, resultSchema, vectorDim); err != nil {
			return fmt.Errorf("failed to register inferred schema: %w", err)
		}
	} else {
		resultSchema = registeredSchema
	}

	// Convert results to an Arrow Record
	record, err := s.searchResultsToRecord(resultSchema, results)
	if err != nil {
		return fmt.Errorf("failed to convert search results to record: %w", err)
	}
	defer record.Release()

	// Create a writer to send the schema and record batch
	writer := flight.NewRecordWriter(server, ipc.WithSchema(resultSchema))
	defer writer.Close()

	// Write the record batch
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	return nil
}

// inferSchemaFromResults infers an Arrow schema from search results
func (s *Server) inferSchemaFromResults(results []*qdrantpb.ScoredPoint) *arrow.Schema {
	if len(results) == 0 {
		return nil
	}

	// Start with standard fields
	fields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
	}

	// Track payload fields we've already processed
	processedFields := make(map[string]arrow.DataType)

	// Examine all results to infer payload field types
	for _, point := range results {
		if point.Payload == nil {
			continue
		}

		for fieldName, value := range point.Payload {
			if _, exists := processedFields[fieldName]; exists {
				continue // Already processed this field
			}

			// Determine Arrow type based on Qdrant value kind
			var fieldType arrow.DataType

			// Use type switching since GetX() methods aren't available
			switch value.GetKind().(type) {
			case *qdrantpb.Value_StringValue:
				fieldType = arrow.BinaryTypes.String
			case *qdrantpb.Value_IntegerValue:
				fieldType = arrow.PrimitiveTypes.Int64
			case *qdrantpb.Value_DoubleValue:
				fieldType = arrow.PrimitiveTypes.Float64
			case *qdrantpb.Value_BoolValue:
				fieldType = &arrow.BooleanType{}
			default:
				// Skip unknown types
				continue
			}

			processedFields[fieldName] = fieldType
			fields = append(fields, arrow.Field{
				Name:     fieldName,
				Type:     fieldType,
				Nullable: true,
			})
		}
	}

	return arrow.NewSchema(fields, nil)
}

// searchResultsToRecord converts Qdrant search results to an Arrow Record
func (s *Server) searchResultsToRecord(schema *arrow.Schema, results []*qdrantpb.ScoredPoint) (arrow.Record, error) {
	// Create record builders for each field
	numResults := len(results)
	builders := make([]array.Builder, schema.NumFields())
	arrays := make([]arrow.Array, schema.NumFields())

	// Initialize builders for each field
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(s.allocator, field.Type)
		defer builders[i].Release()
	}

	// Find field indices
	idFieldIdx := -1
	scoreFieldIdx := -1
	for i, field := range schema.Fields() {
		if field.Name == "id" {
			idFieldIdx = i
		} else if field.Name == "score" {
			scoreFieldIdx = i
		}
	}

	if idFieldIdx == -1 {
		return nil, fmt.Errorf("schema missing required 'id' field")
	}

	// Append values for each result
	for _, result := range results {
		// ID field is always present
		if idBuilder, ok := builders[idFieldIdx].(*array.StringBuilder); ok {
			// Check if the ID is a UUID or numeric
			if uuid, ok := result.Id.GetPointIdOptions().(*qdrantpb.PointId_Uuid); ok {
				idBuilder.Append(uuid.Uuid)
			} else {
				// If not UUID, convert to string
				idBuilder.Append(fmt.Sprintf("%v", result.Id))
			}
		} else if idBuilder, ok := builders[idFieldIdx].(*array.Int64Builder); ok {
			if num, ok := result.Id.GetPointIdOptions().(*qdrantpb.PointId_Num); ok {
				idBuilder.Append(int64(num.Num))
			} else {
				// If not numeric, append zero and log warning
				idBuilder.Append(0)
				s.logger.Warn("ID type mismatch, expected numeric ID but got UUID")
			}
		}

		// Score field (if present in schema)
		if scoreFieldIdx != -1 {
			if scoreBuilder, ok := builders[scoreFieldIdx].(*array.Float32Builder); ok {
				scoreBuilder.Append(result.Score)
			}
		}

		// Process payload fields
		for i, field := range schema.Fields() {
			// Skip id and score fields, already handled
			if i == idFieldIdx || i == scoreFieldIdx {
				continue
			}

			// Get the field value from the payload
			payloadValue, exists := result.Payload[field.Name]
			if !exists || payloadValue == nil {
				// Field not present, append null
				builders[i].AppendNull()
				continue
			}

			// Append the value based on its type
			switch builder := builders[i].(type) {
			case *array.StringBuilder:
				if strVal, ok := payloadValue.GetKind().(*qdrantpb.Value_StringValue); ok {
					builder.Append(strVal.StringValue)
				} else {
					builder.AppendNull()
				}
			case *array.Int64Builder:
				if intVal, ok := payloadValue.GetKind().(*qdrantpb.Value_IntegerValue); ok {
					builder.Append(intVal.IntegerValue)
				} else {
					builder.AppendNull()
				}
			case *array.Float64Builder:
				if doubleVal, ok := payloadValue.GetKind().(*qdrantpb.Value_DoubleValue); ok {
					builder.Append(doubleVal.DoubleValue)
				} else {
					builder.AppendNull()
				}
			case *array.BooleanBuilder:
				if boolVal, ok := payloadValue.GetKind().(*qdrantpb.Value_BoolValue); ok {
					builder.Append(boolVal.BoolValue)
				} else {
					builder.AppendNull()
				}
			default:
				// Unsupported type, append null
				builders[i].AppendNull()
			}
		}
	}

	// Create arrays from builders
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	// Create record batch
	return array.NewRecord(schema, arrays, int64(numResults)), nil
}

// DoExchange handles bidirectional exchange of data (not implemented)
func (s *Server) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	return fmt.Errorf("DoExchange not implemented")
}

// ListFlights lists available flights (not implemented)
func (s *Server) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	return fmt.Errorf("ListFlights not implemented")
}

// GetFlightInfo returns information about a flight (not implemented)
func (s *Server) GetFlightInfo(ctx context.Context, descriptor *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, fmt.Errorf("GetFlightInfo not implemented")
}

// GetSchema returns the schema for a flight (not implemented)
func (s *Server) GetSchema(ctx context.Context, descriptor *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return nil, fmt.Errorf("GetSchema not implemented")
}
