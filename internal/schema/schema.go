package schema

import (
	"errors"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Constants for field names and types
const (
	IDFieldName     = "id"
	VectorFieldName = "vector"
)

var (
	// ErrMissingIDField is returned when the ID field is missing
	ErrMissingIDField = errors.New("schema missing required 'id' field")
	// ErrMissingVectorField is returned when the vector field is missing
	ErrMissingVectorField = errors.New("schema missing required 'vector' field")
	// ErrInvalidIDType is returned when the ID field is not of a supported type
	ErrInvalidIDType = errors.New("id field must be of type string or int64")
	// ErrInvalidVectorType is returned when the vector field is not of list<float32> type
	ErrInvalidVectorType = errors.New("vector field must be of type list<float32>")
)

// Manager manages Arrow schemas for vector collections
type Manager struct {
	allocator    memory.Allocator
	schemaCache  map[string]*arrow.Schema
	schemaMutex  sync.RWMutex
	vectorLength map[string]int
}

// NewManager creates a new schema manager
func NewManager(allocator memory.Allocator) *Manager {
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}

	return &Manager{
		allocator:    allocator,
		schemaCache:  make(map[string]*arrow.Schema),
		vectorLength: make(map[string]int),
	}
}

// Validate validates that a schema meets the requirements for Fletch
func (m *Manager) Validate(schema *arrow.Schema) error {
	// Check for required fields
	idIdx := schema.FieldIndices(IDFieldName)
	if len(idIdx) == 0 {
		return ErrMissingIDField
	}

	vectorIdx := schema.FieldIndices(VectorFieldName)
	if len(vectorIdx) == 0 {
		return ErrMissingVectorField
	}

	// Validate ID field type
	idField := schema.Field(idIdx[0])
	if idField.Type.ID() != arrow.STRING && idField.Type.ID() != arrow.INT64 {
		return ErrInvalidIDType
	}

	// Validate vector field type
	vectorField := schema.Field(vectorIdx[0])
	if vectorField.Type.ID() != arrow.LIST {
		return ErrInvalidVectorType
	}

	// Ensure vector is list<float32>
	listType, ok := vectorField.Type.(*arrow.ListType)
	if !ok || listType.Elem().ID() != arrow.FLOAT32 {
		return ErrInvalidVectorType
	}

	return nil
}

// RegisterSchema registers a schema for a collection
func (m *Manager) RegisterSchema(collectionName string, schema *arrow.Schema, vectorDim int) error {
	if err := m.Validate(schema); err != nil {
		return err
	}

	m.schemaMutex.Lock()
	defer m.schemaMutex.Unlock()

	m.schemaCache[collectionName] = schema
	m.vectorLength[collectionName] = vectorDim

	return nil
}

// GetSchema retrieves a schema for a collection
func (m *Manager) GetSchema(collectionName string) (*arrow.Schema, bool) {
	m.schemaMutex.RLock()
	defer m.schemaMutex.RUnlock()

	schema, ok := m.schemaCache[collectionName]
	return schema, ok
}

// GetVectorDimension retrieves the vector dimension for a collection
func (m *Manager) GetVectorDimension(collectionName string) (int, bool) {
	m.schemaMutex.RLock()
	defer m.schemaMutex.RUnlock()

	dim, ok := m.vectorLength[collectionName]
	return dim, ok
}

// ExtractVectors extracts vectors from a record batch
func ExtractVectors(batch arrow.Record) ([][]float32, error) {
	schema := batch.Schema()
	vectorIdx := schema.FieldIndices(VectorFieldName)
	if len(vectorIdx) == 0 {
		return nil, ErrMissingVectorField
	}

	vectorCol := batch.Column(vectorIdx[0])
	listArray, ok := vectorCol.(*array.List)
	if !ok {
		return nil, ErrInvalidVectorType
	}

	valueArray, ok := listArray.ListValues().(*array.Float32)
	if !ok {
		return nil, ErrInvalidVectorType
	}

	numRows := int(batch.NumRows())
	vectors := make([][]float32, numRows)

	for i := 0; i < numRows; i++ {
		if listArray.IsNull(i) {
			return nil, fmt.Errorf("null vector at row %d", i)
		}

		start := listArray.Offsets()[i]
		end := listArray.Offsets()[i+1]
		length := end - start

		vector := make([]float32, length)
		for j := int32(0); j < length; j++ {
			vector[j] = valueArray.Value(int(start + j))
		}

		vectors[i] = vector
	}

	return vectors, nil
}

// ExtractIDs extracts IDs from a record batch
func ExtractIDs(batch arrow.Record) ([]interface{}, error) {
	schema := batch.Schema()
	idIdx := schema.FieldIndices(IDFieldName)
	if len(idIdx) == 0 {
		return nil, ErrMissingIDField
	}

	idCol := batch.Column(idIdx[0])
	numRows := int(batch.NumRows())
	ids := make([]interface{}, numRows)

	// Extract based on the field type
	switch idArray := idCol.(type) {
	case *array.String:
		for i := 0; i < numRows; i++ {
			if idArray.IsNull(i) {
				return nil, fmt.Errorf("null id at row %d", i)
			}
			ids[i] = idArray.Value(i)
		}
	case *array.Int64:
		for i := 0; i < numRows; i++ {
			if idArray.IsNull(i) {
				return nil, fmt.Errorf("null id at row %d", i)
			}
			ids[i] = idArray.Value(i)
		}
	default:
		return nil, ErrInvalidIDType
	}

	return ids, nil
}

// ExtractPayloads extracts payload fields (all fields except ID and vector) from a record batch
func ExtractPayloads(batch arrow.Record) ([]map[string]interface{}, error) {
	schema := batch.Schema()
	numRows := int(batch.NumRows())
	payloads := make([]map[string]interface{}, numRows)

	// Initialize the maps
	for i := 0; i < numRows; i++ {
		payloads[i] = make(map[string]interface{})
	}

	// Extract each field
	for i, field := range schema.Fields() {
		// Skip ID and vector fields
		if field.Name == IDFieldName || field.Name == VectorFieldName {
			continue
		}

		col := batch.Column(i)
		// Extract values based on field type
		for j := 0; j < numRows; j++ {
			if !col.IsNull(j) {
				// Extract the value based on its type
				switch arr := col.(type) {
				case *array.String:
					payloads[j][field.Name] = arr.Value(j)
				case *array.Int64:
					payloads[j][field.Name] = arr.Value(j)
				case *array.Int32:
					payloads[j][field.Name] = arr.Value(j)
				case *array.Float64:
					payloads[j][field.Name] = arr.Value(j)
				case *array.Float32:
					payloads[j][field.Name] = arr.Value(j)
				case *array.Boolean:
					payloads[j][field.Name] = arr.Value(j)
				// Add more types as needed
				default:
					// Skip unsupported types
					continue
				}
			}
		}
	}

	return payloads, nil
}
