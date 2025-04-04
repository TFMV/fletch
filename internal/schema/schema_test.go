package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_Validate(t *testing.T) {
	allocator := memory.NewGoAllocator()
	manager := NewManager(allocator)

	tests := []struct {
		name    string
		schema  *arrow.Schema
		wantErr error
	}{
		{
			name: "valid schema with string ID",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
				{Name: "metadata", Type: arrow.BinaryTypes.String, Nullable: true},
			}, nil),
			wantErr: nil,
		},
		{
			name: "valid schema with int64 ID",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
				{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
			}, nil),
			wantErr: nil,
		},
		{
			name: "missing ID field",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
			}, nil),
			wantErr: ErrMissingIDField,
		},
		{
			name: "missing vector field",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
			}, nil),
			wantErr: ErrMissingVectorField,
		},
		{
			name: "invalid ID type",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
				{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
			}, nil),
			wantErr: ErrInvalidIDType,
		},
		{
			name: "invalid vector type - not list",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "vector", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
			}, nil),
			wantErr: ErrInvalidVectorType,
		},
		{
			name: "invalid vector type - wrong element type",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: false},
			}, nil),
			wantErr: ErrInvalidVectorType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := manager.Validate(tt.schema)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestManager_RegisterAndGetSchema(t *testing.T) {
	allocator := memory.NewGoAllocator()
	manager := NewManager(allocator)

	// Create a test schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
		{Name: "metadata", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Test registering a schema
	err := manager.RegisterSchema("test_collection", schema, 128)
	require.NoError(t, err)

	// Test retrieving a schema
	retrievedSchema, ok := manager.GetSchema("test_collection")
	assert.True(t, ok)
	assert.Equal(t, schema, retrievedSchema)

	// Test retrieving a non-existent schema
	_, ok = manager.GetSchema("nonexistent_collection")
	assert.False(t, ok)

	// Test retrieving vector dimension
	dim, ok := manager.GetVectorDimension("test_collection")
	assert.True(t, ok)
	assert.Equal(t, 128, dim)

	// Test retrieving a non-existent vector dimension
	_, ok = manager.GetVectorDimension("nonexistent_collection")
	assert.False(t, ok)
}

func createTestRecord(t *testing.T, allocator memory.Allocator) arrow.Record {
	// Create schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	// Create builders
	idBuilder := array.NewStringBuilder(allocator)
	defer idBuilder.Release()

	vectorBuilder := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Float32)
	defer vectorBuilder.Release()
	valueBuilder := vectorBuilder.ValueBuilder().(*array.Float32Builder)

	nameBuilder := array.NewStringBuilder(allocator)
	defer nameBuilder.Release()

	ageBuilder := array.NewInt32Builder(allocator)
	defer ageBuilder.Release()

	// Add data
	idBuilder.AppendValues([]string{"id1", "id2"}, nil)

	// First vector: [1.0, 2.0, 3.0]
	vectorBuilder.Append(true)
	valueBuilder.AppendValues([]float32{1.0, 2.0, 3.0}, nil)

	// Second vector: [4.0, 5.0, 6.0]
	vectorBuilder.Append(true)
	valueBuilder.AppendValues([]float32{4.0, 5.0, 6.0}, nil)

	nameBuilder.AppendValues([]string{"Alice", "Bob"}, nil)
	ageBuilder.AppendValues([]int32{30, 40}, nil)

	// Create arrays
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	vectorArray := vectorBuilder.NewArray()
	defer vectorArray.Release()

	nameArray := nameBuilder.NewArray()
	defer nameArray.Release()

	ageArray := ageBuilder.NewArray()
	defer ageArray.Release()

	// Create record batch
	record := array.NewRecord(schema, []arrow.Array{idArray, vectorArray, nameArray, ageArray}, 2)
	return record
}

func TestExtractVectors(t *testing.T) {
	allocator := memory.NewGoAllocator()
	record := createTestRecord(t, allocator)
	defer record.Release()

	vectors, err := ExtractVectors(record)
	require.NoError(t, err)
	require.Len(t, vectors, 2)

	// Verify first vector
	assert.Equal(t, []float32{1.0, 2.0, 3.0}, vectors[0])

	// Verify second vector
	assert.Equal(t, []float32{4.0, 5.0, 6.0}, vectors[1])
}

func TestExtractIDs(t *testing.T) {
	allocator := memory.NewGoAllocator()
	record := createTestRecord(t, allocator)
	defer record.Release()

	ids, err := ExtractIDs(record)
	require.NoError(t, err)
	require.Len(t, ids, 2)

	// Verify IDs
	assert.Equal(t, "id1", ids[0])
	assert.Equal(t, "id2", ids[1])
}

func TestExtractPayloads(t *testing.T) {
	allocator := memory.NewGoAllocator()
	record := createTestRecord(t, allocator)
	defer record.Release()

	payloads, err := ExtractPayloads(record)
	require.NoError(t, err)
	require.Len(t, payloads, 2)

	// Verify first payload
	assert.Equal(t, "Alice", payloads[0]["name"])
	assert.Equal(t, int32(30), payloads[0]["age"])

	// Verify second payload
	assert.Equal(t, "Bob", payloads[1]["name"])
	assert.Equal(t, int32(40), payloads[1]["age"])
}
