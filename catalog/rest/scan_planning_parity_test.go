// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package rest_test

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/catalog/rest/internal/planfake"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalAndRemoteScanPlanningParity(t *testing.T) {
	ctx := t.Context()
	local, fsF, historicalSnapshotID, firstDataPath := newScanPlanningParityTable(t, ctx)

	srv := planfake.New(t, planfake.Scenario{
		ConfigResponse: planfake.ConfigResponse(planfake.PlanTableScanEndpoint),
		ExpectedTarget: &planfake.ExpectedTarget{Namespace: "db", Table: "parity"},
		PlanResponder:  localPlanningResponder(local),
	})
	planner, err := rest.NewCatalog(ctx, "rest", srv.URL())
	require.NoError(t, err)

	remote := table.New(local.Identifier(), local.Metadata(), local.MetadataLocation(), fsF, planner)

	tests := []struct {
		name               string
		field              string
		snapshotID         *int64
		wantTasks          int
		wantSnapshotSchema bool
		assertTasks        func(*testing.T, []table.FileScanTask)
	}{
		{
			name:      "current schema with deletes",
			field:     "new_name",
			wantTasks: 2,
			assertTasks: func(t *testing.T, tasks []table.FileScanTask) {
				t.Helper()

				var positionDeleteTasks int
				for _, task := range tasks {
					require.Len(t, task.EqualityDeleteFiles, 1)
					if task.File.FilePath() == firstDataPath {
						require.Len(t, task.DeleteFiles, 1)
						positionDeleteTasks++
					} else {
						assert.Empty(t, task.DeleteFiles)
					}
				}
				assert.Equal(t, 1, positionDeleteTasks)
			},
		},
		{
			name:               "historical snapshot schema",
			field:              "old_name",
			snapshotID:         &historicalSnapshotID,
			wantTasks:          1,
			wantSnapshotSchema: true,
			assertTasks: func(t *testing.T, tasks []table.FileScanTask) {
				t.Helper()

				assert.Empty(t, tasks[0].DeleteFiles)
				assert.Empty(t, tasks[0].EqualityDeleteFiles)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestCount := len(scanPlanningParityRequests(t, srv.Requests()))
			filter := iceberg.StartsWith(iceberg.Reference(tt.field), "a")
			opts := []table.ScanOption{table.WithRowFilter(filter)}
			if tt.snapshotID != nil {
				opts = append(opts, table.WithSnapshotID(*tt.snapshotID))
			}

			localTasks, err := local.Scan(append(slices.Clone(opts),
				table.WithScanPlanningMode(table.ScanPlanningLocal))...).PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, localTasks, tt.wantTasks)
			tt.assertTasks(t, localTasks)

			remoteTasks, err := remote.Scan(append(slices.Clone(opts),
				table.WithScanPlanningMode(table.ScanPlanningRemote))...).PlanFiles(ctx)
			require.NoError(t, err)
			require.Equal(t,
				parityTaskSet(t, localTasks, filter),
				parityTaskSet(t, remoteTasks, filter),
			)

			requests := scanPlanningParityRequests(t, srv.Requests())
			require.Len(t, requests, requestCount+1)
			request := requests[len(requests)-1]
			require.NotNil(t, request.UseSnapshotSchema)
			assert.Equal(t, tt.wantSnapshotSchema, *request.UseSnapshotSchema)
			assert.Equal(t, tt.snapshotID, request.SnapshotID)
		})
	}
}

type scanPlanningParityCatalog struct {
	metadata table.Metadata
	location string
	fsF      table.FSysF
}

func (c *scanPlanningParityCatalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	return table.New(ident, c.metadata, c.location, c.fsF, c), nil
}

func (c *scanPlanningParityCatalog) CommitTable(
	_ context.Context,
	_ table.Identifier,
	requirements []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	for _, requirement := range requirements {
		if err := requirement.Validate(c.metadata); err != nil {
			return nil, "", fmt.Errorf("%w: %w", table.ErrCommitFailed, err)
		}
	}

	metadata, err := table.UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = metadata

	return metadata, c.location, nil
}

func newScanPlanningParityTable(
	t *testing.T,
	ctx context.Context,
) (*table.Table, table.FSysF, int64, string) {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "old_name", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	metadata, err := table.NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder,
		location,
		iceberg.Properties{table.PropertyFormatVersion: "3"},
	)
	require.NoError(t, err)

	metadataLocation := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	catalog := &scanPlanningParityCatalog{
		metadata: metadata,
		location: metadataLocation,
		fsF:      fsF,
	}
	tbl := table.New(table.Identifier{"db", "parity"}, metadata, metadataLocation, fsF, catalog)

	tbl = appendScanPlanningParityRow(t, ctx, tbl, 1, "old_name", "alpha")
	historicalSnapshotID := tbl.CurrentSnapshot().SnapshotID
	historicalTasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, historicalTasks, 1)
	firstDataPath := historicalTasks[0].File.FilePath()

	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).
		RenameColumn([]string{"old_name"}, "new_name").
		Commit())
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	tbl = appendScanPlanningParityRow(t, ctx, tbl, 2, "new_name", "alpha")
	tbl = appendScanPlanningParityRow(t, ctx, tbl, 3, "new_name", "omega")

	positionDeleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes,
		location+"/data/position-delete.parquet",
		iceberg.ParquetFile,
		nil, nil, nil,
		1, 128,
	)
	require.NoError(t, err)
	positionDelete := positionDeleteBuilder.ReferencedDataFile(firstDataPath).Build()

	equalityDeleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentEqDeletes,
		location+"/data/equality-delete.parquet",
		iceberg.ParquetFile,
		nil, nil, nil,
		2, 256,
	)
	require.NoError(t, err)
	equalityDelete := equalityDeleteBuilder.EqualityFieldIDs([]int{1}).Build()

	txn = tbl.NewTransaction()
	require.NoError(t, txn.NewRowDelta(nil).
		AddDeletes(positionDelete, equalityDelete).
		Commit(ctx))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	unfiltered, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, unfiltered, 3, "the filtered parity plan must be able to prune a real file")

	return tbl, fsF, historicalSnapshotID, firstDataPath
}

func appendScanPlanningParityRow(
	t *testing.T,
	ctx context.Context,
	tbl *table.Table,
	id int64,
	field, value string,
) *table.Table {
	t.Helper()

	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		fmt.Sprintf(`[{"id":%d,%q:%q}]`, id, field, value),
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	reader := array.NewTableReader(data, -1)
	defer reader.Release()
	committed, err := tbl.Append(ctx, reader, nil)
	require.NoError(t, err)

	return committed
}

type scanPlanningParityRequest struct {
	SnapshotID        *int64          `json:"snapshot-id"`
	Select            []string        `json:"select"`
	Filter            json.RawMessage `json:"filter"`
	CaseSensitive     *bool           `json:"case-sensitive"`
	UseSnapshotSchema *bool           `json:"use-snapshot-schema"`
}

func localPlanningResponder(tbl *table.Table) planfake.PlanResponder {
	return func(ctx context.Context, request planfake.Request) (planfake.Response, error) {
		var wireRequest scanPlanningParityRequest
		if err := json.Unmarshal(request.Body, &wireRequest); err != nil {
			return planfake.Response{}, fmt.Errorf("decode plan request: %w", err)
		}

		schema, err := scanPlanningParitySchema(tbl.Metadata(), wireRequest)
		if err != nil {
			return planfake.Response{}, err
		}
		caseSensitive := true
		if wireRequest.CaseSensitive != nil {
			caseSensitive = *wireRequest.CaseSensitive
		}
		filter, err := decodeScanPlanningParityFilter(wireRequest.Filter, schema, caseSensitive)
		if err != nil {
			return planfake.Response{}, err
		}

		opts := []table.ScanOption{
			table.WithScanPlanningMode(table.ScanPlanningLocal),
			table.WithRowFilter(filter),
		}
		if wireRequest.SnapshotID != nil {
			opts = append(opts, table.WithSnapshotID(*wireRequest.SnapshotID))
		}
		if wireRequest.CaseSensitive != nil {
			opts = append(opts, table.WithCaseSensitive(*wireRequest.CaseSensitive))
		}
		if len(wireRequest.Select) != 0 {
			opts = append(opts, table.WithSelectedFields(wireRequest.Select...))
		}

		tasks, err := tbl.Scan(opts...).PlanFiles(ctx)
		if err != nil {
			return planfake.Response{}, fmt.Errorf("plan locally: %w", err)
		}
		body, err := encodeScanPlanningParityResponse(tasks, wireRequest.Filter)
		if err != nil {
			return planfake.Response{}, err
		}

		return planfake.Response{Status: http.StatusOK, Body: body}, nil
	}
}

// decodeScanPlanningParityFilter is deliberately independent from
// iceberg.ParseExpr. The parity test is intended to detect a request codec that
// assigns the wrong wire operator, term, or literal while still round-tripping
// through its own decoder, so this fake server recognizes only the one Java
// ExpressionParser shape that the test sends.
func decodeScanPlanningParityFilter(
	raw json.RawMessage,
	schema *iceberg.Schema,
	caseSensitive bool,
) (iceberg.BooleanExpression, error) {
	if len(raw) == 0 {
		return iceberg.AlwaysTrue{}, nil
	}

	var predicate struct {
		Type  string `json:"type"`
		Term  string `json:"term"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(raw, &predicate); err != nil {
		return nil, fmt.Errorf("decode plan filter: %w", err)
	}
	if predicate.Type != "starts-with" || predicate.Term == "" {
		return nil, fmt.Errorf("unexpected plan filter %s", raw)
	}

	filter := iceberg.StartsWith(iceberg.Reference(predicate.Term), predicate.Value)
	if _, err := iceberg.BindExpr(schema, filter, caseSensitive); err != nil {
		return nil, fmt.Errorf("bind plan filter to selected schema: %w", err)
	}

	return filter, nil
}

func scanPlanningParitySchema(
	metadata table.Metadata,
	request scanPlanningParityRequest,
) (*iceberg.Schema, error) {
	useSnapshotSchema := request.UseSnapshotSchema != nil && *request.UseSnapshotSchema
	if !useSnapshotSchema {
		return metadata.CurrentSchema(), nil
	}

	var snapshot *table.Snapshot
	if request.SnapshotID != nil {
		snapshot = metadata.SnapshotByID(*request.SnapshotID)
	} else {
		snapshot = metadata.CurrentSnapshot()
	}
	if snapshot == nil {
		return nil, fmt.Errorf("snapshot schema requested without a snapshot")
	}
	if snapshot.SchemaID == nil {
		return metadata.CurrentSchema(), nil
	}
	for _, schema := range metadata.Schemas() {
		if schema.ID == *snapshot.SchemaID {
			return schema, nil
		}
	}

	return nil, fmt.Errorf("snapshot %d references unknown schema %d", snapshot.SnapshotID, *snapshot.SchemaID)
}

type scanPlanningParityContentFile struct {
	SpecID          int    `json:"spec-id"`
	Partition       []any  `json:"partition"`
	Content         string `json:"content"`
	FilePath        string `json:"file-path"`
	FileFormat      string `json:"file-format"`
	FileSizeInBytes int64  `json:"file-size-in-bytes"`
	RecordCount     int64  `json:"record-count"`
}

type scanPlanningParityDataFile struct {
	scanPlanningParityContentFile
}

type scanPlanningParityDeleteFile struct {
	scanPlanningParityContentFile
	EqualityIDs        []int   `json:"equality-ids,omitempty"`
	ReferencedDataFile *string `json:"referenced-data-file,omitempty"`
}

type scanPlanningParityTask struct {
	DataFile             scanPlanningParityDataFile `json:"data-file"`
	DeleteFileReferences []int                      `json:"delete-file-references,omitempty"`
	ResidualFilter       json.RawMessage            `json:"residual-filter,omitempty"`
}

func encodeScanPlanningParityResponse(
	tasks []table.FileScanTask,
	residual json.RawMessage,
) (json.RawMessage, error) {
	wireTasks := make([]scanPlanningParityTask, 0, len(tasks))
	wireDeletes := make([]scanPlanningParityDeleteFile, 0)
	deleteIndexes := make(map[string]int)

	for _, task := range tasks {
		dataFile, err := scanPlanningParityDataFileFrom(task.File)
		if err != nil {
			return nil, err
		}
		wireTask := scanPlanningParityTask{
			DataFile:       dataFile,
			ResidualFilter: slices.Clone(residual),
		}

		deletes := make([]iceberg.DataFile, 0,
			len(task.DeleteFiles)+len(task.EqualityDeleteFiles)+len(task.DeletionVectorFiles))
		deletes = append(deletes, task.DeleteFiles...)
		deletes = append(deletes, task.EqualityDeleteFiles...)
		deletes = append(deletes, task.DeletionVectorFiles...)
		for _, deleteFile := range deletes {
			key := fmt.Sprintf("%d\x00%s", deleteFile.ContentType(), deleteFile.FilePath())
			index, ok := deleteIndexes[key]
			if !ok {
				wireDelete, err := scanPlanningParityDeleteFileFrom(deleteFile)
				if err != nil {
					return nil, err
				}
				index = len(wireDeletes)
				deleteIndexes[key] = index
				wireDeletes = append(wireDeletes, wireDelete)
			}
			wireTask.DeleteFileReferences = append(wireTask.DeleteFileReferences, index)
		}
		wireTasks = append(wireTasks, wireTask)
	}

	return json.Marshal(struct {
		Status        string                         `json:"status"`
		PlanID        string                         `json:"plan-id"`
		FileScanTasks []scanPlanningParityTask       `json:"file-scan-tasks"`
		DeleteFiles   []scanPlanningParityDeleteFile `json:"delete-files,omitempty"`
	}{
		Status:        "completed",
		PlanID:        "local-parity-plan",
		FileScanTasks: wireTasks,
		DeleteFiles:   wireDeletes,
	})
}

func scanPlanningParityDataFileFrom(file iceberg.DataFile) (scanPlanningParityDataFile, error) {
	content, err := scanPlanningParityContentFileFrom(file)
	if err != nil {
		return scanPlanningParityDataFile{}, err
	}
	if file.ContentType() != iceberg.EntryContentData {
		return scanPlanningParityDataFile{}, fmt.Errorf("data task contains non-data file %q", file.FilePath())
	}

	return scanPlanningParityDataFile{scanPlanningParityContentFile: content}, nil
}

func scanPlanningParityDeleteFileFrom(file iceberg.DataFile) (scanPlanningParityDeleteFile, error) {
	content, err := scanPlanningParityContentFileFrom(file)
	if err != nil {
		return scanPlanningParityDeleteFile{}, err
	}

	return scanPlanningParityDeleteFile{
		scanPlanningParityContentFile: content,
		EqualityIDs:                   slices.Clone(file.EqualityFieldIDs()),
		ReferencedDataFile:            cloneStringPointer(file.ReferencedDataFile()),
	}, nil
}

func scanPlanningParityContentFileFrom(file iceberg.DataFile) (scanPlanningParityContentFile, error) {
	if int(file.SpecID()) != iceberg.UnpartitionedSpec.ID() {
		return scanPlanningParityContentFile{}, fmt.Errorf(
			"parity fixture only supports the unpartitioned spec, got spec ID %d", file.SpecID())
	}

	var content string
	switch file.ContentType() {
	case iceberg.EntryContentData:
		content = "data"
	case iceberg.EntryContentPosDeletes:
		content = "position-deletes"
	case iceberg.EntryContentEqDeletes:
		content = "equality-deletes"
	default:
		return scanPlanningParityContentFile{}, fmt.Errorf(
			"unsupported content type %d for %q", file.ContentType(), file.FilePath())
	}

	return scanPlanningParityContentFile{
		SpecID:          int(file.SpecID()),
		Partition:       []any{},
		Content:         content,
		FilePath:        file.FilePath(),
		FileFormat:      strings.ToLower(string(file.FileFormat())),
		FileSizeInBytes: file.FileSizeBytes(),
		RecordCount:     file.Count(),
	}, nil
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value

	return &cloned
}

type scanPlanningParityTaskFingerprint struct {
	Path            string
	Start           int64
	Length          int64
	PositionDeletes []scanPlanningParityDeleteFingerprint
	EqualityDeletes []scanPlanningParityDeleteFingerprint
	DeletionVectors []scanPlanningParityDeleteFingerprint
	Residual        string
}

type scanPlanningParityDeleteFingerprint struct {
	Path               string
	Format             iceberg.FileFormat
	ReferencedDataFile string
	EqualityIDs        []int
}

func parityTaskSet(
	t *testing.T,
	tasks []table.FileScanTask,
	fallbackResidual iceberg.BooleanExpression,
) []scanPlanningParityTaskFingerprint {
	t.Helper()

	result := make([]scanPlanningParityTaskFingerprint, 0, len(tasks))
	for _, task := range tasks {
		residual := task.Residual
		if residual == nil {
			residual = fallbackResidual
		}
		require.True(t, residual.Equals(fallbackResidual),
			"task %q changed the parity fixture's unsimplified residual", task.File.FilePath())
		residualJSON, err := json.Marshal(residual)
		require.NoError(t, err)

		result = append(result, scanPlanningParityTaskFingerprint{
			Path:            task.File.FilePath(),
			Start:           task.Start,
			Length:          task.Length,
			PositionDeletes: parityDeleteSet(task.DeleteFiles),
			EqualityDeletes: parityDeleteSet(task.EqualityDeleteFiles),
			DeletionVectors: parityDeleteSet(task.DeletionVectorFiles),
			Residual:        string(residualJSON),
		})
	}
	slices.SortFunc(result, func(left, right scanPlanningParityTaskFingerprint) int {
		if pathCmp := cmp.Compare(left.Path, right.Path); pathCmp != 0 {
			return pathCmp
		}
		if startCmp := cmp.Compare(left.Start, right.Start); startCmp != 0 {
			return startCmp
		}

		return cmp.Compare(left.Length, right.Length)
	})

	return result
}

func parityDeleteSet(files []iceberg.DataFile) []scanPlanningParityDeleteFingerprint {
	result := make([]scanPlanningParityDeleteFingerprint, 0, len(files))
	for _, file := range files {
		referencedDataFile := ""
		if reference := file.ReferencedDataFile(); reference != nil {
			referencedDataFile = *reference
		}
		result = append(result, scanPlanningParityDeleteFingerprint{
			Path:               file.FilePath(),
			Format:             file.FileFormat(),
			ReferencedDataFile: referencedDataFile,
			EqualityIDs:        slices.Clone(file.EqualityFieldIDs()),
		})
	}
	slices.SortFunc(result, func(left, right scanPlanningParityDeleteFingerprint) int {
		return cmp.Compare(left.Path, right.Path)
	})

	return result
}

func scanPlanningParityRequests(t *testing.T, requests []planfake.Request) []scanPlanningParityRequest {
	t.Helper()

	result := make([]scanPlanningParityRequest, 0)
	for _, request := range requests {
		if request.Method != http.MethodPost || !strings.HasSuffix(request.Path, "/plan") {
			continue
		}
		var decoded scanPlanningParityRequest
		require.NoError(t, json.Unmarshal(request.Body, &decoded))
		result = append(result, decoded)
	}

	return result
}
