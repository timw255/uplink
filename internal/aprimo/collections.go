package aprimo

import (
	"context"
	"net/url"
	"slices"
)

// Collections is the collections resource.
type Collections struct {
	r *requester
}

// CollectionBatchSize bounds how many record ids go into one UpdateRecords
// call when filing records into a collection. Exported so a batching caller
// (the importer) can align its own chunking — and its ledger markers — with
// the per-call batch.
const CollectionBatchSize = 1000

// AddRecords files records into a collection, chunking large sets into
// UpdateRecords calls of CollectionBatchSize. AddOrUpdate is idempotent
// (a record already in the collection is a no-op), so retries and resumes
// are safe to re-file.
func (cs *Collections) AddRecords(ctx context.Context, collectionID string, recordIDs []string) error {
	for chunk := range slices.Chunk(recordIDs, CollectionBatchSize) {
		refs := make([]IDRef, len(chunk))
		for i, id := range chunk {
			refs[i] = IDRef{ID: id}
		}
		req := UpdateCollectionRequest{Records: &CollectionRecordActions{AddOrUpdate: refs}}
		if err := cs.UpdateRecords(ctx, collectionID, req); err != nil {
			return err
		}
	}
	return nil
}

// CollectionRecordActions is the add/remove action set applied to a
// collection's record list. Shape mirrors Files and Classifications.
type CollectionRecordActions struct {
	AddOrUpdate []IDRef `json:"addOrUpdate,omitempty"`
	Remove      []IDRef `json:"remove,omitempty"`
}

// UpdateCollectionRequest is the body for Collections.UpdateRecords.
type UpdateCollectionRequest struct {
	Records *CollectionRecordActions `json:"records,omitempty"`
}

// UpdateRecords adds or removes records from a collection. The endpoint
// is PUT /api/core/collection/{id} with a partial-update body, matching
// the Records.Update convention.
//
// To file a single new record into a collection:
//
//	err := client.Collections.UpdateRecords(ctx, collectionID,
//	    aprimo.UpdateCollectionRequest{
//	        Records: &aprimo.CollectionRecordActions{
//	            AddOrUpdate: []aprimo.IDRef{{ID: recordID}},
//	        },
//	    })
func (cs *Collections) UpdateRecords(ctx context.Context, collectionID string, req UpdateCollectionRequest) error {
	return cs.r.putJSON(ctx, "/api/core/collection/"+url.PathEscape(collectionID), req, nil, nil)
}
