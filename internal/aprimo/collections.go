package aprimo

import (
	"context"
	"net/url"
)

// Collections is the collections resource.
type Collections struct {
	r *requester
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
