package aprimo

import (
	"context"
	"fmt"
	"strconv"
)

// UserGroups is the user-groups resource. Uplink prefetches the
// catalog so companion scripts can reference groups by display name.
type UserGroups struct {
	r *requester
}

// UserGroup is the slice Uplink uses.
type UserGroup struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type userGroupPage struct {
	Items []UserGroup `json:"items"`
	Links struct {
		Next *struct {
			Href string `json:"href"`
		} `json:"next"`
	} `json:"_links"`
}

const maxUserGroups = 10_000

// List walks every page of /api/core/usergroups.
func (g *UserGroups) List(ctx context.Context) ([]UserGroup, error) {
	headers := map[string]string{"pageSize": strconv.Itoa(listPageSize)}

	path := "/api/core/usergroups"
	var out []UserGroup
	for {
		var page userGroupPage
		if err := g.r.getJSON(ctx, path, headers, &page); err != nil {
			return nil, fmt.Errorf("aprimo: list user groups: %w", err)
		}
		out = append(out, page.Items...)
		if len(out) > maxUserGroups {
			return nil, fmt.Errorf("aprimo: list user groups: exceeded cap of %d entries", maxUserGroups)
		}
		if page.Links.Next == nil {
			break
		}
		nextPath, err := relativizePath(page.Links.Next.Href)
		if err != nil {
			return nil, fmt.Errorf("aprimo: parse next link %q: %w", page.Links.Next.Href, err)
		}
		path = nextPath
	}
	return out, nil
}
