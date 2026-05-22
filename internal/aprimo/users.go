package aprimo

import (
	"context"
	"fmt"
	"strconv"
)

// Users is the users resource. Uplink prefetches the user catalog so
// companion scripts can reference users by email (preferred) or login
// `name` rather than the tenant-specific GUID.
type Users struct {
	r *requester
}

// User is the slice Uplink uses. Aprimo's full User object also carries
// audit fields, language preferences, storage quotas, etc.; we don't
// need any of that for name→id resolution.
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type userPage struct {
	Items []User `json:"items"`
	Links struct {
		Next *struct {
			Href string `json:"href"`
		} `json:"next"`
	} `json:"_links"`
}

const maxUsers = 100_000

// List walks every page of /api/core/users and returns the full set.
func (u *Users) List(ctx context.Context) ([]User, error) {
	headers := map[string]string{"pageSize": strconv.Itoa(200)}

	path := "/api/core/users"
	var out []User
	for {
		var page userPage
		if err := u.r.getJSON(ctx, path, headers, &page); err != nil {
			return nil, fmt.Errorf("aprimo: list users: %w", err)
		}
		out = append(out, page.Items...)
		if len(out) > maxUsers {
			return nil, fmt.Errorf("aprimo: list users: exceeded cap of %d entries", maxUsers)
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
