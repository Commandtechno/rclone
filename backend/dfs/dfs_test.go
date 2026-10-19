// Test dfs filesystem interface
package dfs

import (
	"fmt"
	"testing"

	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against a fake Discord and
// the postgres in DFS_TEST_DATABASE_URL
func TestIntegration(t *testing.T) {
	dbURL := testDatabaseURL(t)
	d := newFakeDiscord(t)
	fstests.Run(t, &fstests.Opt{
		RemoteName: fmt.Sprintf(":dfs,discord_token=%q,channel_id=1,database_url=%q,api_url=%q:",
			fakeToken, dbURL, d.apiURL()),
		NilObject: (*Object)(nil),
	})
}
