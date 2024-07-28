// Test Cloudinary filesystem interface

package cloudinary_test

import (
	"testing"

	"github.com/rclone/rclone/backend/cloudinary"
	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName:      "TestCloudinary:",
		NilObject:       (*cloudinary.Object)(nil),
		SkipInvalidUTF8: true,
	})
}
