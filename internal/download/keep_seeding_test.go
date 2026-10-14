package download

import (
	"testing"

	"github.com/elsbrock/plundrio/internal/config"
)

// completeTransferForCleanup drives a transfer with a Put.io source file
// through the coordinator so the registered cleanup hooks run.
func completeTransferForCleanup(t *testing.T, keepSeeding bool, deletedFiles chan int64) *Manager {
	t.Helper()
	m := New(&config.Config{
		TargetDir:   t.TempDir(),
		FolderID:    testFolderID,
		WorkerCount: 1,
		KeepSeeding: keepSeeding,
	}, &fakeClient{deletedFiles: deletedFiles})
	m.coordinator.InitiateTransfer(101, "completed", 501, 0)
	if err := m.coordinator.StartDownload(101); err != nil {
		t.Fatal(err)
	}

	m.cleanupTransfer(101)

	ctx, ok := m.coordinator.GetTransferContext(101)
	if !ok || ctx.GetState() != TransferLifecycleProcessed {
		t.Fatalf("transfer was not marked processed: context=%v exists=%v", ctx, ok)
	}
	return m
}

func TestCleanupDeletesSourceFileByDefault(t *testing.T) {
	deletedFiles := make(chan int64, 1)
	completeTransferForCleanup(t, false, deletedFiles)

	select {
	case fileID := <-deletedFiles:
		if fileID != 501 {
			t.Fatalf("DeleteFile called with %d, want 501", fileID)
		}
	default:
		t.Fatal("DeleteFile was not called; the default must delete the source after the local copy")
	}
}

func TestCleanupKeepsSourceFileWhenKeepSeeding(t *testing.T) {
	deletedFiles := make(chan int64, 1)
	completeTransferForCleanup(t, true, deletedFiles)

	select {
	case fileID := <-deletedFiles:
		t.Fatalf("DeleteFile called with %d, want the source kept on Put.io for seeding", fileID)
	default:
	}
}
