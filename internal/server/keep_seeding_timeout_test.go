package server

import (
	"testing"
	"time"

	"github.com/elsbrock/go-putio"
	"github.com/elsbrock/plundrio/internal/config"
	"github.com/elsbrock/plundrio/internal/download"
)

// timeoutTestNow is the fixed time of the clock in the keep-seeding-timeout tests.
var timeoutTestNow = time.Date(2026, time.October, 17, 20, 0, 0, 0, time.UTC)

// serverWithProcessedTransfer builds a server with one locally processed
// transfer and a fixed clock.
func serverWithProcessedTransfer(t *testing.T, cfg *config.Config, transfer *putio.Transfer) *Server {
	t.Helper()
	ctx := download.NewTransferContext(transfer.ID, 1, download.TransferLifecycleProcessed)
	ctx.SetTotalSize(1000)
	ctx.AddDownloadedBytes(1000)
	cfg.TargetDir = t.TempDir()
	return &Server{
		cfg: cfg,
		dlService: &torrentAddDownloadService{
			transfers: []*putio.Transfer{transfer},
			contexts:  map[int64]*download.TransferContext{transfer.ID: ctx},
		},
		now: func() time.Time { return timeoutTestNow },
	}
}

// finishedAgo returns the finish time of a transfer that ended age before the
// fixed clock.
func finishedAgo(age time.Duration) *putio.Time {
	return &putio.Time{Time: timeoutTestNow.Add(-age)}
}

func TestTorrentGetKeepSeedingTimeout(t *testing.T) {
	tests := []struct {
		name         string
		keepSeeding  bool
		timeout      time.Duration
		putioStatus  string
		finishedAt   *putio.Time
		wantIdleMode int
	}{
		{name: "older than the limit", keepSeeding: true, timeout: 72 * time.Hour, putioStatus: "SEEDING", finishedAt: finishedAgo(73 * time.Hour), wantIdleMode: transmissionLimitModeSingle},
		{name: "exactly at the limit", keepSeeding: true, timeout: 72 * time.Hour, putioStatus: "SEEDING", finishedAt: finishedAgo(72 * time.Hour), wantIdleMode: transmissionLimitModeSingle},
		{name: "one minute before the limit", keepSeeding: true, timeout: 72 * time.Hour, putioStatus: "SEEDING", finishedAt: finishedAgo(72*time.Hour - time.Minute), wantIdleMode: transmissionLimitModeUnlimited},
		{name: "limit disabled", keepSeeding: true, timeout: 0, putioStatus: "SEEDING", finishedAt: finishedAgo(1000 * time.Hour), wantIdleMode: transmissionLimitModeUnlimited},
		{name: "no finish time", keepSeeding: true, timeout: 72 * time.Hour, putioStatus: "SEEDING", finishedAt: nil, wantIdleMode: transmissionLimitModeUnlimited},
		{name: "zero finish time", keepSeeding: true, timeout: 72 * time.Hour, putioStatus: "SEEDING", finishedAt: &putio.Time{}, wantIdleMode: transmissionLimitModeUnlimited},
		// A COMPLETED transfer is removable without the limit.
		{name: "completed before the limit", keepSeeding: true, timeout: 72 * time.Hour, putioStatus: "COMPLETED", finishedAt: finishedAgo(time.Hour), wantIdleMode: transmissionLimitModeSingle},
		// Without keep-seeding the option has no effect and the default holds:
		// a processed transfer is removable.
		{name: "keep-seeding off", keepSeeding: false, timeout: 72 * time.Hour, putioStatus: "SEEDING", finishedAt: finishedAgo(time.Hour), wantIdleMode: transmissionLimitModeSingle},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := serverWithProcessedTransfer(t,
				&config.Config{KeepSeeding: tt.keepSeeding, KeepSeedingTimeout: tt.timeout},
				&putio.Transfer{ID: 1, Hash: "processed", Status: tt.putioStatus, PercentDone: 100, Size: 1000, FinishedAt: tt.finishedAt})

			got := singleTorrentGet(t, server)

			if got.Status != trStatusSeed {
				t.Errorf("status = %d, want %d (seed)", got.Status, trStatusSeed)
			}
			if got.SeedIdleMode != tt.wantIdleMode {
				t.Errorf("seedIdleMode = %d, want %d", got.SeedIdleMode, tt.wantIdleMode)
			}
		})
	}
}

// torrent-get runs often, so the time limit must log once per transfer.
func TestSeedingTimedOutLogsOncePerTransfer(t *testing.T) {
	server := serverWithProcessedTransfer(t,
		&config.Config{KeepSeeding: true, KeepSeedingTimeout: time.Hour},
		&putio.Transfer{ID: 7, Status: "SEEDING"})
	young := &putio.Transfer{ID: 7, Status: "SEEDING", FinishedAt: finishedAgo(time.Minute)}
	old := &putio.Transfer{ID: 7, Status: "SEEDING", FinishedAt: finishedAgo(2 * time.Hour)}

	if server.seedingTimedOut(young) {
		t.Fatal("a young transfer must not time out")
	}
	if _, logged := server.seedingTimeoutLogged.Load(int64(7)); logged {
		t.Fatal("a young transfer must not be marked as logged")
	}
	for i := 0; i < 3; i++ {
		if !server.seedingTimedOut(old) {
			t.Fatalf("call %d: an old transfer must time out", i)
		}
	}
	if _, logged := server.seedingTimeoutLogged.Load(int64(7)); !logged {
		t.Fatal("an old transfer must be marked as logged")
	}
}
