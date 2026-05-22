package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	storepkg "github.com/timw255/uplink/internal/store"
)

func runRetry(args []string, out, errOut io.Writer) error {
	fset := flag.NewFlagSet("retry", flag.ExitOnError)
	dataDir := fset.String("data-dir", "./data", "path to uplink data directory")
	id := fset.String("id", "", "specific job id to retry")
	channel := fset.String("channel", "", "retry every failed job for this channel")
	all := fset.Bool("all", false, "retry every failed job")
	if err := fset.Parse(args); err != nil {
		return err
	}

	if (*id == "" && *channel == "" && !*all) ||
		(*id != "" && *channel != "") ||
		(*id != "" && *all) ||
		(*channel != "" && *all) {
		return errors.New("specify exactly one of --id, --channel, --all")
	}

	ctx := context.Background()
	s, err := storepkg.Open(ctx, *dataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	failed, err := s.ListJobs(ctx, storepkg.StatusFailed)
	if err != nil {
		return err
	}
	if len(failed) == 0 && *id == "" {
		fmt.Fprintln(out, "no failed jobs")
		return nil
	}

	moved := 0
	for _, j := range failed {
		if *id != "" && j.ID != *id {
			continue
		}
		if *channel != "" && j.ChannelName != *channel {
			continue
		}
		if _, err := s.DB().ExecContext(ctx, `
            UPDATE jobs
               SET status = 'pending', attempts = 0, last_error = NULL,
                   next_run_at = ?
             WHERE id = ?`,
			time.Now().UTC().Format(time.RFC3339Nano), j.ID,
		); err != nil {
			fmt.Fprintf(errOut, "warn: retry %s: %v\n", j.ID, err)
			continue
		}
		moved++
		if *id != "" {
			break
		}
	}

	if *id != "" && moved == 0 {
		return fmt.Errorf("no failed job with id %q", *id)
	}
	fmt.Fprintf(out, "retried %d job(s)\n", moved)
	return nil
}
