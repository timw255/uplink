package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

func runInspect(args []string, out io.Writer) error {
	if len(args) < 1 || isFlag(args[0]) {
		return errors.New(`usage: uplink inspect {sync|state|upload} [flags]`)
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "sync":
		return runInspectSync(rest, out)
	case "state":
		return runInspectState(rest, out)
	case "upload":
		return runInspectUpload(rest, out)
	default:
		return fmt.Errorf("unknown inspect subcommand %q", sub)
	}
}

func runInspectSync(args []string, out io.Writer) error {
	fset := flag.NewFlagSet("inspect sync", flag.ExitOnError)
	dataDir := fset.String("data-dir", "./data", "path to uplink data directory")
	channel := fset.String("channel", "", "channel name")
	path := fset.String("path", "", "source path")
	if err := fset.Parse(args); err != nil {
		return err
	}
	if *channel == "" || *path == "" {
		return errors.New("--channel and --path are required")
	}

	st, err := openStoreForCLI(*dataDir)
	if err != nil {
		return err
	}
	defer st.Close()

	entry, err := st.LookupLatest(context.Background(), *channel, *path)
	if err != nil {
		return err
	}
	if entry == nil {
		fmt.Fprintln(out, "(no sync_log entry)")
		return nil
	}
	return prettyJSON(out, map[string]any{
		"id":               entry.ID,
		"ts":               entry.TS,
		"channel":          entry.ChannelName,
		"source_connector": entry.SourceConnector,
		"source_path":      entry.SourcePath,
		"source_version":   entry.SourceVersion,
		"dest_id":          entry.DestID,
		"kind":             string(entry.Kind),
	})
}

func runInspectState(args []string, out io.Writer) error {
	fset := flag.NewFlagSet("inspect state", flag.ExitOnError)
	dataDir := fset.String("data-dir", "./data", "path to uplink data directory")
	name := fset.String("connector", "", "connector instance name")
	if err := fset.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--connector is required")
	}
	path := filepath.Join(*dataDir, "state", *name+".json")
	return dumpJSONFile(path, out)
}

func runInspectUpload(args []string, out io.Writer) error {
	fset := flag.NewFlagSet("inspect upload", flag.ExitOnError)
	dataDir := fset.String("data-dir", "./data", "path to uplink data directory")
	jobID := fset.String("job", "", "job id (ULID)")
	if err := fset.Parse(args); err != nil {
		return err
	}
	if *jobID == "" {
		return errors.New("--job is required")
	}
	path := filepath.Join(*dataDir, "uploads", *jobID+".session.json")
	return dumpJSONFile(path, out)
}

// prettyJSON writes v as indented JSON to out.
func prettyJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// dumpJSONFile reads a JSON file and writes it back out indented for
// human reading. If the file is missing, returns a friendly error.
func dumpJSONFile(path string, out io.Writer) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("file not found: %s", path)
	}
	if err != nil {
		return err
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		// Not JSON — just print as-is.
		_, _ = out.Write(data)
		fmt.Fprintln(out)
		return nil
	}
	return prettyJSON(out, v)
}
