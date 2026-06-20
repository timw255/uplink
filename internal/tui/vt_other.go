//go:build !windows

package tui

import "os"

// enableVirtualTerminal is a no-op off Windows, where terminals process ANSI
// escapes natively.
func enableVirtualTerminal(*os.File) {}
