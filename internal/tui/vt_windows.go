//go:build windows

package tui

import (
	"os"

	"golang.org/x/sys/windows"
)

// enableVirtualTerminal turns on ANSI escape processing for a Windows
// console handle. Windows Terminal enables this itself, but the classic
// console (conhost) does not, so without it the codes would print as literal
// text. Failures are ignored — the caller still prints, just uncolored.
func enableVirtualTerminal(f *os.File) {
	handle := windows.Handle(f.Fd())
	var mode uint32
	if err := windows.GetConsoleMode(handle, &mode); err != nil {
		return
	}
	_ = windows.SetConsoleMode(handle, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING)
}
