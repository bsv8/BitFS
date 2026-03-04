//go:build windows

package main

import (
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	kernel32DLL                  = windows.NewLazySystemDLL("kernel32.dll")
	procGetUserDefaultLocaleName = kernel32DLL.NewProc("GetUserDefaultLocaleName")
)

func detectWindowsLocale() string {
	// Windows 文档约定最大长度是 85（包含结尾 \0）。
	buf := make([]uint16, 85)
	r1, _, _ := procGetUserDefaultLocaleName.Call(
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)),
	)
	if r1 == 0 {
		return ""
	}
	return strings.TrimSpace(windows.UTF16ToString(buf))
}
