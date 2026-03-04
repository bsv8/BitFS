//go:build !windows

package main

func detectWindowsLocale() string {
	return ""
}
