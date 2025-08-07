//go:build linux
// +build linux

package memiavl

import "golang.org/x/sys/unix"

func fadviseDontNeed(fd uintptr) {
	_ = unix.Fadvise(int(fd), 0, 0, unix.FADV_DONTNEED)
}
