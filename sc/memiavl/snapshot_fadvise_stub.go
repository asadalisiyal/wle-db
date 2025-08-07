//go:build !linux
// +build !linux

package memiavl

func fadviseDontNeed(fd uintptr) {}
