//go:build linux

package compat

import "syscall"

func getCtime(stat *syscall.Stat_t) int64 {
	return stat.Ctim.Sec
}

func getDev(stat *syscall.Stat_t) uint64 {
	return stat.Dev
}

func getRdev(stat *syscall.Stat_t) uint64 {
	return stat.Rdev
}
