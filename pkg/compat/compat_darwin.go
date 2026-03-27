//go:build darwin

package compat

import "syscall"

func getCtime(stat *syscall.Stat_t) int64 {
	return stat.Ctimespec.Sec
}

func getDev(stat *syscall.Stat_t) uint64 {
	return uint64(stat.Dev)
}

func getRdev(stat *syscall.Stat_t) uint64 {
	return uint64(stat.Rdev)
}
