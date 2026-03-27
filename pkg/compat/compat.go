package compat

import "syscall"

// StatInfo extracts portable fields from a syscall.Stat_t.
type StatInfo struct {
	Mode  uint32
	Uid   uint32
	Gid   uint32
	Ctime int64
	Dev   uint64
	Rdev  uint64
}

// GetStatInfo extracts a StatInfo from an os.FileInfo's underlying Stat_t.
func GetStatInfo(stat *syscall.Stat_t) StatInfo {
	return StatInfo{
		Mode:  uint32(stat.Mode),
		Uid:   stat.Uid,
		Gid:   stat.Gid,
		Ctime: getCtime(stat),
		Dev:   getDev(stat),
		Rdev:  getRdev(stat),
	}
}
