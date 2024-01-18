package toywal

import (
	"fmt"
	"path/filepath"
)

func SegmentFileName(dirPath string, fileSuffix string, id uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d%v", id, fileSuffix))
}
