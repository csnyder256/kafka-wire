package broker

import (
	"errors"
	"os"
)

// readDirSafe is os.ReadDir but returns an empty slice instead of an
// error when the directory doesn't exist (vs. a real I/O error).
func readDirSafe(dir string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return entries, nil
}

// removeAllSafe is os.RemoveAll but returns nil when the path is
// already absent.
func removeAllSafe(path string) error {
	if err := os.RemoveAll(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return nil
}
