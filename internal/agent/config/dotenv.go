package config

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/joho/godotenv"
)

// LoadDotenv loads env vars from the .env file at path. If the file does
// not exist, returns nil (silent skip). Already-set process env vars are NOT
// overwritten (godotenv.Load's default behavior — explicit env wins).
func LoadDotenv(path string) error {
	if err := godotenv.Load(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("load dotenv %q: %w", path, err)
	}
	return nil
}
