// Package config loads service configuration from the environment, with an
// optional .env file for local development. Values already set in the real
// environment win over .env entries.
package config

import (
	"fmt"
	"os"
	"strings"
)

// LoadDotenv reads KEY=VALUE lines from path into the process environment.
// Missing file is not an error; services on Railway have no .env.
func LoadDotenv(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
	return nil
}

// MustGet returns the value of key or exits with a clear message.
func MustGet(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "missing required env var %s\n", key)
		os.Exit(1)
	}
	return v
}
