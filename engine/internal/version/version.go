package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

var (
	// Version is the application version (set at build time)
	Version = "dev"

	// BuildTime is the build time (set at build time)
	BuildTime = "unknown"

	// GitCommit is the git commit hash (set at build time)
	GitCommit = "unknown"

	// GoVersion is the Go version used to build
	GoVersion = runtime.Version()
)

// Info holds version information
type Info struct {
	Version   string
	BuildTime string
	GitCommit string
	GoVersion string
}

// Get returns version information
func Get() Info {
	// Try to get version from build info
	if Version == "dev" {
		if info, ok := debug.ReadBuildInfo(); ok {
			for _, setting := range info.Settings {
				switch setting.Key {
				case "vcs.revision":
					if GitCommit == "unknown" {
						GitCommit = setting.Value
					}
				case "vcs.time":
					if BuildTime == "unknown" {
						BuildTime = setting.Value
					}
				}
			}
		}
	}

	return Info{
		Version:   Version,
		BuildTime: BuildTime,
		GitCommit: GitCommit,
		GoVersion: GoVersion,
	}
}

// String returns a formatted version string
func String() string {
	info := Get()
	return fmt.Sprintf("FlowMesh version %s (build time: %s, commit: %s, go: %s)",
		info.Version, info.BuildTime, info.GitCommit, info.GoVersion)
}

