// Package systemd statically verifies that the fillwire container unit
// example keeps its deployment invariants: digest-pinned image only,
// secrets via --env-file, read-only config mounts, and restart bounds.
package systemd

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

const (
	unitExamplePath  = "fillwire.service.example"
	imageExamplePath = "fillwire.service.image.example"
)

var imagePinPattern = regexp.MustCompile(`^IMAGE=ghcr\.io/mgh3326/fillwire@sha256:[0-9a-f]{64}$`)

// validateUnit returns every deploy-invariant violation found in a unit file.
func validateUnit(text string) []string {
	var violations []string
	if strings.Contains(text, ":latest") {
		violations = append(violations, "must not reference a :latest tag")
	}
	if !strings.Contains(text, "${IMAGE}") {
		violations = append(violations, "ExecStart must run ${IMAGE} from the EnvironmentFile")
	}
	if !strings.Contains(text, "--env-file ") {
		violations = append(violations, "secrets must be injected with --env-file")
	}
	if regexp.MustCompile(`(^|\s)(-e|--env)[=\s]+\S`).MatchString(text) {
		violations = append(violations, "inline -e/--env KEY=value assignments are forbidden")
	}
	execStart := regexp.MustCompile(`(?m)^ExecStart=(.+)$`).FindStringSubmatch(text)
	if execStart == nil {
		violations = append(violations, "missing ExecStart line")
	} else {
		mounts := regexp.MustCompile(`(?:^|\s)(?:-v|--volume)[=\s]+([^\s]+)`).FindAllStringSubmatch(execStart[1], -1)
		if len(mounts) == 0 {
			violations = append(violations, "expected at least one bind mount for the TOML config")
		}
		for _, mount := range mounts {
			if !strings.HasSuffix(mount[1], ":ro") {
				violations = append(violations, "bind mount must be read-only (:ro): "+mount[1])
			}
		}
	}
	for _, directive := range []string{"StartLimitIntervalSec=", "StartLimitBurst=", "RestartPreventExitStatus="} {
		if !strings.Contains(text, directive) {
			violations = append(violations, "missing restart-bound directive "+directive)
		}
	}
	prevent := regexp.MustCompile(`(?m)^RestartPreventExitStatus=(.+)$`).FindStringSubmatch(text)
	if prevent != nil {
		fields := strings.Fields(prevent[1])
		found := false
		for _, field := range fields {
			if field == "42" {
				found = true
			}
		}
		if !found {
			violations = append(violations, "RestartPreventExitStatus must include exit 42 (KIS session occupied)")
		}
	}
	return violations
}

// validateImagePin returns every violation found in a pin-state file.
func validateImagePin(text string) []string {
	var violations []string
	if strings.Contains(text, ":latest") {
		violations = append(violations, "pin must not reference a :latest tag")
	}
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) != 1 {
		violations = append(violations, "pin file must contain exactly one line")
		return violations
	}
	if !imagePinPattern.MatchString(lines[0]) {
		violations = append(violations, "pin must match IMAGE=ghcr.io/mgh3326/fillwire@sha256:<64 lowercase hex>")
	}
	return violations
}

func readExample(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}

func TestUnitExample(t *testing.T) {
	if violations := validateUnit(readExample(t, unitExamplePath)); len(violations) > 0 {
		t.Fatalf("%s violates deploy invariants:\n%s", unitExamplePath, strings.Join(violations, "\n"))
	}
}

func TestImagePinExample(t *testing.T) {
	if violations := validateImagePin(readExample(t, imageExamplePath)); len(violations) > 0 {
		t.Fatalf("%s violates deploy invariants:\n%s", imageExamplePath, strings.Join(violations, "\n"))
	}
}

// Mutant: swapping the digest pin for a :latest tag must be rejected.
func TestMutantLatestTag(t *testing.T) {
	mutated := strings.ReplaceAll(readExample(t, imageExamplePath), "@sha256:0000000000000000000000000000000000000000000000000000000000000000", ":latest")
	if len(validateImagePin(mutated)) == 0 {
		t.Fatal("validateImagePin accepted a :latest tag mutant")
	}
	unitMutant := strings.ReplaceAll(readExample(t, unitExamplePath), "${IMAGE}", "ghcr.io/mgh3326/fillwire:latest")
	if len(validateUnit(unitMutant)) == 0 {
		t.Fatal("validateUnit accepted a :latest tag mutant")
	}
}

// Mutant: dropping :ro from the config mount must be rejected.
func TestMutantWritableMount(t *testing.T) {
	mutated := strings.ReplaceAll(readExample(t, unitExamplePath), ":ro", "")
	if len(validateUnit(mutated)) == 0 {
		t.Fatal("validateUnit accepted a writable config-mount mutant")
	}
}
