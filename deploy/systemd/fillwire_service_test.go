// Package systemd statically verifies that the fillwire container unit
// example keeps its deployment invariants: digest-pinned image only,
// secrets via --env-file, read-only config mounts, and restart bounds.
package systemd

import (
	"fmt"
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

// parseUnit splits unit text into section -> ordered key=value directives,
// dropping blank lines and '#' / ';' comments.
func parseUnit(text string) map[string][][2]string {
	sections := map[string][][2]string{}
	section := ""
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = line
			continue
		}
		if key, value, found := strings.Cut(line, "="); found && section != "" {
			sections[section] = append(sections[section], [2]string{key, value})
		}
	}
	return sections
}

// directives returns every value for key inside one section.
func directives(sections map[string][][2]string, section, key string) []string {
	var values []string
	for _, kv := range sections[section] {
		if kv[0] == key {
			values = append(values, kv[1])
		}
	}
	return values
}

// directiveCount returns the total number of key= directives across all sections.
func directiveCount(sections map[string][][2]string, key string) int {
	count := 0
	for _, kvs := range sections {
		for _, kv := range kvs {
			if kv[0] == key {
				count++
			}
		}
	}
	return count
}

// validateUnit returns every deploy-invariant violation found in a unit file.
func validateUnit(text string) []string {
	var violations []string
	if strings.Contains(text, ":latest") {
		violations = append(violations, "must not reference a :latest tag")
	}
	sections := parseUnit(text)

	// The pin file is fed to systemd through exactly one EnvironmentFile.
	// Its value must be a literal absolute path: systemd expands only '%'
	// specifiers there, so a $VARIABLE indirection would read nothing.
	if total := directiveCount(sections, "EnvironmentFile"); total != 1 {
		violations = append(violations, fmt.Sprintf("expected exactly one EnvironmentFile= directive, got %d", total))
	}
	for _, value := range directives(sections, "[Service]", "EnvironmentFile") {
		if !strings.HasPrefix(value, "/") {
			violations = append(violations, "EnvironmentFile must be an absolute path without '-' prefix")
		}
		if strings.Contains(value, "$") {
			violations = append(violations, "EnvironmentFile must not use $VARIABLE expansion")
		}
	}

	// The image argument must be the ${IMAGE} token; nothing else may name
	// an image, and matching text inside a comment must not satisfy this.
	execStarts := directives(sections, "[Service]", "ExecStart")
	if len(execStarts) != 1 {
		violations = append(violations, fmt.Sprintf("expected exactly one ExecStart= directive, got %d", len(execStarts)))
	} else {
		imageTokens := 0
		for _, field := range strings.Fields(execStarts[0]) {
			if field == "${IMAGE}" {
				imageTokens++
			}
		}
		if imageTokens != 1 {
			violations = append(violations, "ExecStart must contain exactly one ${IMAGE} image argument")
		}
		if strings.Contains(execStarts[0], "ghcr.io") {
			violations = append(violations, "ExecStart must not hard-code an image reference")
		}
		if !strings.Contains(execStarts[0], "--env-file ") {
			violations = append(violations, "secrets must be injected with --env-file")
		}
		if regexp.MustCompile(`(^|\s)(-e|--env)[=\s]+\S`).MatchString(execStarts[0]) {
			violations = append(violations, "inline -e/--env KEY=value assignments are forbidden")
		}
		mounts := regexp.MustCompile(`(?:^|\s)(?:-v|--volume)[=\s]+([^\s]+)`).FindAllStringSubmatch(execStarts[0], -1)
		if len(mounts) == 0 {
			violations = append(violations, "expected at least one bind mount for the TOML config")
		}
		for _, mount := range mounts {
			if !strings.HasSuffix(mount[1], ":ro") {
				violations = append(violations, "bind mount must be read-only (:ro): "+mount[1])
			}
		}
	}

	// Restart bounds: the rate limit lives in [Unit], and exit codes that
	// retrying can never fix live in [Service].
	for _, directive := range []string{"StartLimitIntervalSec", "StartLimitBurst"} {
		if len(directives(sections, "[Unit]", directive)) != 1 {
			violations = append(violations, "missing [Unit] directive "+directive)
		}
	}
	prevents := directives(sections, "[Service]", "RestartPreventExitStatus")
	if len(prevents) != 1 {
		violations = append(violations, "missing [Service] directive RestartPreventExitStatus")
	} else {
		found := false
		for _, field := range strings.Fields(prevents[0]) {
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

// Mutant: deleting the EnvironmentFile directive must be rejected — the
// unit would start with an empty ${IMAGE} argument.
func TestMutantEnvironmentFileRemoved(t *testing.T) {
	var kept []string
	for _, line := range strings.Split(readExample(t, unitExamplePath), "\n") {
		if !strings.HasPrefix(line, "EnvironmentFile=") {
			kept = append(kept, line)
		}
	}
	if len(validateUnit(strings.Join(kept, "\n"))) == 0 {
		t.Fatal("validateUnit accepted a unit with no EnvironmentFile directive")
	}
}

// Mutant: a ${STATE_DIR}-style indirection in EnvironmentFile must be
// rejected — systemd never expands $VARIABLES there.
func TestMutantEnvironmentFileVariable(t *testing.T) {
	mutated := strings.Replace(readExample(t, unitExamplePath),
		"EnvironmentFile=/var/lib/fillwire/fillwire.service.image",
		"EnvironmentFile=${STATE_DIR}/fillwire.service.image", 1)
	if len(validateUnit(mutated)) == 0 {
		t.Fatal("validateUnit accepted a $VARIABLE-indirection EnvironmentFile mutant")
	}
}

// Mutant: ${IMAGE} surviving only inside a comment must be rejected — the
// ExecStart line would carry no image argument at all.
func TestMutantImageOnlyInComment(t *testing.T) {
	mutated := strings.Replace(readExample(t, unitExamplePath), "${IMAGE} -config", "-config", 1)
	mutated += "# reminder: ExecStart should run ${IMAGE}\n"
	if len(validateUnit(mutated)) == 0 {
		t.Fatal("validateUnit accepted ${IMAGE} surviving only in a comment")
	}
}
