package ciworkflow

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Static contract for the CI toolchain cache (hk 3590). The Go toolchain comes
// from one local composite action, .github/actions/setup-go-cached, which
// restores the toolchain from the Actions cache before actions/setup-go and
// retries the download on a miss. These tests fail when a job bypasses the
// composite, when a pin is a tag instead of a full SHA, or when a job whose
// checkout is not github.sha loads the composite from its own checkout.

const (
	workflowsDir       = "../../.github/workflows"
	compositeFile      = "../../.github/actions/setup-go-cached/action.yml"
	compositeUses      = "/setup-go-cached"
	ownRevisionActions = "./.ci-actions/"
	sideCheckoutPath   = ".ci-actions"
	goShaRef           = "${{ github.sha }}"
)

var (
	jobHeader      = regexp.MustCompile(`^  ([A-Za-z0-9_-]+):\s*$`)
	usesLine       = regexp.MustCompile(`^\s*(?:- )?uses:\s*(\S+)`)
	fullSHAUses    = regexp.MustCompile(`^([^@\s]+)@([0-9a-f]{40})$`)
	versionComment = regexp.MustCompile(`#\s*v\d+\.\d+\.\d+\s*$`)
	goCommand      = regexp.MustCompile(`(^|[\s;&|(])go (test|vet|build)\b`)
	exactVersion   = regexp.MustCompile(`^\d+\.\d+\.\d+$`)
)

type workflowFile struct {
	name  string
	lines []string
	jobs  []jobInfo
}

type jobInfo struct {
	name  string
	lines []string
	steps []stepInfo
}

// stepInfo is one list item under a steps: key, as raw lines.
type stepInfo struct {
	job   string
	index int
	lines []string
}

func indentOf(s string) int { return len(s) - len(strings.TrimLeft(s, " ")) }

func isComment(s string) bool { return strings.HasPrefix(strings.TrimSpace(s), "#") }

func isBlank(s string) bool { return strings.TrimSpace(s) == "" }

func readLines(t *testing.T, path string) []string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.Split(string(body), "\n")
}

// stepsOf returns the list items of the first steps: key in lines. Items sit
// two spaces deeper than the key; a line at or above the key's indent ends the
// list.
func stepsOf(lines []string) [][]string {
	key, start := -1, -1
	for i, l := range lines {
		if strings.TrimSpace(l) == "steps:" {
			key, start = indentOf(l), i
			break
		}
	}
	if start < 0 {
		return nil
	}
	var steps [][]string
	for _, l := range lines[start+1:] {
		if isBlank(l) || isComment(l) {
			if len(steps) > 0 {
				steps[len(steps)-1] = append(steps[len(steps)-1], l)
			}
			continue
		}
		ind := indentOf(l)
		if ind <= key {
			break
		}
		trimmed := strings.TrimSpace(l)
		if ind == key+2 && (strings.HasPrefix(trimmed, "- ") || trimmed == "-") {
			steps = append(steps, []string{l})
			continue
		}
		if len(steps) > 0 {
			steps[len(steps)-1] = append(steps[len(steps)-1], l)
		}
	}
	return steps
}

// value returns the first "key:" value in the step, without quotes or a
// trailing comment. ok is false when the key is absent.
func (s stepInfo) value(key string) (string, bool) {
	re := regexp.MustCompile(`^\s*(?:- )?` + regexp.QuoteMeta(key) + `:(.*)$`)
	for _, l := range s.lines {
		if isComment(l) {
			continue
		}
		m := re.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		v := strings.TrimSpace(m[1])
		if i := strings.Index(v, " #"); i >= 0 {
			v = strings.TrimSpace(v[:i])
		}
		return strings.Trim(v, `"'`), true
	}
	return "", false
}

func (s stepInfo) uses() string {
	v, _ := s.value("uses")
	return v
}

func (s stepInfo) where() string {
	return fmt.Sprintf("job %q step %d (uses %s)", s.job, s.index+1, s.uses())
}

func hasAction(uses, name string) bool {
	return uses == name || strings.HasPrefix(uses, name+"@")
}

// loadWorkflows parses every workflow file under workflowsDir.
func loadWorkflows(t *testing.T) []workflowFile {
	t.Helper()
	entries, err := os.ReadDir(workflowsDir)
	if err != nil {
		t.Fatalf("read %s: %v", workflowsDir, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && (strings.HasSuffix(e.Name(), ".yml") || strings.HasSuffix(e.Name(), ".yaml")) {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		t.Fatalf("%s has no workflow files", workflowsDir)
	}
	sort.Strings(names)
	var out []workflowFile
	for _, name := range names {
		lines := readLines(t, filepath.Join(workflowsDir, name))
		wf := workflowFile{name: name, lines: lines}
		start := -1
		for i, l := range lines {
			if l == "jobs:" {
				start = i
				break
			}
		}
		if start < 0 {
			t.Fatalf("%s has no top-level jobs: key", name)
		}
		cur := -1
		for _, l := range lines[start+1:] {
			if m := jobHeader.FindStringSubmatch(l); m != nil {
				wf.jobs = append(wf.jobs, jobInfo{name: m[1]})
				cur = len(wf.jobs) - 1
				continue
			}
			if !isBlank(l) && !isComment(l) && indentOf(l) == 0 {
				break
			}
			if cur >= 0 {
				wf.jobs[cur].lines = append(wf.jobs[cur].lines, l)
			}
		}
		for j := range wf.jobs {
			for i, raw := range stepsOf(wf.jobs[j].lines) {
				wf.jobs[j].steps = append(wf.jobs[j].steps, stepInfo{job: wf.jobs[j].name, index: i, lines: raw})
			}
		}
		out = append(out, wf)
	}
	return out
}

func goCommandIn(lines []string) bool {
	for _, l := range lines {
		if !isComment(l) && goCommand.MatchString(l) {
			return true
		}
	}
	return false
}

// TestParserSeesWorkflows keeps the line parser honest: the jobs and steps the
// other tests rely on must actually be found.
func TestParserSeesWorkflows(t *testing.T) {
	want := map[string][]string{
		"ci.yml":                     {"verify", "secrets", "publish", "auto-tag-test", "ci-ok-status", "deploy-dispatch"},
		"ci-any-ref.yml":             {"validate", "verify", "secrets", "record"},
		"post-ci-any-ref-status.yml": {"resolve", "post"},
		"deploy.yml":                 {"candidate", "race", "macos", "release", "deploy-ok"},
		"release.yml":                {"build", "test", "publish"},
	}
	found := map[string]map[string]bool{}
	for _, wf := range loadWorkflows(t) {
		found[wf.name] = map[string]bool{}
		for _, j := range wf.jobs {
			found[wf.name][j.name] = true
		}
	}
	for file, jobs := range want {
		for _, j := range jobs {
			if !found[file][j] {
				t.Errorf("%s: parser did not find job %q", file, j)
			}
		}
	}
	for _, wf := range loadWorkflows(t) {
		if wf.name != "deploy.yml" {
			continue
		}
		for _, j := range wf.jobs {
			if j.name == "race" && len(j.steps) < 4 {
				t.Errorf("deploy.yml race: parser found %d steps, want at least 4", len(j.steps))
			}
		}
	}
}

func jobNamed(wf workflowFile, name string) (jobInfo, bool) {
	for _, j := range wf.jobs {
		if j.name == name {
			return j, true
		}
	}
	return jobInfo{}, false
}

func hasLine(lines []string, want string) bool {
	for _, line := range lines {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

func TestAnyRefWorkflowContract(t *testing.T) {
	var anyRef, poster workflowFile
	for _, wf := range loadWorkflows(t) {
		switch wf.name {
		case "ci-any-ref.yml":
			anyRef = wf
		case "post-ci-any-ref-status.yml":
			poster = wf
		}
	}
	if anyRef.name == "" || poster.name == "" {
		t.Fatal("any-ref workflow and default-branch poster must both exist")
	}
	for _, line := range anyRef.lines {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "statuses: write") || strings.Contains(trimmed, "packages: write") {
			t.Errorf("any-ref workflow has forbidden permission %q", trimmed)
		}
	}

	// Dispatch accepts only the target ref. The validator is the first job, and
	// every job that checks out source depends on its successful completion.
	if !hasLine(anyRef.lines, "workflow_dispatch:") || !hasLine(anyRef.lines, "ref:") {
		t.Error("any-ref workflow must be dispatch-only with a ref input")
	}
	inOn := false
	var triggers, dispatchInputs []string
	for _, line := range anyRef.lines {
		if line == "on:" {
			inOn = true
			continue
		}
		if inOn && indentOf(line) == 0 && !isBlank(line) && !isComment(line) {
			break
		}
		if !inOn || isBlank(line) || isComment(line) {
			continue
		}
		if indentOf(line) == 2 && strings.HasSuffix(strings.TrimSpace(line), ":") {
			triggers = append(triggers, strings.TrimSuffix(strings.TrimSpace(line), ":"))
		}
		if indentOf(line) == 6 && strings.HasSuffix(strings.TrimSpace(line), ":") {
			dispatchInputs = append(dispatchInputs, strings.TrimSuffix(strings.TrimSpace(line), ":"))
		}
	}
	if strings.Join(triggers, ",") != "workflow_dispatch" || strings.Join(dispatchInputs, ",") != "ref" {
		t.Errorf("any-ref workflow must have only workflow_dispatch with only input ref; got triggers=%v inputs=%v", triggers, dispatchInputs)
	}
	if len(anyRef.jobs) == 0 || anyRef.jobs[0].name != "validate" {
		t.Error("validate must be the first any-ref job")
	}
	validate, ok := jobNamed(anyRef, "validate")
	if !ok || len(validate.steps) == 0 || !strings.Contains(strings.Join(validate.steps[0].lines, "\n"), "40-hex") {
		t.Error("the validator's first step must validate the exact 40-hex ref")
	}
	for _, step := range validate.steps {
		if hasAction(step.uses(), "actions/checkout") {
			t.Error("ref validation must precede every checkout")
		}
	}
	allowedJobs := map[string]bool{"validate": true, "verify": true, "secrets": true, "record": true}
	for _, job := range anyRef.jobs {
		if !allowedJobs[job.name] {
			t.Errorf("any-ref workflow has unexpected job %q; its call graph must remain limited to validation, watched checks and recording", job.name)
		}
		for _, step := range job.steps {
			if hasAction(step.uses(), "actions/checkout") && job.name != "validate" && !hasLine(job.lines, "needs: validate") {
				t.Errorf("any-ref job %q checks out before requiring validation", job.name)
			}
		}
		for _, line := range job.lines {
			trimmed := strings.TrimSpace(line)
			if strings.Contains(trimmed, "statuses: write") || strings.Contains(trimmed, "packages: write") {
				t.Errorf("any-ref job %q has forbidden permission %q", job.name, trimmed)
			}
		}
		for _, line := range job.lines {
			if strings.Contains(line, "uses: ./.github/workflows/") {
				t.Errorf("any-ref job %q calls another workflow; its permission graph is not closed", job.name)
			}
		}
	}
	if _, ok := jobNamed(anyRef, "publish"); ok {
		t.Error("any-ref graph must not contain image publishing")
	}
	if _, ok := jobNamed(anyRef, "ci-ok-status"); ok {
		t.Error("any-ref graph must not contain ci-ok status posting")
	}
	if _, ok := jobNamed(anyRef, "deploy-dispatch"); ok {
		t.Error("any-ref graph must not dispatch deploy")
	}
	if strings.Contains(strings.Join(anyRef.lines, "\n"), "./.github/workflows/deploy.yml") {
		t.Error("any-ref workflow must not invoke deploy.yml")
	}
	for _, name := range []string{"verify", "secrets"} {
		job, ok := jobNamed(anyRef, name)
		if !ok {
			t.Errorf("any-ref workflow is missing watched job %q", name)
			continue
		}
		foundRefCheckout := false
		for _, step := range job.steps {
			if hasAction(step.uses(), "actions/checkout") {
				ref, exists := step.value("ref")
				if exists && ref == "${{ inputs.ref }}" {
					foundRefCheckout = true
				}
			}
		}
		if !foundRefCheckout {
			t.Errorf("any-ref job %q must check out inputs.ref", name)
		}
	}

	// The workflow_run poster is loaded from the default branch and is the only
	// workflow on this path that can write statuses. Its job names cannot collide
	// with either watched context.
	if !hasLine(poster.lines, "workflow_run:") || !hasLine(poster.lines, `workflows: ["CI (any ref)"]`) {
		t.Error("poster must run from workflow_run for CI (any ref)")
	}
	post, ok := jobNamed(poster, "post")
	if !ok || !hasLine(post.lines, "statuses: write") {
		t.Error("only poster job must hold statuses: write")
	}
	statusPermissionLines := 0
	for _, line := range poster.lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "statuses: write" {
			statusPermissionLines++
		}
		if trimmed == "packages: write" {
			t.Error("any-ref poster path must not hold packages: write")
		}
	}
	if statusPermissionLines != 1 {
		t.Errorf("poster workflow must have exactly one statuses: write permission, got %d", statusPermissionLines)
	}
	if !ok || len(post.steps) == 0 || !strings.Contains(strings.Join(post.steps[0].lines, "\n"), "github.event.repository.default_branch") {
		t.Error("poster must check out the default branch")
	}
	for _, job := range poster.jobs {
		if job.name == "verify" || job.name == "secrets" || hasLine(job.lines, "name: verify") || hasLine(job.lines, "name: secrets") {
			t.Errorf("poster job %q collides with a watched name", job.name)
		}
		if job.name != "post" && hasLine(job.lines, "statuses: write") {
			t.Errorf("non-poster job %q has statuses: write", job.name)
		}
	}
}

// TestWorkflowPinning keeps CI reproducible: every external action in the
// workflows and the composite action is pinned to a full commit SHA with a
// version comment, and each action resolves to one SHA everywhere.
func TestWorkflowPinning(t *testing.T) {
	files := map[string][]string{}
	for _, wf := range loadWorkflows(t) {
		files[wf.name] = wf.lines
	}
	files[compositeFile] = readLines(t, compositeFile)

	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)

	shas := map[string]string{} // action -> "sha (file)"
	for _, name := range names {
		for i, l := range files[name] {
			if isComment(l) {
				continue
			}
			m := usesLine.FindStringSubmatch(l)
			if m == nil {
				continue
			}
			uses := m[1]
			if strings.HasPrefix(uses, "./") || strings.HasPrefix(uses, "docker://") {
				continue
			}
			where := fmt.Sprintf("%s:%d", name, i+1)
			pin := fullSHAUses.FindStringSubmatch(uses)
			if pin == nil {
				t.Errorf("%s: %q is not pinned to a full 40-hex commit SHA", where, uses)
				continue
			}
			if !versionComment.MatchString(l) {
				t.Errorf("%s: %q needs a trailing version comment such as # v4.2.2", where, uses)
			}
			if prev, ok := shas[pin[1]]; ok && !strings.HasPrefix(prev, pin[2]) {
				t.Errorf("%s is pinned to %s and to %s; use one SHA per action", pin[1], prev, where)
			}
			shas[pin[1]] = pin[2] + " (" + where + ")"
		}
	}
	if len(shas) == 0 {
		t.Fatal("found no pinned actions; the guard must match at least one")
	}
}

// TestNoDirectSetupGo makes every Go job go through the composite, and keeps
// the Go version in one place: no workflow may set go-version itself.
func TestNoDirectSetupGo(t *testing.T) {
	for _, wf := range loadWorkflows(t) {
		for _, j := range wf.jobs {
			for _, s := range j.steps {
				if hasAction(s.uses(), "actions/setup-go") {
					t.Errorf("%s %s: call the local composite instead of actions/setup-go, so the toolchain cache and download retry apply", wf.name, s.where())
				}
			}
			for _, l := range j.lines {
				if !isComment(l) && regexp.MustCompile(`^\s*(?:- )?go-version:`).MatchString(l) {
					t.Errorf("%s job %q: go-version is set here; the pinned version lives only in %s", wf.name, j.name, compositeFile)
				}
			}
		}
	}
}

// TestGoJobsUseComposite checks that a job running go test, vet or build
// loads the composite before its first Go command.
func TestGoJobsUseComposite(t *testing.T) {
	goJobs := 0
	for _, wf := range loadWorkflows(t) {
		for _, j := range wf.jobs {
			firstGo, firstSetup := -1, -1
			for i, s := range j.steps {
				if firstGo < 0 && goCommandIn(s.lines) {
					firstGo = i
				}
				if firstSetup < 0 && strings.HasSuffix(s.uses(), compositeUses) {
					firstSetup = i
				}
			}
			if firstGo < 0 {
				continue
			}
			goJobs++
			if firstSetup < 0 || firstSetup > firstGo {
				t.Errorf("%s job %q: the Go toolchain must be set up by %s before the first go command", wf.name, j.name, "setup-go-cached")
			}
		}
	}
	if goJobs == 0 {
		t.Fatal("found no job running go test, vet or build; the guard must match at least one")
	}
}

// TestNonSHACheckoutLoadsActionFromSHA covers a job that checks out a ref other
// than github.sha (a candidate or planned SHA). Its local action must come from
// a side checkout of .github/actions at github.sha, made after the candidate
// checkout, because an older candidate has no action.
func TestNonSHACheckoutLoadsActionFromSHA(t *testing.T) {
	nonSHAJobs := 0
	for _, wf := range loadWorkflows(t) {
		for _, j := range wf.jobs {
			var checkouts []int
			nonSHA := false
			for i, s := range j.steps {
				if !hasAction(s.uses(), "actions/checkout") {
					continue
				}
				checkouts = append(checkouts, i)
				if ref, ok := s.value("ref"); ok && ref != goShaRef {
					nonSHA = true
				}
			}
			if !nonSHA {
				continue
			}
			nonSHAJobs++
			loadsLocal := false
			for _, s := range j.steps {
				if strings.HasPrefix(s.uses(), "./") && !strings.HasPrefix(s.uses(), "./.github/workflows/") {
					loadsLocal = true
				}
			}
			if !loadsLocal {
				continue
			}
			sideIdx := -1
			if len(checkouts) > 0 {
				for _, i := range checkouts[1:] {
					s := j.steps[i]
					ref, _ := s.value("ref")
					path, _ := s.value("path")
					sparse, _ := s.value("sparse-checkout")
					if ref == goShaRef && path == sideCheckoutPath && sparse == ".github/actions" {
						sideIdx = i
						break
					}
				}
			}
			if sideIdx < 0 {
				t.Errorf("%s job %q checks out a non-github.sha ref, so it must check out .github/actions at %s into %s after the candidate checkout", wf.name, j.name, goShaRef, sideCheckoutPath)
				continue
			}
			for i, s := range j.steps {
				uses := s.uses()
				if !strings.HasPrefix(uses, "./") || strings.HasPrefix(uses, "./.github/workflows/") {
					continue
				}
				if !strings.HasPrefix(uses, ownRevisionActions) {
					t.Errorf("%s %s: a job with a non-github.sha checkout must load its local action from %s, not from its checkout", wf.name, s.where(), ownRevisionActions)
				}
				if i < sideIdx {
					t.Errorf("%s %s: the local action is used before its side checkout", wf.name, s.where())
				}
			}
		}
	}
	if nonSHAJobs == 0 {
		t.Fatal("found no job with a non-github.sha checkout; the guard must match at least one")
	}
}

// compositeStep reads the composite action's own steps.
func compositeSteps(t *testing.T) []stepInfo {
	t.Helper()
	lines := readLines(t, compositeFile)
	var out []stepInfo
	for i, raw := range stepsOf(lines) {
		out = append(out, stepInfo{job: "composite", index: i, lines: raw})
	}
	if len(out) == 0 {
		t.Fatalf("%s has no steps", compositeFile)
	}
	return out
}

// compositeInputDefault returns the default of an input in the composite.
func compositeInputDefault(t *testing.T, input string) string {
	t.Helper()
	in := false
	for _, l := range readLines(t, compositeFile) {
		if l == "  "+input+":" {
			in = true
			continue
		}
		if !in {
			continue
		}
		if !isBlank(l) && indentOf(l) <= 2 {
			break
		}
		if m := regexp.MustCompile(`^\s+default:\s*(.+?)\s*$`).FindStringSubmatch(l); m != nil {
			return strings.Trim(m[1], `"'`)
		}
	}
	t.Fatalf("%s: input %q has no default", compositeFile, input)
	return ""
}

// TestSetupGoCachedAction checks the toolchain cache and the download retry in
// the composite. The cache restores before actions/setup-go, its path and key
// carry the version, runner OS and arch, and setup-go gets three attempts.
func TestSetupGoCachedAction(t *testing.T) {
	steps := compositeSteps(t)
	cacheIdx, firstSetup, setupAttempts := -1, -1, 0
	for i, s := range steps {
		switch {
		case hasAction(s.uses(), "actions/cache"):
			if cacheIdx < 0 {
				cacheIdx = i
			}
			path, _ := s.value("path")
			if !strings.HasPrefix(path, "${{ runner.tool_cache }}/go/") {
				t.Errorf("cache path must be the tool cache's go/<version> directory, got %q", path)
			}
			key, _ := s.value("key")
			for _, want := range []string{"runner.os", "runner.arch", "inputs.go-version"} {
				if !strings.Contains(key, want) {
					t.Errorf("cache key %q must include %s so toolchains do not leak across versions or platforms", key, want)
				}
			}
		case hasAction(s.uses(), "actions/setup-go"):
			setupAttempts++
			if firstSetup < 0 {
				firstSetup = i
			}
			if v, _ := s.value("go-version"); v != "${{ inputs.go-version }}" {
				t.Errorf("setup-go must take go-version from the input, got %q", v)
			}
			if v, _ := s.value("cache"); v != "true" {
				t.Errorf("setup-go must keep its module and build cache on (cache: true), got %q", v)
			}
		}
	}
	if cacheIdx < 0 || firstSetup < 0 || cacheIdx > firstSetup {
		t.Errorf("actions/cache must restore before actions/setup-go (cache step %d, first setup-go step %d)", cacheIdx, firstSetup)
	}
	if setupAttempts != 3 {
		t.Errorf("setup-go must run three bounded attempts on a cache miss, got %d", setupAttempts)
	}
}

// TestCompositeGoVersionPinned checks the one pinned version: an exact x.y.z,
// inside the go line of go.mod.
func TestCompositeGoVersionPinned(t *testing.T) {
	v := compositeInputDefault(t, "go-version")
	if !exactVersion.MatchString(v) {
		t.Fatalf("go-version default %q must be an exact x.y.z version", v)
	}
	goMod := readLines(t, "../../go.mod")
	goLine := ""
	for _, l := range goMod {
		if strings.HasPrefix(l, "go ") {
			goLine = strings.TrimSpace(strings.TrimPrefix(l, "go "))
			break
		}
	}
	if goLine == "" {
		t.Fatal("go.mod has no go line")
	}
	if !strings.HasPrefix(v, goLine+".") {
		t.Errorf("pinned go-version %q must be inside the go.mod line %q", v, goLine)
	}
}
