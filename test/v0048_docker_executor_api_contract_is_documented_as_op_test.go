package k8sopenhands_test

// Requirement: Docker Executor API contract is documented as OpenAPI (row 320),
// K8s analogue: the K8s Executor keeps the JSON Schemas of what it publishes
// under schemas/ (every publish in every test is
// validated against them by the harness), and the fixtures it consumes match
// the publisher files in the tracked Gitea declaration projection.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
)

func repoRoot() string { return filepath.Dir(schemaDir()) }

func fixtureDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("FLOWAI_CONTRACT_FIXTURE_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("FLOWAI_CONTRACT_FIXTURE_DIR must point to the absolute staged publisher schema directory")
	}
	return dir
}

// repoSchemas resolves publisher-owned absolute schema IDs in the staged
// fixture tree. The file loader handles relative references between fixtures.
type repoSchemas struct{ dir string }

func (r repoSchemas) Load(url string) (any, error) {
	rest, ok := strings.CutPrefix(url, "https://flowai.local/schemas/")
	svc, file, hasFile := strings.Cut(rest, "/")
	if !ok || !hasFile || !filepath.IsLocal(svc) || !filepath.IsLocal(file) || strings.Contains(file, "/") {
		return nil, fmt.Errorf("unknown schema URL %s", url)
	}
	f, err := os.Open(filepath.Join(r.dir, "svc", svc, "schemas", file))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return jsonschema.UnmarshalJSON(f)
}

func compileAt(t *testing.T, rel string) *jsonschema.Schema {
	t.Helper()
	dir := fixtureDir(t)
	c := jsonschema.NewCompiler()
	c.UseLoader(jsonschema.SchemeURLLoader{"file": jsonschema.FileLoader{}, "https": repoSchemas{dir: dir}})
	s, err := c.Compile(filepath.Join(dir, rel))
	if err != nil {
		t.Fatalf("compile %s: %v", rel, err)
	}
	return s
}

func validJSON(t *testing.T, s *jsonschema.Schema, what string, data []byte) {
	t.Helper()
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if err := s.Validate(v); err != nil {
		t.Fatalf("%s does not match the publisher's schema: %v\n%s", what, err, data)
	}
}

// Scenario: Developer implements Executor service clients (row 320).
func TestDeveloperImplementsExecutorServiceClients(t *testing.T) {
	// The Executor's own schemas are declared test inputs of this service.
	inputs, err := os.ReadFile(filepath.Join(repoRoot(), "qualification", "schema-declarations.json"))
	if err != nil {
		t.Fatal(err)
	}
	var declared struct {
		Schemas map[string][]string `json:"schemas"`
	}
	if err := json.Unmarshal(inputs, &declared); err != nil {
		t.Fatal(err)
	}
	consumed := []string{
		"svc/task-manager/schemas/task.schema.json", "svc/task-manager/schemas/command.schema.json",
		"svc/task-manager/schemas/executor.claim-token.reply.schema.json", "svc/task-manager/schemas/error.reply.schema.json",
		"svc/executor-registry/schemas/executor.heartbeat.reply.schema.json",
		"svc/model-registry/schemas/executor.check.reply.schema.json", "svc/model-registry/schemas/edge.litellm-key.reply.schema.json",
		"svc/env-registry/schemas/executor.launch-parameters-open.reply.schema.json", "svc/env-registry/schemas/error.reply.schema.json",
	}
	for _, name := range []string{"executor-event", "claim-token-request", "heartbeat-request", "check-request"} {
		consumed = append(consumed, "executor/k8s-openhands/schemas/"+name+".schema.json")
	}
	for _, rel := range consumed {
		ok := false
		for _, svc := range declared.Schemas[rel] {
			ok = ok || svc == "executor_k8s_openhands"
		}
		if !ok {
			t.Fatalf("%s is not declared as a test input of executor_k8s_openhands", rel)
		}
	}

	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.finish(tk, "finished")
	h.waitKind(tk, "finished")

	// Consumed messages and replies match the publishers' schemas.
	validJSON(t, compileAt(t, "svc/task-manager/schemas/task.schema.json"), "task message", mustMarshal(h.newTask().Msg))
	cmd := h.cancelCommand(tk, 9)
	validJSON(t, compileAt(t, "svc/task-manager/schemas/command.schema.json"), "cancel command", cmd.Msg.Data)
	ct := h.claimTokenRequests()[0]
	validJSON(t, compileAt(t, "svc/task-manager/schemas/executor.claim-token.request.schema.json"), "claim-token request", ct.Data)
	validJSON(t, compileAt(t, "svc/task-manager/schemas/executor.claim-token.reply.schema.json"), "claim-token reply", claimTokenReply(ct, "").Data)
	validJSON(t, compileAt(t, "svc/task-manager/schemas/error.reply.schema.json"), "claim-token error", fakebroker.ErrorReply(409, "claim_rejected").Data)
	hb := h.heartbeats()[0]
	validJSON(t, compileAt(t, "svc/executor-registry/schemas/executor.heartbeat.request.schema.json"), "heartbeat request", hb.Data)
	r, _ := echoHeartbeat(hb)
	validJSON(t, compileAt(t, "svc/executor-registry/schemas/executor.heartbeat.reply.schema.json"), "heartbeat reply", r.Data)
	chk := h.checkRequests()[0]
	validJSON(t, compileAt(t, "svc/model-registry/schemas/executor.check.request.schema.json"), "check request", chk.Data)
	validJSON(t, compileAt(t, "svc/model-registry/schemas/executor.check.reply.schema.json"), "check reply",
		mustMarshal(map[string]any{"result": "ok", "model_alias": "3/m1"}))
	_, open := h.envOpen("1")
	validJSON(t, compileAt(t, "svc/env-registry/schemas/executor.launch-parameters-open.reply.schema.json"), "open reply", mustMarshal(open))
	_, openErr := envError(422, "configuration_error", "missing_required_parameter")
	validJSON(t, compileAt(t, "svc/env-registry/schemas/error.reply.schema.json"), "open error", mustMarshal(openErr))
	_, key := h.keyEdge("1")
	validJSON(t, compileAt(t, "svc/model-registry/schemas/edge.litellm-key.reply.schema.json"), "key reply", mustMarshal(key))
	_, keyErr := edgeError(422, "configuration_error", "llm_configuration_incomplete")
	validJSON(t, compileAt(t, "svc/model-registry/schemas/edge.litellm-key.reply.schema.json"), "key error", mustMarshal(keyErr))

	// The claim token is the only HTTP credential.
	for _, c := range h.edgeRequests("") {
		if !strings.HasPrefix(c.Authorization, "Bearer claim-token-") {
			t.Fatalf("edge %s authorization %q", c.Path, c.Authorization)
		}
	}
	for _, want := range []string{"/v1/tasks/" + itoa(tk.TaskID) + "/launch-parameters/open", "/v1/tasks/" + itoa(tk.TaskID) + "/litellm-key"} {
		if len(h.edgeRequests(want)) != 1 {
			t.Fatalf("edge %s called %d times", want, len(h.edgeRequests(want)))
		}
	}
}

func mustMarshal(v any) []byte { b, _ := json.Marshal(v); return b }
