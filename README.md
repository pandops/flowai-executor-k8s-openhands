# executor_k8s_openhands

`executor_k8s_openhands` is the concrete K8s Executor. It runs each won claim
in one Kubernetes Pod with an OpenHands agent server and talks to the
platform only through the NATS JetStream broker and two HTTP edges called
with the claim token. It keeps no task history; task-manager owns claims and
executor-registry owns the Executor's connection state.

## Identity and configuration

The Executor's one-time JWT from executor-registry is its only credential.
Its `name` claim pins `team_id`, `executor_id`, `tag` and `executor_type`.

The Helm chart renders the whole configuration, including `broker.token`,
into the Secret `<name>-config`. The `install-config` init container copies
it to `/var/lib/flowai/executor-k8s/config/config.yaml` on the Executor's
volume with mode `0600`. The Executor reads that file on every start.
The deployment repository owns release installation and token reissue.

The standalone packaging workflow uses a frozen copy of the baseline chart and
a synthetic value in [qualification](qualification/). This fixture renders
locally and requires no cluster or Executor JWT.

See [configs/executor_k8s_openhands.yaml](configs/executor_k8s_openhands.yaml)
for every key and its default.

## Broker surface

| Purpose     | Subject or consumer                                                                                         |
| ----------- | ----------------------------------------------------------------------------------------------------------- |
| Tasks       | durable `tasks-<team_id>-<sha256(tag)[:16]>` on `TASK_MANAGER_TASK`, pulled up to the free slots            |
| Commands    | durable `commands-<executor_id>` on `TASK_MANAGER_COMMAND`, created by the Executor on every connect        |
| Events      | `executor.event.<executor_id>.<kind>.pub`, `Nats-Msg-Id` `executor-<team_id>-<executor_id>-<publisher_seq>` |
| Heartbeat   | request `executor-registry.executor.<executor_id>.heartbeat.pub`                                            |
| Claim token | request `task-manager.executor.<executor_id>.claim-token.pub`                                               |
| Model check | request `model-registry.executor.<executor_id>.check.pub`                                                   |

Requests carry the headers `team` and `request_id` and the body
`{"params":{},"body":{...}}`. The inbox prefix is `_INBOX.executor-<id>`.
The JSON Schemas of everything the Executor publishes or requests are in
[schemas/](schemas/).

## Claim lifecycle

1. Pull a task, check team and tag, publish `created`, acknowledge the
   message (a foreign or malformed message is terminated and reported as
   `protocol_violation`).
2. Ask task-manager for the claim token; `claim_not_processed` is repeated
   every second for 10 s, `claim_rejected` drops the claim.
3. Check the image and the pinned revision, then the model check, the
   launch-parameter open (env-registry) and the LiteLLM key (model-registry).
   Temporary reasons keep the claim `pending` (rechecked every
   `pending_recheck_interval`) until `pending_limit_seconds`, then
   `released pending_timeout`. Configuration errors publish `failed` with
   `retryable: false`.
4. Create the Pod with the resolved image verbatim, the identity labels and
   the LiteLLM data-plane labels; start the OpenHands conversation and
   publish `running`. Assistant and tool output becomes `log` events.
5. The OpenHands status decides the terminal event (`finished`, or `failed`
   `openhands_<status>`); Pod failures are `failed` with
   `image_pull_failed`, `pod_failed`, `pod_restarted_before_finish` or
   `agent_server_failed`, all retryable. After the broker acknowledged the
   terminal event and the cleanup delay passed, the Pod is deleted and the
   slot is freed.
6. A cancel command pauses the agent, deletes the Pod and publishes
   `control_acknowledged` and `cancelled`, or `control_failed`.

Every event is written to the bbolt outbox before its publish and retried
with the same bytes until acknowledged; `publisher_seq` is fsynced to
`executor.state` before each use.

## Heartbeats and connection loss

A heartbeat goes every `heartbeat_interval` with the held claims, the slots
and the primary interface address. After `heartbeat_miss_limit` misses (or a
revoked JWT) the Executor pauses its agents and stops pulling. A reply with
`claims_known: true` restores the connection: listed claims continue,
omitted ones are dropped, and `executor_connection_lost` and
`executor_connection_restored` are published. When `connection_wait` passes
without a reply, every claim is dropped and its Pod deleted.

## Restart and shutdown

On start the Executor deletes its leftover task Pods (by
`flowai.executor_id` and `flowai.runtime=k8s`), republishes pending outbox
events and publishes `released executor_restarted` for each cached claim
younger than `connection_wait`. One process holds `cache/executor.lock`.

SIGTERM publishes `executor_stopping`, releases created and pending claims at
once, lets running claims finish for `drain_timeout_seconds` (default 30),
releases the rest with `executor_shutdown`, deletes all Pods and publishes
`executor_stopped`. The chart sets `terminationGracePeriodSeconds` to the
drain plus 30. Without a broker connection the Executor deletes its Pods at
once and publishes nothing.

## Probes

One listener (`executor_api_bind`, default `:8080`) serves only
`GET /v1/livez` and `GET /v1/readyz`. Ready means connected with a valid JWT,
owning the command consumer, not in connection loss and able to reach the
Kubernetes API.

## Tests

```bash
GOFLAGS=-p=2 npm test -- --service=executor_k8s_openhands --category=unit --category=integration
```
