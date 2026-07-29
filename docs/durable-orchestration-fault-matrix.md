# Durable Orchestration 故障注入验收矩阵

本文是 `docs/durable-orchestration-spec.md` 第 18 节 F01–F30 的可执行证据索引。
映射只引用具体测试方法；“相关测试存在”不算证据。

状态含义：

- **满足**：当前自动化测试直接注入该故障窗口，并断言规范要求的持久状态和恢复结果。
- **部分满足**：核心安全状态已证明，但规范中的一个独立能力仍未实现或未被端到端证明。
- **未满足**：没有可执行证据，不计入通过率。

所有新增注入都使用 fake Activity、显式逻辑时钟、Store fault seam 或固定并发 barrier，
不依赖真实时间窗口碰运气。

## F01–F30 证据

| ID | 状态 | 具体测试方法 | 持久状态与可核验证据 |
|---|---|---|---|
| F01 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f01_crash_before_run_created_leaves_no_partial_run_and_retry_works` | Store 在 `create_run` 前受控抛错；断言 Run/Event 均不存在。解除故障后用相同 `run_id` 创建成功，只有一个 Run，projection 可验证。 |
| F02 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f02_fault_after_event_insert_rolls_back_projection_and_has_no_gap` | 在 compound claim 的 `event.after_insert` 抛错；idempotency、Attempt、Run 和 Event 一起回滚。重试后的 Event sequence 恰为前一 sequence + 1。 |
| F03 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f03_restart_between_schedule_and_claim_reuses_same_attempt` | 显式停在 `Attempt=SCHEDULED`，重建 Store/Scheduler 后 claim；claim 的 `attempt_id` 与崩溃前相同，Attempt 总数仍为 1。 |
| F04 | 满足 | `tests.test_orchestration_lease.DurableLeaseRecoveryTests.test_claimed_crash_retries_once_and_reaper_restart_is_idempotent` | CLAIMED 且没有 `attempt.started`；重启后的 Reaper 将旧 Attempt 置为 ABANDONED、Node 置为 WAITING_RETRY 并持久化 `retry_due_at`。第二次重启扫描不重复写 Event。 |
| F05 | 满足 | `tests.test_orchestration_lease.DurableLeaseRecoveryTests.test_running_effect_classes_recover_conservatively`；`tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f12_stale_worker_rejection_is_audited_once_without_secret_echo` | read-only Attempt 在 `attempt.started` 后保持 RUNNING；lease 到期后旧 Attempt ABANDONED，并可创建一个 fencing 更高的新 Attempt。 |
| F06 | 满足 | `tests.test_orchestration_lease.DurableLeaseRecoveryTests.test_running_effect_classes_recover_conservatively`；`tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f10_receipt_commit_fault_leaves_orphan_and_recovers_by_effect_class` | 工具已返回但完成事务未提交，与 F05 具有相同持久边界：RUNNING 且无 Receipt。注入 Tx D 失败后 read-only 恢复为 ABANDONED/WAITING_RETRY。 |
| F07 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f07_committed_idempotent_write_is_probed_without_duplicate_effect` | fake 外部 operation ledger 只写一次；崩溃时 Attempt=RUNNING。恢复只调用 probe，验证 Artifact 后置为 SUCCEEDED，不创建第二个 Attempt/外部结果。 |
| F08 | 满足 | `tests.test_orchestration_lease.DurableLeaseRecoveryTests.test_running_effect_classes_recover_conservatively`；`tests.test_orchestration_executor.TrustedActivityExecutorTests.test_destructive_backend_uncertainty_moves_run_to_recovery` | non-idempotent/destructive 的外部结果不可证明时，Attempt=OUTCOME_UNKNOWN，Node/Run=WAITING_RECOVERY；probe 不被调用，Scheduler 不自动 retry。 |
| F09 | 满足 | `tests.test_orchestration_scheduler.DurableSchedulerTests.test_rejected_written_artifact_is_orphaned_not_referenced`；`tests.test_orchestration_artifacts_gc.ArtifactGarbageCollectorTests.test_f09_old_orphan_is_dry_run_by_default_then_restorable`、`test_committed_complete_ref_is_retained_but_digest_string_is_not`、`test_gc_claim_linearizes_against_concurrent_run_reference`、`test_crash_after_claim_is_recovered_without_losing_source`、`test_crash_after_move_is_recovered_and_restorable`、`test_failure_after_move_rolls_back_file_and_claim`、`test_restore_release_uncertainty_keeps_restored_bytes_available`；`tests.test_orchestration_store.DurableRunStoreTests.test_version_two_migration_backfills_artifact_references`、`test_append_event_rejects_reference_to_claimed_artifact`、`test_compound_event_rolls_back_reference_to_claimed_artifact` | Artifact 写完但登记前失败时保持孤立且没有 Run Event 引用。schema v3 从 v2 Event 回填 canonical 引用索引；三条 Event 写入路径在同一 Tx 内以逐对象 tombstone 与 GC claim 线性化。固定 barrier 证明 claim 胜出时源字节仍可读且新 Event/Run 引用整体拒绝；Event 胜出时 GC 跳过。claim 后或 move 后崩溃由下一次持锁操作恢复，普通异常回滚文件与 claim；restore 清 tombstone 的 commit 响应不确定时保留已验证源字节，避免“无 claim 且内容又回到隔离区”。regular/temporary collect、list、restore 共用跨进程锁。GC 仍默认 dry-run，使用 grace、二次扫描和同文件系统 quarantine 保守筛选且不直接 unlink；该保证不覆盖绕过 Store Event Tx 的任意外部登记。 |
| F10 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f10_receipt_commit_fault_leaves_orphan_and_recovers_by_effect_class`；`tests.test_orchestration_store.DurableRunStoreTests.test_compound_complete_faults_roll_back_receipt_and_projections` | Artifact 已完整写入后，在 Receipt/idempotency compound Tx 内抛错；无完成 Event、无悬空引用，Attempt 仍 RUNNING。lease 到期后按 read-only 规则恢复。Store 的各 Tx fault stage 也逐一证明整体回滚。 |
| F11 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f11_completion_response_loss_is_idempotent` | fake 外部执行和 Artifact writer 各调用一次；第一次 Tx D 已持久化 SUCCEEDED + 真实、已验证 ArtifactRef。模拟响应丢失后重复完成返回同一 Record/Event，Event 数不增加。 |
| F12 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f12_stale_worker_rejection_is_audited_once_without_secret_echo`；`tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f12_audit_dedup_survives_later_fencing_generation` | lease recovery 先持久化更高 fencing token；随后 8 个带不同 `occurred_at` 的并发旧 worker completion 均抛原 `IdempotencyConflictError`。失败事务回滚后只追加一条幂等 `activity.commit_rejected`，Node/Attempt/idempotency 业务状态不变；重复攻击不增长。跨后续 fencing generation 的同 Attempt+reason 拒绝保留 first-write-wins 审计快照，不发生 intent 冲突。SQLite 审计行、Domain Event、Web/telemetry 安全投影均不回显拒绝输入，logical replay 与在线 projection 一致。 |
| F13 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f13_retry_backoff_restart_schedules_once_at_persisted_due_time` | 失败 Tx 原子写入 WAITING_RETRY + `retry_due_at=101`。重启后 100.999 不调度；101 时两个并发 Scheduler 只有一个成功创建/claim 第二个 Attempt。 |
| F14 | 满足 | `tests.test_orchestration_deadline.DurableDeadlineTests.test_read_only_execution_timeout_retries`；`tests.test_orchestration_deadline.DurableDeadlineTests.test_execution_and_heartbeat_tie_prefers_execution`；`tests.test_orchestration_process_backend.LocalProcessSupervisorBackendTests.test_timeout_terminates_parent_child_and_grandchild_group`；同类的 `test_selector_registration_failure_reaps_process_tree_and_streams` | execution deadline 在边界触发 TIMED_OUT 并按 retry policy 进入 WAITING_RETRY。DEVELOPMENT_UNSAFE read-only 本地进程 backend 使用独立 POSIX process group；wall timeout 或 Popen 后监督初始化/读取异常均执行 TERM→有界等待→KILL→wait，并关闭 selector/streams；fixture 验证 parent/child/grandchild 均无运行残留。该证据不宣称 CPU/memory/process-count 隔离或任意代码安全沙箱。 |
| F15 | 满足 | `tests.test_orchestration_deadline.DurableDeadlineTests.test_running_effect_classes_are_conservative_and_probe_writes`；`tests.test_orchestration_deadline.DurableDeadlineTests.test_run_deadline_hook_and_running_write_never_looks_cancelled` | timeout/run deadline 对 non-idempotent/destructive 写不推断“未执行”，持久化 OUTCOME_UNKNOWN/WAITING_RECOVERY，绝不伪装 CANCELLED 或自动 retry。 |
| F16 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f16_cancel_intent_survives_restart_and_converges`、`test_f16_running_worker_observes_restarted_store_cancel_intent`；`tests.test_orchestration_process_backend.LocalProcessSupervisorBackendTests.test_durable_cancellation_reaps_running_process_group`、`test_cancellation_probe_identity_cannot_be_substituted` | SCHEDULED Attempt 在重启后继续取消传播。运行中 worker 通过绑定 Run/Node/Attempt 的 Store probe 观察持久 cancel，进程 backend 对整个进程组 TERM→KILL→reap，Executor 只在 durable intent 存在时提交 CANCELLED Receipt；伪造或错绑信号 fail closed。 |
| F17 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f17_cancel_success_race_has_one_legal_order_and_keeps_receipt`；同文件的 `test_duplicate_cancel_intents_converge_after_projection_race`、`test_pause_retries_when_completion_advances_run_projection`、`test_duplicate_resume_intents_converge_after_projection_race` | barrier 固定 control intent 与 success/重复 intent 的 CAS 竞争。SQLite Tx 形成单一顺序；control intent 仅在确认 Run projection 已被并发事务推进时有界重读，重复请求只产生一个 intent Event，真实同版本冲突仍 fail closed。success 提交时保留真实 Artifact Receipt，最终 projection 可由 Event 验证。 |
| F18 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f18_pause_restart_blocks_new_claim_until_safe_boundary` | PAUSING + RUNNING Attempt 跨重启保存；重启后不能 claim 新工作。worker 确认安全边界后 Attempt=CANCELLED、Node=PAUSED、Run=PAUSED。 |
| F19 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f19_waiting_approval_restart_keeps_single_resolution`；`tests.test_orchestration_executor.TrustedActivityExecutorTests.test_required_approval_waits_durably_and_resumes_same_attempt`、`test_trusted_rejection_terminalizes_pending_approval`、`test_pending_activity_approval_can_be_cancelled_durably`；`tests.test_orchestration_scheduler.DurableSchedulerTests.test_concurrent_identical_approval_resolution_commits_once`、`test_approval_retries_after_unrelated_run_projection_update`、`test_approval_does_not_swallow_non_concurrent_store_conflict` | policy `REQUIRE_APPROVAL` 原子释放 claim lease，并把 Run/Node/Attempt 持久化为 WAITING_APPROVAL。重启后保持同一 request/Attempt；可信 grant 使同一 Attempt 以更高 fencing 重新 claim 且 backend 只执行一次，可信 rejection/cancel 均在零 backend 调用下终止。重复同决议只产生一个 resolution，冲突决议 fail closed。 |
| F20 | 满足 | `tests.test_orchestration_executor.TrustedActivityExecutorTests.test_f20_restart_reuses_authorization_then_executes_once` | approval issuance/policy authorization 已提交后注入进程崩溃，backend 调用数仍为 0。重启复用同一 durable authorization，`attempt.started` 与 backend 执行各一次。 |
| F21 | 满足 | `tests.test_orchestration_hierarchy.DurableHierarchyTests.test_map_enforces_concurrency_and_merges_out_of_order_by_index`；`tests.test_orchestration_scheduler.DurableSchedulerTests.test_diamond_dag_parallel_ready_and_join_completion` | child 以 1、2、0 的顺序完成，Event sequence 独立递增；最终聚合严格按 child index 0、1、2 排序，join/Run 结果确定。 |
| F22 | 满足 | `tests.test_orchestration_hierarchy.DurableHierarchyTests.test_cancel_and_pause_propagate_and_parent_waits` | 父 Run 先进入 CANCELLING，child 收到 CANCELLING；父 Node 保持 RUNNING，直到 child CANCELLED 后才收敛为父 CANCELLED。 |
| F23 | 满足 | `tests.test_orchestration_telemetry.OrchestrationTelemetryTests.test_event_sink_and_otel_failures_do_not_mutate_committed_run` | EventSink 和 OTel 同时抛错；Domain Run、Event、projection 保持正确，telemetry cursor 只记录 attempted，不声称远端 ack。 |
| F24 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f23_f24_telemetry_neither_controls_nor_invents_domain_state` | telemetry 已成功导出已有 Event；随后在新 Domain Event insert 后注入 Tx 回滚。Bridge 看不到失败 Event、不会产生 trace，也不能推进 Domain 状态。 |
| F25 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f25_projection_damage_rebuilds_from_unchanged_events`；`tests.test_orchestration_store.DurableRunStoreTests.test_projection_tampering_is_detected_without_corrupting_offline_rebuild` | 人工修改 live Run projection 后 `verify_projections=False`；Event 数和内容不变，`rebuild_projections` 返回与损坏前完全相同的 Run/Node/Attempt。 |
| F26 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f26_unknown_event_schema_requires_migration` | schema_version=99 的首个 Event 使 replay 立即抛 `ReplayIntegrityError(code="unknown_schema")`，错误明确包含 migration，不跳过未知 Event。 |
| F27 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f27_f28_event_id_replay_or_conflict_is_fail_closed` | 相同 event id、payload、projection intent 返回原 Event，Domain Event 总数不变。 |
| F28 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f27_f28_event_id_replay_or_conflict_is_fail_closed` | 相同 event id 但不同 payload 抛 ProjectionConflictError；Event 总数和 projection 不变。 |
| F29 | 满足 | `tests.test_orchestration_web_api.OrchestrationWebApiTests.test_sse_snapshot_then_reconnect_resumes_from_sequence`；`tests.test_orchestration_web_api.OrchestrationWebApiTests.test_slow_sse_consumer_does_not_hold_a_writer_lock` | 首次 SSE 给 snapshot + 单调 sequence；断线期间 Run 可继续 append。重连传 `after_seq` 只返回下一 sequence，不重复旧 Event；SSE 路径只读 Store，不进入执行 driver。 |
| F30 | 满足 | `tests.test_orchestration_fault_matrix.FaultMatrixAcceptanceTests.test_f30_explicit_legacy_checkpoint_import_never_uses_latest_or_receipts` | importer 只接受调用者显式选择且 payload id 匹配的 checkpoint，拒绝 `latest`。创建新的 `recovery_mode=legacy_prompt` Run，resume prompt 只存敏感 Artifact；SQLite/WAL/SHM 不含 task/query/tool 摘要原文，不创建 Attempt、idempotency 或 verified ToolReceipt。重复导入幂等返回同一 Run。 |

## 跨场景组合不变量

F01–F30 覆盖故障窗口；以下证据额外验证模块组合时不能绕过的边界：

| 不变量 | 证据 | 结论 |
|---|---|---|
| 全局 Workflow identity | `tests.test_orchestration_store.DurableRunStoreTests.test_create_run_atomically_locks_workflow_version_and_later_fills_ref`、`test_concurrent_direct_run_creation_has_one_workflow_digest_winner`、`test_child_creation_cannot_bypass_global_workflow_binding` | 同一 `(workflow_id, version)` 只有一个 digest；direct/child Run 都在创建事务中绑定，Artifact ref 只能补齐不能替换。 |
| 确定性 Artifact 数据流 | `tests.test_orchestration_dataflow.SchedulerDataflowTests.test_end_to_end_join_and_claim_inputs_are_deterministic`、`test_request_hash_binds_resolved_artifact_identity`、`test_missing_upstream_artifact_fails_closed` | input mapping 的完整 Artifact identity 进入请求 hash；join 与字段声明顺序、分支完成顺序无关，缺失/损坏输入在 Attempt schedule 前失败。 |
| 子工作流定义自主恢复 | `tests.test_orchestration_hierarchy.DurableHierarchyTests.test_subworkflow_restart_and_duplicate_reconcile_are_idempotent` | 重启后即使内存 registry 为空，也只从已绑定的不可变定义 Artifact 恢复，同一 child 不重复创建。 |
| 控制面不可挂载给 Activity | `tests.test_orchestration_executor.TrustedActivityExecutorTests.test_agent_scope_cannot_overlap_control_plane_storage`；`tests.test_orchestration_demo.DurableOrchestrationDemoTests.test_runtime_layout_rejects_control_plane_symlink_escape` | Store/Artifact 与 Sandbox root 相等、祖先/后代重叠或 symlink 逃逸均在 backend 前 fail closed。 |
| Run 精确驱动与公开组合面 | `tests.test_orchestration_runtime.OrchestrationRuntimeTests.test_executor_factory_and_driver_are_bound_to_requested_run`、`test_recovery_driver_is_bound_to_requested_run`；`tests.test_orchestration_public_api.OrchestrationPublicApiTests.test_public_surface_is_unique_and_covers_composition_entries`；`tests.test_orchestration_demo.DurableOrchestrationDemoTests.test_minimal_runtime_composition_completes_with_disjoint_roots` | executor factory 与 execution/recovery driver 绑定调用方指定的 Run；顶层 API 可组合且不隐式加载前端，最小示例完成并通过 replay。 |

## 当前验收结论

- 满足：30 / 30
- 部分满足：0 / 30
- 未满足：0 / 30

当前 F01–F30 均有直接可执行证据；这只表示本矩阵的本地持久化、恢复、审计与进程监管边界
已满足，不等价于 remote worker/A2A、任意代码强隔离或外部系统 exactly-once。

F30 的 `source_checkpoint_digest` 只用于绑定“导入的是哪个显式 payload”。它不是签名、授权、
外部副作用证明或精确恢复证明；导入 Run 明确标记 `exact_recovery=false`，恢复事实仍来自新 Run
自己的 Event History。

F12 的“不泄露”边界专指拒绝路径不把未受信任的 worker/token/request/result 原文或其
可离线枚举哈希新增到审计 Event；测试使用此前从未持久化的 return-path canary 扫描
SQLite 主文件、WAL 和 SHM。SQLite 本身是受信任控制面存储，合法 claim 的 canonical
projection/idempotency state 可能已经包含当前或历史 lease 身份；本验收不声称整库从未
保存过合法 claim credential。
