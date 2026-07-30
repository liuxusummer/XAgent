# Memory 生命周期对抗审查记录

> 状态：三轮审查通过；未解决 P0/P1 为 0。
>
> 范围：`MemoryStore` schema v2、`memory_propose`、运行时注入、兼容迁移、
> supersession、retention、provenance 和质量指标。

## 第 1 轮：权限、投毒与信息流

按 tenant、ACL、review scope、候选隔离、受众重叠和模型可见数据逐项攻击。

发现并修复：

- P1：只按当前 reviewer 可见记录查重时，`subject:A` 旧记录与 `tenant:T` 新记录会
  对 A 同时可见。现在只要受众有交集而 reviewer 无权审核全部记录，审批就 fail
  closed，且只返回冲突计数，不泄露不可见 record ID。
- P1：`memory_key=null` 会被当成“未提供”并生成默认 key。现在非字符串、控制字符、
  空白和非安全标识符字符均在写盘前拒绝。
- P1：来源、ACL 或 supersession 接受惰性 iterable 时先完整物化，存在内存耗尽面。
  现在消费到第 65 项即停止并拒绝。

验证覆盖：跨 tenant、同 subject 跨 tenant、ACL 交叠、pending tool-untrusted
候选、非法 key、超量 iterable 和受管 Memory 文件旁路。

## 第 2 轮：并发、线性化与状态完整性

按“先 assess、后 approve”的竞态、同时批准冲突候选、陈旧审批、图篡改、原子写失败
和并发清理逐项复攻。

发现并修复：

- P1：若 duplicate/conflict 检查不与写入共锁，两个候选可同时通过。现在 review
  在统一工作区锁内重新计算完整冲突集合，旧 assessment 只是提示，不是 authority。
- P1：陈旧 assessment 可能覆盖其后刚批准的事实。现在 supplied
  `supersede_record_ids` 必须与锁内重新计算的集合精确相等，否则候选保持 pending。
- P1：双向 record 关系校验不足以排除人工构造的 supersession 环。现在加载和写入
  均验证完整图无环。

验证覆盖：并发冲突审批仅一个成功、陈旧审批拒绝、候选—记录双向一致、伪造 successor、
孤立 record、symlink replacement 和写入路径替换。

## 第 3 轮：升级、保留与容量

按 v1 滚动升级、过期/已拒绝/已替代数据清理、跨 ACL 清理、时钟回拨、容量临界和
审计数据泄漏逐项复攻。

发现并修复：

- P1：批量清理若复用 64 KiB canonical metadata digest，会在最需要回收容量时因
  summary 过大失败。现在 purge chain 使用长度分帧的增量 SHA-256，不物化单个巨大
  canonical payload。
- P1：记录同时 expired 和 superseded 时只取 superseded 时间会不必要地延迟清理。
  现在使用最早退休时间。
- P1：删除 successor 但保留无权清理的 predecessor 会产生断链。现在清理集合执行
  反向闭包，任何仍保留 predecessor 所依赖的 successor 都不会被删除。

兼容路径严格校验 v1 的精确字段、枚举、双向关系和 canonical 值，只在内存中映射为
v2；下一次真实 mutation 才原子写回。清理只处理 reviewer 同 tenant 且 ACL 授权的
记录，累计计数有 63-bit 上限，receipt/telemetry 只含 ID、digest、数量和字符成本，
不含记忆正文。

## 最终门禁

- Memory/Provider/Kernel 专项：85/85 通过。
- 全部已跟踪后端测试：1174/1174 通过（Python 3.12）。
- tools schema JSON 校验、`py_compile`、`git diff --check` 通过。
- 注入模式扫描和 credential-shaped diff 扫描无发现。
- Python 3.12 临时工作区基准（200 条）：proposal 1.178s、review 4.797s、
  两次完整读取/指标 0.040s、状态文件 360104 bytes。当前文件存储适合人工审核速率，
  不宣称是高吞吐记忆数据库。
- 未解决 P0/P1：0。
