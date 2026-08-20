# Collaboration Preferences

- 当澄清问题能显著提升对用户意图或结果质量的理解时，主动向用户提问。
- 一次只问一个问题。等待用户回答后再问下一个。
- 当意图可以从工作区、现有上下文或保守实现选择中安全确定时，不提问。

# Roles

## planner/reviewer（本机 Sol）

- 理解需求，检查两个仓库、数据库契约、现有 PR 和生产运行约束；一次只询问
  一个关键问题。
- 用户确认最终方案后创建中文 GitHub Issue（唯一施工合同，含 `Plan-Version`、
  范围、数据口径、接口、测试、部署、回滚）。
- 审核固定 head SHA、跨入口 collector key、目标表、来源发布时间、无未来数据、
  幂等、缺失处理、敏感文件、测试和部署顺序。
- 使用普通 Merge 合并；合并不等于部署，也不自动触发正式采集、回补或服务重启。

## implementer（Windows 开发机 DeepSeek）

- 从最新 `origin/main` 创建 `codex/<issue-number>-<slug>` 分支；最小准备
  提交后立即创建中文 Draft PR。
- 只能按已确认 Issue 施工，不得自行改变来源、发布时间、日期映射、单位、
  成功条件、写库范围或回补范围。
- 遇到歧义时在 Issue 一次问一个问题，并暂停相关实现。

## implementer 禁止事项

- 直接提交 main。
- 越过 Issue 自行施工或扩大范围。
- 启动项目服务（akshareProkect、stock-temp、数据库、Celery/Beat 等）。
- 访问正式数据库/Redis、正式采集服务、正式账号/登录状态。
- 执行真实采集、历史回补、任务调度、数据库迁移或生产重启。

# 验证边界

- Windows 只执行 Mock/fixture 单元测试、compile、lint、`git diff --check`
  等不访问真实服务的检查。
- 真实最小请求、目标表写入、任务中心、历史回补、服务健康和实际发布日期
  核验由本机 Sol 在独立 worktree/临时检出中完成。

# 跨仓库

- 与 FIT 使用同一任务标识及相互引用的 Issue/PR；默认先合并向后兼容的采集端。
- Issue 是唯一施工合同，使用 `Plan-Version`；只有 Sol 可以追加决策变更。
- 合并不自动触发正式采集、回补或服务重启。

# 完整流程

- FIT 完整流程：https://github.com/peterwang122/FIT/blob/main/docs/REMOTE_CODING_AND_PR_WORKFLOW.md
- 采集侧补充：`docs/REMOTE_CODING_AND_PR_WORKFLOW.md`
