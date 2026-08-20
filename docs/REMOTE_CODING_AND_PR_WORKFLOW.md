# 采集仓库异机编码与 PR 工作流（Sol + DeepSeek）

**Plan-Version:** v2
**任务标识:** `sol-deepseek-pr-workflow`
**完整流程来源:** FIT Issue #18 与
https://github.com/peterwang122/FIT/blob/main/docs/REMOTE_CODING_AND_PR_WORKFLOW.md

## 1. Windows 边界

Windows 开发机只用于编辑 akshareProkect 代码、维护功能分支、运行 Mock/纯
单元测试和静态检查、提交中文 Draft PR。它不启动 akshareProkect、stock-temp、
数据库、Celery/Beat、正式采集任务或完整测试环境，也不连接任何生产资源。

因此不再建设 `AK_RUNTIME_PROFILE=lan-test`、测试数据库守卫、采集白名单或
测试采集服务。

## 2. 角色

- Sol（planner/reviewer）：审核固定 head SHA、跨入口 collector key、目标表、
  来源发布时间、无未来数据、幂等、缺失处理、敏感文件、测试和部署顺序。
- DeepSeek（implementer）：只能按已确认 Issue 施工；不得自行改变来源、
  发布时间、日期映射、单位、成功条件、写库范围或回补范围。

## 3. 标准流程

与 FIT 完整流程一致（10 步），采集侧补充：

- 跨仓库使用同一任务标识及相互引用的 Issue/PR。
- 默认先合并向后兼容的采集端，再合并 FIT 调用端。
- 合并不自动触发正式采集、回补或服务重启。

## 4. 采集侧审核要求

- collector_key 与所有入口（CLI / HTTP / 任务中心）统一。
- 官方来源、发布时间、备用源、目标日期与实际来源日期映射明确。
- 直接/间接写入表、看板重算、缓存影响明确。
- 幂等、重试、缺失处理、历史回补范围明确。
- 无未来数据：滚动百分位、指标训练和策略回放不得读取当日之后的数据。

## 5. 验证分工

- Windows（DeepSeek）：Mock/fixture 单元测试、compile、lint、`git diff --check`
  等不访问真实服务的检查。
- 本机 Sol：真实最小请求、目标表写入、任务中心、历史回补、服务健康和实际
  发布日期核验，在独立 worktree/临时检出中完成。

## 6. Issue 与 PR 模板

- Issue 模板：`.github/ISSUE_TEMPLATE/sol-collection-implementation-plan.md`
- PR 模板：`.github/PULL_REQUEST_TEMPLATE.md`

## 7. 合并与部署

- 使用普通 Merge；合并不等于部署，也不自动触发正式采集、回补或服务重启。

## 8. 相关链接

- FIT 完整流程：https://github.com/peterwang122/FIT/blob/main/docs/REMOTE_CODING_AND_PR_WORKFLOW.md
- 根目录 `AGENTS.md`
