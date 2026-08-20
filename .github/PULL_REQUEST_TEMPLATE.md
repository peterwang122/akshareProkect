## 关联 Issue / Plan-Version

<#issue-number>（Plan-Version: vN）

## 最终 head SHA

<填入 git rev-parse HEAD>

## 实际改动

（按模块分组；若只含协作规范文件，明确说明）

## collector key 与所有入口

（CLI / HTTP / 任务中心；无则写“无”）

## 数据来源、发布时间、日期映射

（官方来源、发布时间、目标日期与实际来源日期；无则写“无”）

## 数据库表 / 网络 / 通知影响

（读取表 / 写入表 / 看板重算 / 缓存 / 外部请求；无则写“无”）

## Windows 已执行的安全测试及结果

（Mock/fixture 测试、compile、lint、git diff --check 等）

## 必须由本机 Sol 执行的集成验证清单

（真实请求、写库、回补、服务健康、发布日期核验）

## 部署顺序与回滚
