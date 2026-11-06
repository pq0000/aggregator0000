# aggregator0000 部署说明

本仓库是 fork 自 [wzdnzd/aggregator](https://github.com/wzdnzd/aggregator) 的代理节点采集中心，通过 GitHub Actions 定时爬取 Telegram 频道 + GitHub 源，检测后生成 Mihomo 订阅并推送到私有 Gist。

## 配置结构

配置保存在 **Gist**，通过仓库 Secret SUBSCRIBE_CONF 指向其 raw URL，GitHub Actions 每次运行从该 URL 拉取最新配置。因此修改配置**无需提交代码**，只需更新 Gist 内容。

## 两个 Gist

| 用途 | Gist ID | 说明 |
|------|---------|------|
| 配置 | d54a6bd8bfd304c45888d7a3cd168d1b | 订阅源、Telegram 频道、storage 设置 |
| 输出 | 942e3c9de1396e6b3bf4afab0f054349 | 采集结果（all.yaml 等） |

## 最终订阅地址（导入 Mihomo）

`
https://gist.githubusercontent.com/pq0000/942e3c9de1396e6b3bf4afab0f054349/raw/all.yaml
`

## 仓库 Secrets

| Secret | 用途 | 权限 |
|--------|------|------|
| GH_TOKEN | GitHub 爬虫 API 搜索 | public_repo |
| GIST_PAT | Gist 操作 | gist |
| PUSH_TOKEN | 推送结果到 Gist | gist |
| SUBSCRIBE_CONF | 配置 Gist 的 raw URL | 无需 |

## 仓库 Variables

| Variable | 值 | 说明 |
|----------|-----|------|
| SKIP_ALIVE_CHECK | false | 执行 Clash 活性检查 |
| SKIP_REMARK | false | 更新订阅备注 |
| WORKFLOW_MODE | 0 | 爬取+聚合+检测 |
| ENABLE_SPECIAL_PROTOCOLS | true | 启用 VLESS/Hysteria2 等 |

## 配置的订阅源

1. **Telegram 频道**：34 个（27 启用），采集公开频道的订阅链接和节点
2. **GitHub API 搜索**：搜索公开仓库/Issues/代码中的代理订阅
3. **GitHub raw 源**：6 个验证过的公开订阅仓库

## 修改订阅源

编辑配置 Gist（d54a6bd8bfd304c45888d7a3cd168d1b）中的 sites 和 crawl.telegram.channels，保存后下次 Actions 自动生效。

## 手动触发

仓库 → Actions → Process → Run workflow

## 定时运行

每天 UTC 03:05 和 11:05 自动执行。
