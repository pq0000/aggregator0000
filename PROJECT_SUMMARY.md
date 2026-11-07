# aggregator0000 项目摘要

## 目标
把 beck-8/subs-check 整合进 aggregator0000，流水线：aggregator0000 拉取 -> subs-check 检测可用节点 -> 只保留 vless/hysteria2/mieru/masque -> 覆盖 Gist all.yaml。

## 铁律
- 拉取、上传全程用代理 http://192.168.31.204:7891
- 网络搜索用 SearXNG http://192.168.31.204:3332/（不走代理）
- 用户只验收，不参与

## 关键仓库/链接
- 生产仓库: https://github.com/pq0000/aggregator0000
- 本地: F:\pq0000aggregator000
- subs-check: https://github.com/beck-8/subs-check
- 配置 Gist: d54a6bd8bfd304c45888d7a3cd168d1b (39 个源)
- 输出 Gist: 942e3c9de1396e6b3bf4afab0f054349
- 用户自维护合集: https://gist.githubusercontent.com/pq0000/7ca4b62214dd23ec6a022ec46da6f344/raw/1.yaml

## 流水线配置
- workflow: .github/workflows/mihomo-pipeline.yaml
- 两 job: aggregate（aggregator0000 拉取）-> subs-check（检测+覆盖 Gist all.yaml）
- subs-check config: subscribe/subs-check-conf/config.yaml
  - node-type: vless/hysteria2/mieru/masque
  - save-method: gist（覆盖输出 Gist all.yaml）
  - min-speed: 300（严格测速）
- callback.sh 检测完成后退出 subs-check

## 定时
- 每天中国 04:50/16:50 触发（cron 50 20/8 * * *）-> 05:00/17:00 交付

## 注意事项
- GitHub Actions 里不能用 gh CLI，用 GitHub API + token
- Gist API content 超 1MB 会截断，用 raw URL 下载
- 免费代理节点质量差，需不断提高测速标准+加高时效源
- 命令输出易被转义污染，用简单 bash 或 PowerShell 数组