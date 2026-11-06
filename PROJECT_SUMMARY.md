# aggregator0000 项目摘要

## 目标
把 beck-8/subs-check 整合进 aggregator0000，实现：拉取 -> 检测可用节点 -> 只保留 vless/hysteria2/mieru/masque。

## 铁律
- 拉取、上传全程用代理 http://192.168.31.204:7891
- 网络搜索用 SearXNG http://192.168.31.204:3332/ (局域网直连不走代理)
- 用户只验收，不参与

## 关键仓库
- 生产仓库: https://github.com/pq0000/aggregator0000
- 本地: F:\pq0000aggregator000
- subs-check: https://github.com/beck-8/subs-check
- 配置 Gist: d54a6bd8bfd304c45888d7a3cd168d1b
- 输出 Gist: 942e3c9de1396e6b3bf4afab0f054349

## 注意事项
- GitHub Actions 里不能用 gh CLI (未认证)，要用 GitHub API + token
- Gist API content 超 1MB 会截断，用 raw URL 下载完整文件
- filter_nodes.py 从 raw URL 读 all.yaml，过滤 vless/hysteria2/mieru/masque
- 协议筛选是核心需求，需确保 filter_nodes.py 正确工作
