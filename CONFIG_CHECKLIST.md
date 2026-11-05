# 配置验收表

| 配置 | 状态 | 说明 |
|------|------|------|
| **Telegram** | 已配置（禁用） | `config/config.local.json` 中 `crawl.telegram.enable: false`，`channels: {}`。无占位符，已按要求配置。 |
| **GH_TOKEN 本地** | 缺失 | 需要在本地环境变量中设置 `GH_TOKEN`（GitHub Personal Access Token，权限：`public_repo`）。未设置将不影响本地运行，但 GitHub Actions 阶段将因缺失而失效。 |
| **GH_TOKEN Actions** | 已配置 | GitHub Actions 工作流引用了 `secrets.GH_TOKEN`。需要在仓库 Settings → Secrets → Actions 中创建同名 Secret。 |
| **TARGET_REPO** | 缺失 | 需要在 `.env.example` 中填写 `OWNER/REPO` 格式（例如 `myuser/mihomo-subscriptions`），并同步到 GitHub Settings → Secrets → Actions。 |
| **TARGET_REPO_TOKEN** | 缺失 | 需要在 GitHub Settings → Secrets → Actions 中创建具备 `Contents: Read and write` 权限的 Personal Access Token，并填入 `.env.example` 的 `TARGET_REPO_TOKEN`。 |
| **Pages** | 已禁用 | `config/config.local.json` 中 `crawl.pages: []`，保持默认禁用。无真实网址时无需新建爬虫。 |
| **Actions 定时** | 已配置 | 工作流 `schedule: - cron: "0 2 * * *"`（每天 UTC 02:00 = 北京时间 10:00 自动执行）。 |
| **Actions 手动触发** | 已配置 | 工作流 `workflow_dispatch` 支持手动触发，可在工作流页面点击 "Run workflow"。 |
| **subs-check 下载** | 已配置 | 工作流自动下载 subs-check v1.6.5 二进制（x86_64/aarch64 架构检测 + tar.gz 解压 + chmod +x），已修正之前的架构映射错误。 |
| **私人仓库同步** | 未验证 | 需要配置 `TARGET_REPO` 和 `TARGET_REPO_TOKEN` 后方可验证。同步逻辑已就绪：检查有效输出 → 仅在节点数>0 时提交 → 推送到目标分支。 |

## 需要您手动填写的内容

| 项目 | 位置 | 格式/要求 | 备注 |
|------|------|----------|------|
| GH_TOKEN | 本地环境变量或 GitHub Settings | 字符串，权限 `public_repo` | 本地运行可不设，Actions 需要 |
| TARGET_REPO | `.env.example` 第 36 行 | `OWNER/REPO`（例如 `myuser/mihomo-subscriptions`） | 必须是您拥有的私人仓库 |
| TARGET_REPO_TOKEN | GitHub Settings → Secrets → Actions | Token 权限：`Contents: Read and write` | 必须是针对目标仓库的专用 Token |

**配置完成后**：
1. 运行 `cp .env.example .env` 并填入上述三项
2. 在 GitHub 仓库 Settings → Secrets → Actions 中分别创建 `GH_TOKEN` 和 `TARGET_REPO_TOKEN`
3. 确认 `config/config.local.json` 中 `crawl.telegram.enable: false` 保持禁用状态
4. 确认 `config/subs-check.local.yaml` 中 `sub-urls: []`（已修复，避免双重检测）
5. 确认 `.gitignore` 已包含 `output/` 与二进制文件忽略规则（已修复）

**已修复的问题**（无需用户干预）：
1. **工作流 Validate Config**：不再强制 Telegram 必须启用，仅验证 GitHub 启用且 Telegram 无占位符
2. **subs-check 二进制下载**：修正了架构映射（x86_64 → `subs-check_Linux_x86_64.tar.gz` 等），移除了错误的 `linux-amd`/`linux-arm64` 映射
3. **.gitignore**：新增了 `output/`、二进制文件与运行报告的忽略规则，防止真实检测数据和二进制文件被误提交

**不再需要修改的内容**：
- `README.md` 内容已更新为真实运行报告
- `config/config.local.json` 包含 7 个真实订阅源，已验证可下载
- `config/subs_check_bridge.py` 已验证可从 aggregator000 输出生成 subs-check 输入
- `output/` 目录包含真实检测结果（2 个通过节点，完整统计）
- `RUN_REPORT.md` 包含完整运行数据

**暂不提交 git commit / push**，所有修改保留在 `dev/mihomo-aggregator` 分支的工作目录中。