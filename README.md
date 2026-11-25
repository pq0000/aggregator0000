# aggregator0000：采集、检测与可信订阅发布

本仓库是 Python 节点聚合器与 [subs-check](https://github.com/beck-8/subs-check) 的组合，不是 subs-check 的 Go 源码仓库。正式采集和节点检测在 **GitHub Actions** 运行；本地默认只进行少量合成离线回归。

## 工作流程

```text
已有订阅配置 + 经核实的公开来源
  → 抓取 / 保真解析 / 完整连接参数去重 / 合并输出路由
  → 本轮原始候选与脱敏采集报告
  → 协议筛选与本轮隔离传递
  → HTTPS 测活、既有质量筛选与测速
  → 不同 HTTPS 目标复检
  → 完整性、时效和本轮身份校验
  → 仅发布确认通过的成品到既有 Gist
```

- 当前成品范围：**VLESS、Hysteria2、Mieru、MASQUE**。支持范围必须与检测器能力保持一致；提取出一个协议的 URI 不等于下游已经能完整解码该 URI。
- 原生 Mihomo YAML 直接解析、直接导出，不经过可能丢失现代字段的订阅转换。散装 URI 保留原始密码、Base64、路径和查询参数的大小写。
- 去重比较完整连接配置，保留同端点但不同认证、传输、TLS 或 REALITY 设置的候选。检测器内部仍存在粗粒度去重时，冲突候选必须明确隔离、统计，不能假称已经全部检测通过。
- 订阅 URL 去重合并输出分组。缺省来源路由继承全局设置，显式空路由不被偷偷重新启用。
- 采集阶段禁止旧存储适配器或插件通过常规推送接口提前覆写远端订阅。本轮候选与已测成品分离；失败、空轮、超时或旧文件不能冒充本轮完成。
- GitHub 上的两阶段输入按本次运行身份传递。公开仓库的中间产物必须加密，明文配置、日志、节点快照不得作为公开诊断附件上传。

> 本次改动的 GitHub 验证状态请以 PR 和具体 Actions 运行结果为准。静态检查或离线回归通过，不表示真实节点已通过网络检测。

## GitHub Actions 使用

工作流定义在 [Mihomo Pipeline](<.github/workflows/mihomo-pipeline.yaml>)。

1. 在仓库的 **Settings → Secrets and variables → Actions** 检查已有 Secrets，不要把其内容贴在 Issue、PR、日志或源码中。
2. 确认目标 Gist ID 与当前账号匹配。其他 fork 需要修改工作流中的目标配置，不能继续覆盖本仓库维护者的 Gist。
3. 专用改进分支支持正式 push 验证；正式流程也可手动运行。PR 流程只执行离线回归，不为外部 PR 提供 Secrets。
4. 正式流程必须 checkout 被运行的提交，而不是硬编码其他分支。
5. 查看本轮完成状态和脱敏统计报告，确认不是只有测试步骤通过而采集、检测或发布被跳过。

| 配置 | 用途与注意事项 |
| --- | --- |
| `SUBSCRIBE_CONF` | 聚合配置：JSON 文档、配置文件路径或 HTTPS 配置地址。正式流程使用已有 Secret；[默认示例](<subscribe/examples/config.default.json>)仅是模板，不能据此推断线上来源。 |
| `GH_TOKEN` | GitHub 来源检索所需的读取凭据；按实际检索功能授予最小权限。 |
| `PUSH_TOKEN` | 既有 Gist 发布凭据，需对目标 Gist 有写入权限；也是中间产物加密所用的已有凭据之一，不需要将密钥另行公开。 |
| `GIST_ID` | 当前成品目标；迁移 fork 时按自己的 Gist 修改。 |

定时表达式按 UTC 解释；原有 `0 0 * * *` / `0 12 * * *` 对应北京时间 **08:00 / 20:00**。GitHub 调度可能延迟，fork 的定时工作流也可能需要在 Actions 中手动启用。

## 公开增量来源

[公共来源清单](<subscribe/examples/public-sources.json>)是独立的、可审查的增量配置，不替代 Secret 中已有订阅。

当前只启用核实过真实路径、近期更新和 VLESS 内容索引的 `roosterkid/openproxylist` RAW 来源。不重复导入它的 Base64 镜像，也不把 403/404、身份不明的仓库伪装成有效新源。

- 注册项须有唯一安全 ID、`enabled` 状态和无凭据的 GitHub HTTPS raw 地址。
- 不配置 `push_to` 时路由到现有输出组；如明确配置，必须指向有效输出组。
- 来源贡献按每轮解析数、唯一数、重叠数和进入候选池的数量统计。不把 HTTP 200 或上游“已检查”声明视为本项目通过凭据。
- 核实日期只是核实记录，不是永久可用保证。根据连续运行证据维护来源，不承诺无限增加有效免费节点。
- 新增来源前确认其公开发布意图；不要扫描未授权服务、批量注册账号或收集泄露凭据。

## 本地：仅少量离线回归

```bash
python3 -m venv .venv
.venv/bin/python -m pip install PyYAML tqdm geoip2 pycryptodomex
.venv/bin/python -m unittest discover -s tests -v
bash -n run_subs_check.sh
```

测试使用合成节点、临时目录和 mock 网络入口，不依赖真实订阅或真实代理。**不要为本地验证启动全量聚合、来源抓取或全量测速。**

正式 CI 依赖仍以 [requirements](<requirements.txt>)为准；上述本地安装仅包含回归需要的轻量依赖。

## 聚合命令接口

以下是正式 runner 使用的接口说明，**不是建议在本地执行全量流程**：

```bash
python -u subscribe/process.py \
  --overwrite --special-protocols --skip-alive-check --skip-remark \
  --output-dir output/raw --skip-push \
  --sources-file subscribe/examples/public-sources.json
```

- `--output-dir`：写本轮保真 Mihomo 合并输入、各组输入和脱敏采集报告。组名不会直接成为文件路径。
- `--skip-push`：跳过采集阶段远端发布与配置刷新；仍可读取已有来源缓存。必须同时指定输出目录。
- `--sources-file`：选用版本化公开增量来源；省略时不自动增加新来源。
- `--overwrite`：本轮不自动混入上一轮成品；如要复用历史候选，应重新验证并明确记录来源与时效。
- `--skip-alive-check`：仅跳过聚合器的重复基础检测，后续 subs-check 检测与复检不能跳过。

[运行包装器](<run_subs_check.sh>)的第二个参数为每阶段超时秒数。它负责本轮隔离、服务就绪、仅清理自己的子进程，并将异常终止与成功完成区别处理。

## 如何判断“真正跑通”

至少同时满足：

1. Actions 运行对应预期分支和提交 SHA。
2. 有本轮采集、解析、去重和协议筛选统计，而不是读取旧 Gist 代替采集输入。
3. 检测快照、完成计数和产物属于同一轮，并满足配置的主检测与复检条件。
4. 成品非空、结构和协议正确、认证与传输字段完整，并且只包含已确认通过的节点。
5. 发布步骤实际成功；失败不覆盖上一版成品。
6. 公开报告只包含时间、SHA、运行地址、匿名来源贡献和失败分类，不含订阅 token、节点密码、服务器地址、原始日志或快照。

**GitHub runner 测通不等于你的本地网络能用。** 客户端内核版本、IPv6/UDP、运营商路径和区域封锁都会产生差异。实际可用率必须说明测试时间、位置、判定标准和分母；不要用“没有 YAML `alive` 字段”来判定节点死亡。

## 安全与上游

- 真实配置和候选/检测产物不提交到 Git，参考 [.gitignore](<.gitignore>)。不上传未脱敏日志或配置缓存。
- 保持 TLS 校验；不要用关闭证书校验来掩盖网络错误。
- 固定检测器版本并核验下载校验和，升级后重新验证快照、回调、协议和去重行为。
- 聚合器源自 [wzdnzd/aggregator](https://github.com/wzdnzd/aggregator)，检测器使用 [beck-8/subs-check](https://github.com/beck-8/subs-check)，Mihomo 来自 [MetaCubeX/mihomo](https://github.com/MetaCubeX/mihomo)。各自许可和风险说明仍适用。

公开代理不适合传输敏感信息；使用者应遵守来源条款及当地法律法规。
