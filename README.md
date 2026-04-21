# BTC 5m — 快速下单工具 & 自动化策略框架

> **Author:** 岳来岳会赚 &nbsp;|&nbsp; **X:** [@188888_x](https://x.com/188888_x)

一个带辅助线的 Polymarket BTC 5 分钟涨跌市场快速下单工具。支持纯本地运行快捷游玩，也可部署到云服务器实现低延迟自动化交易。

## 对比官网的优势

| | Polymarket 官网 | 本工具 |
|---|---|---|
| 下单流程 | 每次需要钱包签名，容易错过时机 | 首次签名后自动缓存凭证，一键下单 |
| 价格参考 | 仅 Chainlink 预言机价格 | 同时显示 Binance 实时价格，反应更快 |
| 数据可视化 | 仅概率数字 | 概率曲线 + Binance 价格曲线 + 差价对比，更直观 |
| 自动交易 | 无 | 模块化策略框架，支持自定义策略全自动执行 |
| 部署方式 | 仅浏览器 | 本地运行 / 云服务器部署，7×24 无人值守 |

> **注意**：内置策略仅作为框架示例，无法保证稳定盈利。建议根据自身分析自行开发和调整策略。
<img width="1351" height="816" alt="截屏2026-04-21 22 30 59_副本" src="https://github.com/user-attachments/assets/b3f32792-e574-44f6-b299-f5ef86722a26" />
## 功能特点

### 快速下单
- 首次私钥签名后 API 凭证自动缓存，后续无需重复签名
- FOK（全部成交或取消）市价单，避免挂单风险
- 前端面板一键买涨/买跌，支持滑点设置

### 实时数据
- 四路 WebSocket 并行：Polymarket 盘口、Chainlink 预言机、用户成交、Binance 实时价
- Binance 价格比 Chainlink 更快响应 BTC 波动，提供领先信号
- 价格偏移自动校准（MAD 异常值过滤 + 修剪均值）
- 每 5 分钟自动切换市场窗口

### 策略框架
- `strategies/` 目录下每个策略独立一个文件，实现统一接口
- 新增策略：新建文件 → 注册 → 重启，前端自动显示
- 策略参数带注释，前端 hover 描述自动生成
- 内置 5 套示例策略，可独立开关、设定金额
- 支持单局多次入场（次数可配置，持久化保存）
- 交易记录自动保存，含入场/出场原因和盈亏明细

### 前端面板（双模式）
- **Full 模式** — 完整仪表盘：盘口深度、概率走势曲线、Binance 价格曲线、手动下单面板（方向/金额/滑点）、策略控制、交易记录，适合手动交易和盯盘
- **Low 模式** — 自动策略专用面板：精简为核心数据（概率/差价/倒计时）+ 策略状态 + 交易记录，低带宽消耗，适合挂机运行和移动端查看
- 两种模式均可实时切换策略开关、金额、单局次数
- 到期仓位自动 Claim

### 数据收集与回测
- 前端一键开启回测数据收集
- 每秒记录差价、概率、剩余时间等关键指标
- 配套 Python 分析脚本，支持参数遍历优化（多核并行）

## 快速开始

### 环境要求

- Node.js 20+

### 安装

```bash
npm install
```

### 配置

```bash
cp .env.example .env
```

必填：
- `POLYMARKET_PRIVATE_KEY` — Polygon 私钥（首次运行自动生成 API 凭证）
- `POLYMARKET_PROXY_ADDRESS` — Polymarket 代理钱包地址

可选：
- `APP_MODE` — `full`（带面板）或 `headless`（纯 API）
- `STRATEGY_S{1..5}_ENABLED` / `STRATEGY_S{1..5}_AMOUNT` — 策略开关和金额
- `ORDER_DEFAULT_SLIPPAGE` — 默认滑点
- `AUTO_CLAIM_ENABLED` — 自动领取到期仓位

### 启动

```bash
# macOS / Linux
./start.sh

# Windows
start.bat

# 或
npm start
```

启动后访问 http://localhost:3456

## 项目结构

```
├── server.ts          # 后端服务（Express + WebSocket，端口 3456）
├── index.html         # 前端面板
├── strategies/        # 策略模块
│   ├── types.ts       # 共享类型和 IStrategy 接口
│   ├── registry.ts    # 策略注册表
│   ├── s1.ts          # 策略1 · 常规加强
│   ├── s2.ts          # 策略2 · 常规
│   ├── s3.ts          # 策略3 · 扫尾
│   ├── s4.ts          # 策略4 · 反转
│   └── s5.ts          # 策略5 · 概率追赶
├── backtest-data/     # 回测数据（运行时生成）
├── .env.example       # 环境变量模板
├── start.sh           # macOS/Linux 启动脚本
└── start.bat          # Windows 启动脚本
```

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/state` | 完整状态快照 |
| GET | `/api/strategy/descriptions` | 策略描述 |
| POST | `/api/strategy/config` | 更新策略配置 |
| POST | `/api/order` | 手动下单 |
| POST | `/api/claim` | 领取到期仓位 |
| POST | `/api/backtest/toggle` | 切换回测数据收集 |

## 示例策略

| 编号 | 名称 | 逻辑简述 |
|------|------|---------|
| 策略1 | 常规加强 | diff 穿越入场 + 追踪止损 + 回撤止盈 + 阶梯止盈 |
| 策略2 | 常规 | diff 穿越入场 + 阶梯止盈 + 固定止损 |
| 策略3 | 扫尾 | 窗口尾部大差价入场 |
| 策略4 | 反转 | 窗口末尾概率反转入场 |
| 策略5 | 概率追赶 | diff 穿越时概率偏低入场，等概率追赶后止盈 |

每个策略文件顶部有完整参数和注释。hover 策略名称可查看详细规则。

## 自定义策略

1. 在 `strategies/` 下新建文件，实现 `IStrategy` 接口
2. 在 `types.ts` 中添加新编号
3. 在 `registry.ts` 中注册
4. 重启服务即生效

## 云服务器部署

将项目上传到服务器后，按上述步骤配置启动即可。推荐配合 `screen` 保持后台运行：

# 使用 screen
screen -S btc5m
npm start
# Ctrl+A D 退出 screen
```

通过 SSH 隧道安全访问面板：

```bash
ssh -L 3456:127.0.0.1:3456 用户名@服务器IP
```

然后本地浏览器打开 http://127.0.0.1:3456

## 安全说明

### 私钥与凭证
- 私钥仅存储在本地 `.env` 文件中，不会上传到任何外部服务器
- API 凭证从私钥派生后缓存到 `.polymarket-creds.json`，后续复用
- 这些文件已在 `.gitignore` 中排除

### 网络访问
- 服务默认监听 `localhost:3456`，仅本机可访问
- **不要将端口直接暴露到公网** — 任何能访问该端口的人都可以通过 API 下单
- 远程访问务必使用 SSH 隧道

### 资金安全
- 建议初次使用时用最小金额测试，确认行为符合预期后再调整
- 策略开关和金额可随时通过前端调整
- 内置策略仅为示例，不构成投资建议

### 敏感文件
以下文件包含敏感信息，**请勿分享或提交到公开仓库**：

| 文件 | 内容 |
|------|------|
| `.env` | 私钥和钱包地址 |
| `.polymarket-creds.json` | API 凭证 |
| `.trade-history.json` | 交易记录 |
| `.strategy-config.json` | 持久化配置 |

## 策略共建

工具提供了完整的策略框架和回测能力，但好的策略需要持续迭代。如果你：

- 有更好的入场/出场思路
- 发现了新的数据规律
- 想一起做策略回测和优化
- 或者对 Polymarket BTC 5分钟市场有任何想法

欢迎联系交流，一起做出 1+1 > 2 的效果。

**X:** [@188888_x](https://x.com/188888_x)

## 💝 赞助支持

> 无邀请码，用爱发电 ⚡

如蒙老板们慷慨赞助，可打赏至以下地址（Token费太贵了，入不敷出 😂）：

### 🔗 收款地址

| 链 / Chain | 地址 / Address |
| :--- | :--- |
| **EVM 全链** | `0x0D71D55aF87fb297D4eE7b15acEE37572e34AC91` |
| **Solana (SOL)** | `5JNfS3cyFKkWmoV7KfpYtN7vYE7LnZaGYF2gd8ZTpCUR` |

---

> 🙏 感谢每一位支持者，你们的赞助是持续更新的动力！

## 许可

仅供个人使用和学习研究。使用本工具进行交易的风险由使用者自行承担。

---

<p align="center">Made by <b>岳来岳会赚</b> &nbsp;|&nbsp; <a href="https://x.com/188888_x">@188888_x</a></p>
