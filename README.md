<div align="center">

# 🌐 ProxyHarvest

**全网免费代理抓取 · 真实流量验证 · 中文结构化输出**

一个零依赖的 Python 命令行工具，自动从 40+ 公开数据源抓取免费代理，
通过真实网页加载测试筛选出**真正可用**的高速代理。

[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Zero Deps](https://img.shields.io/badge/Dependencies-0-orange.svg)](#)

</div>

---

## ✨ 核心特性

| 特性 | 说明 |
|------|------|
| 🔍 **多源抓取** | 40+ 数据源（GitHub 仓库 + 直连 API），GitHub 自动走国内镜像 |
| ✅ **真实验证** | 不只测 TCP 握手，而是用代理**真正打开网页**验证可用性 |
| 🎯 **三步检测** | 出口 IP 验证 → 真实网页测速 → 匿名泄露检测 |
| 🌍 **中文输出** | 国家 / 城市 / 运营商自动翻译为中文 |
| 📊 **结构化导出** | TXT（标准格式）+ CSV（18 字段详情表）+ Top20 推荐 |
| 🚀 **一键运行** | 双击 `start_proxy.bat` 即可，无需任何参数 |
| ⏰ **定时更新** | 内置 Windows 计划任务脚本，每天自动更新 |
| 📦 **零依赖** | 纯 Python 标准库，无需 `pip install` |

---

## 🚀 快速开始

### 方式一：双击运行（推荐）

```
双击 start_proxy.bat
```

### 方式二：命令行

```bash
python proxy_scraper.py
```

运行完成后自动生成三个文件：

| 文件 | 说明 |
|------|------|
| `proxies_top20.txt` | ⭐ 最快的 20 个推荐代理（直接用这个） |
| `proxies.txt` | 全部可用代理（`protocol://ip:port` 格式） |
| `proxies.csv` | 完整信息表（可用 Excel 打开筛选） |

---

## 🎯 按需筛选

```bash
# 只要中国的 SOCKS5 代理，保留最快 20 个
python proxy_scraper.py --country 中国 --protocol socks5 --top 20

# 只要高匿代理
python proxy_scraper.py --anonymous-only

# 中国的高匿代理，延迟不超过 2 秒
python proxy_scraper.py --country 中国 --anonymous-only --max-latency 2

# 快速测试（只验证 3000 个）
python proxy_scraper.py --limit 3000
```

### 全部参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--protocol` | `all` | 协议类型：`http` / `socks4` / `socks5` / `all` |
| `--country` | - | 按国家筛选（中文），如 `中国,美国` |
| `--anonymous-only` | - | 只保留高匿代理 |
| `--top N` | `0` | 只保留最快的 N 个 |
| `--workers` | `80` | 并发验证线程数 |
| `--timeout` | `3` | 单个代理超时（秒） |
| `--max-latency` | `3` | 最大延迟过滤（秒），`0`=不限制 |
| `--limit` | `0` | 最多验证 N 个后停止 |
| `--no-verify` | - | 跳过验证，只抓取 |
| `--no-location` | - | 跳过 IP 归属地查询 |
| `-o` | `proxies` | 输出文件前缀 |

---

## 📊 CSV 字段说明

| 列名 | 说明 |
|------|------|
| 代理地址 | `socks5://1.2.3.4:1080` 格式 |
| 协议 | HTTP代理 / SOCKS4代理 / SOCKS5代理 |
| 出口IP | 通过代理访问网站时的出口 IP |
| 国家 / 城市 / 运营商ISP | 中文字段，可直接筛选 |
| **真实延迟(秒)** | 访问百度的真实加载速度 |
| 延迟等级 | 极速 / 快速 / 一般 / 较慢 |
| 网页打开 | 正常 / 失败 |
| 匿名类型 | 高匿 / 普匿 / 透明 |
| 状态 | 可用 / 仅通道 |

---

## 🔍 验证原理

```
┌─────────────────────────────────────────────┐
│  第一步：出口 IP 验证                        │
│  通过代理请求 api.ip.sb/ip                   │
│  → 确认代理通道畅通，获取出口 IP             │
├─────────────────────────────────────────────┤
│  第二步：真实网页测速                        │
│  用代理访问百度（www.baidu.com）              │
│  → 测量真实加载速度，验证能否打开网页        │
├─────────────────────────────────────────────┤
│  第三步：匿名泄露检测                        │
│  检查是否泄露 Via / X-Forwarded-For 等头    │
│  → 判定高匿 / 普匿 / 透明                    │
└─────────────────────────────────────────────┘
```

### 匿名类型

| 类型 | 含义 | 安全性 |
|------|------|--------|
| **高匿** | 出口 IP ≠ 代理 IP，不泄露任何客户端信息 | ⭐⭐⭐ |
| **普匿** | 出口 IP ≠ 代理 IP，但泄露了代理标识 | ⭐⭐ |
| **透明** | 出口 IP = 代理 IP，直接暴露你的真实 IP | ⚠️ |

---

## ⏰ 定时自动更新

```bash
# 一键设置每天 9:00 自动更新（需管理员权限）
setup_schedule.bat
```

管理命令：
```bash
schtasks /query /tn "DailyProxyUpdate"    # 查看
schtasks /run /tn "DailyProxyUpdate"     # 手动运行
schtasks /delete /tn "DailyProxyUpdate"  # 删除
```

---

## 📁 项目结构

```
proxy-harvest/
├── proxy_scraper.py      # 主程序（零依赖）
├── start_proxy.bat       # 双击一键运行
├── setup_schedule.bat    # 设置定时任务
├── README.md             # 说明文档
└── .gitignore
```

---

## ❓ 常见问题

**Q: 运行很慢？**
> 验证阶段需要测试每个代理的真实可用性，几千个代理需要 5-15 分钟。
> 可以用 `--limit 3000` 限制数量，或 `--workers 150` 提高并发。

**Q: 结果文件被占用？**
> 如果 Excel 打开着 `proxies.csv`，程序会自动写入带时间戳的新文件。

**Q: 某些数据源显示失败？**
> 正常现象。部分 GitHub 源在国内访问不稳定，程序会自动切换镜像并跳过失败的源。

**Q: 找不到代理？**
> 尝试放宽延迟限制：`--max-latency 5` 或 `--max-latency 0`（不限制）。

---

## ⚠️ 免责声明

本工具仅供学习、研究和合法用途使用。使用者应遵守当地法律法规，
不得将代理用于任何违法活动。因使用本工具产生的任何后果由使用者自行承担。

---

## 📄 License

[MIT](LICENSE)

<div align="center">

如果这个项目对你有帮助，请给个 ⭐ Star 支持一下！

</div>
