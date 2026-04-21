# Notion Schema v2 — Company / Deal / Person 三库

当前版本（v1）只有一个 "Signals" 库，所有信号扁平堆在一起。v2 把信号进一步拆成三条时间线：**公司**、**交易**、**人**。

Signals 库保留作为"原始事件流"，每条信号同时挂到它影响的 Company / Deal / Person 上，供纵向回看。

## 三个数据库概览

| 数据库 | 主键 | 粒度 | 增长节奏 |
|--------|------|------|----------|
| **Companies** | 公司名 | 每家 AI 公司一条 | 慢：新增/状态更新 |
| **Deals** | Deal 标题（公司+轮次） | 每个融资轮一条 | 中：每天新增几条 |
| **People** | 人名 | 每个关注的个人一条 | 慢：按需新增 |
| **Signals** (v1) | Signal title | 每条原始事件 | 快：每天数十条 |

## 关系拓扑

```
Signals --many-to-one--> Companies
Signals --many-to-one--> Deals
Signals --many-to-many-> People
Deals   --many-to-one--> Companies
People  --many-to-many-> Companies
```

---

## 1. Companies（公司库）

### 字段

| Property | Type | 说明 |
|----------|------|------|
| Company | title | 公司标准名（英文优先） |
| Aliases | rich_text | 逗号分隔的别名（中文名、曾用名） |
| Geography | select | `CN` / `US` / `EU` / `Other` |
| Category | multi_select | `Foundation Model`, `Agent`, `Infra`, `Vertical AI`, `Open Source`, `Chip`, `Data`, `Tooling` |
| Status | status | `Radar`, `Following`, `Diligence`, `Passed`, `Portfolio`, `Dormant` |
| Priority | select | `P0`, `P1`, `P2` |
| Last Raised | date | 最近一轮融资宣布日期 |
| Last Round | rich_text | `Series B @ $2B post` |
| Lead Investors | rich_text | 最近一轮的 lead |
| One-liner | rich_text | 一句话产品/定位 |
| Founder(s) | relation → People | 创始人/关键高管 |
| Website | url | |
| GitHub | url | 主仓库链接（如开源） |
| Twitter | url | 公司官方账号 |
| Source URL | url | 信息来源链接（首次发现） |
| First Seen | date | 进入 watchlist 的时间 |
| Signal Count | rollup(Signals, count) | 累计相关信号数 |
| Last Signal At | rollup(Signals, latest date) | 最近一条信号日期 |
| Deals | relation → Deals | 该公司所有历史交易 |
| Notes | rich_text | 自由编辑的备注 |

### Views

- **Radar**（Status=Radar，按 Last Signal At 倒序）
- **Following**（Status=Following，按 Priority 升序）
- **CN 当前活跃**（Geography=CN AND Last Signal At 最近 14 天）
- **US 技术前沿**（Geography=US AND Category includes Foundation/Infra）

---

## 2. Deals（交易库）

### 字段

| Property | Type | 说明 |
|----------|------|------|
| Deal | title | `{Company} {Round}` 如 `Moonshot AI Series B` |
| Company | relation → Companies | |
| Round | select | `Pre-Seed`, `Seed`, `Series A`, `Series B`, `Series C+`, `Strategic`, `Acquisition`, `IPO`, `Unknown` |
| Amount | rich_text | `$250M` |
| Post-Money | rich_text | `$3.3B` |
| Announced | date | 宣布日期 |
| Lead | rich_text | lead investor |
| Investors | multi_select | 所有已披露投资方 |
| Geography | rollup from Company | |
| Category | rollup from Company | |
| Confidence | select | `Confirmed`, `Reported`, `Rumored` |
| Priority | select | `Must Chase`, `Monitor`, `Skip` |
| Reason to Chase | rich_text | 为什么值得跟 |
| Status | status | `To Review`, `Outreach Sent`, `Meeting Scheduled`, `Passed`, `Tracking` |
| Source URL | url | |
| Signals | relation → Signals | 该 deal 相关的所有信号 |
| Created At | created_time | |

### Views

- **Today's Must Chase**（Priority=Must Chase AND Announced 本周）
- **CN 活跃**（Geography=CN，按 Announced 倒序）
- **Pipeline**（Status=Outreach Sent/Meeting Scheduled）

---

## 3. People（人库）

### 字段

| Property | Type | 说明 |
|----------|------|------|
| Name | title | 英文主名 |
| Chinese Name | rich_text | 中文名（如适用） |
| Current Role | rich_text | `Co-founder @ xAI` |
| Current Company | relation → Companies | |
| Past Companies | relation → Companies | |
| Focus Area | multi_select | `Foundation Models`, `Reinforcement Learning`, `Agent`, `Infra`, `Robotics`, `Investing`, ... |
| Geography | select | `CN` / `US` / `EU` |
| Priority | select | `P0`, `P1`, `P2` |
| Twitter | url | |
| LinkedIn | url | |
| Homepage | url | |
| Why Track | rich_text | 一句话：为什么关注这个人 |
| Signals | relation → Signals | 该人相关的所有信号 |
| Last Move At | rollup(Signals, latest date for event_type=talent_*) | |
| Created At | created_time | |

### Views

- **Top Researchers**（Priority=P0，按 Last Move At 倒序）
- **Recent Moves**（Last Move At 最近 30 天）

---

## 4. Signals（原有库扩展）

在 v1 基础上增加关联字段：

| Property | Type | 说明 |
|----------|------|------|
| ...（v1 字段保留） | | |
| Company Record | relation → Companies | 自动解析：从 matched_entities 查找 |
| Deal Record | relation → Deals | 如果是融资类型，链到具体 deal |
| People Mentioned | relation → People | 从 key_people 查找 |
| LLM Score | number | 0-10，LLM 个性化打分 |
| LLM TL;DR | rich_text | 一句话摘要 |
| LLM Reason | rich_text | 为什么推给你 |
| Digest Rank | number | 当天 Top-N 的排名（1=最顶） |
| In Digest | checkbox | 是否出现在当日邮件日报里 |

## 建库步骤（你手动操作）

1. 在 Notion 里新建一个 page 叫 "AI Primary Market Watch"
2. 在这个 page 下建三个 database：Companies / Deals / People（按上面字段）
3. 打开已有的 Signals database，添加上面"Signals 扩展"里的字段和关系
4. 从每个 database 的 share 菜单复制 `data_source_id`（URL 里的 UUID）
5. 填到 `config/delivery.json` 的 `notion_database` 配置里：

```json
{
  "id": "notion-market-watch",
  "type": "notion_database",
  "data_source_id": "collection://<signals-id>",
  "auth_token_env": "NOTION_API_TOKEN",
  "upsert": true,
  "relation_targets": [
    {
      "property_key": "Company Record",
      "data_source_id": "<companies-id>",
      "lookup_properties": ["Company", "Aliases"],
      "signal_fields": ["company_name", "matched_entities"]
    },
    {
      "property_key": "Deal Record",
      "data_source_id": "<deals-id>",
      "lookup_properties": ["Deal"],
      "signal_fields": ["cluster_key"]
    },
    {
      "property_key": "People Mentioned",
      "data_source_id": "<people-id>",
      "lookup_properties": ["Name", "Chinese Name"],
      "signal_fields": ["key_people"]
    }
  ]
}
```

6. 邀请你的 Notion integration 到这三个新 database（右上角 `...` → Add connections）

## 迁移策略

- v1 的单 Signals 库继续保留和使用；v2 是**在 Signals 旁边新增** Company / Deal / People，不替换 v1。
- 上线后第一周，只写入 Companies/Deals/People，不做双向反查。让数据先沉淀。
- 第二周开始，Signals 新建记录时自动 upsert 到三个库，并建立 relation。
- 第三周开始，邮件日报里的 "推荐理由" 可以引用 Companies/People 里的 context（如 "该 founder 是 xAI 前核心研究员"）。
