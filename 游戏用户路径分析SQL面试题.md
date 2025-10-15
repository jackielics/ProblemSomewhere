（某大厂内包岗位数据工程面试题）游戏用户路径分析
为了了解用户在游戏中的使用和操作习惯，我们要通过路径分析的方法去看用户的操作路径，假设用户在游戏中的操作事件序列如下：

| Dteventtime(时间)   | Roleid（用户 id） | Pathcode（事件） |
|---------------------|-------------------|------------------|
| 2025-07-29 00:30:52 | 1000              | 10001            |
| 2025-07-29 00:30:53 | 1000              | 10002            |
| 2025-07-29 00:30:54 | 1000              | 10003            |
| 2025-07-29 00:30:55 | 1000              | 10001            |
| 2025-07-29 00:30:56 | 1000              | 10003            |
| 2025-07-29 00:30:57 | 1000              | 10005            |
| 2025-07-29 00:30:52 | 1001              | 10001            |
| 2025-07-29 00:30:53 | 1001              | 10002            |
| 2025-07-29 00:30:54 | 1001              | 10004            |
| 2025-07-29 00:30:54 | 1002              | 10001            |

第一问：要按天统计每条路径（单条路径最长保留5跳）下的人数和次数，例如上述例子中要统计出
10001->10002->10003->10001->10003（最长5跳）  1人，1次
10001->10002->10003->10001  1人，1次
10001->10002	2 人，2次
10002->10003	1人，1次
给出详细的计算sql，可以建中间表和结果表

***

环境：https://www.db-fiddle.com/，在线数据库Lab，版本：MySQL v9

<h2>使用窗口函数ROW_NUMBER()，给每个用户的每个event根据时间排</h2>

```sql
SELECT
    DATE(Dteventtime) AS event_date,  -- 提取日期
    Roleid,
    Pathcode,
    ROW_NUMBER() OVER (
      PARTITION BY Roleid, DATE(Dteventtime) 
      ORDER BY Dteventtime
    ) AS event_seq  -- 每个用户每天的事件序号
  FROM user_events
```
<img width="1345" height="815" alt="image" src="https://github.com/user-attachments/assets/8308194c-c430-49fb-933b-f1fffa5c4014" />

<h2>递归查询，基于Seed基础查询去递归迭代</h2>

```sql
, path_recursive AS (
  -- Seed基础查询：1跳路径
  SELECT
    event_date,
    Roleid,
    Pathcode AS path,
    1 AS hop_count  -- 路径长度（跳数）
  FROM event_order
  WHERE event_seq = 1

  UNION ALL

  -- 递归部分：拼接后续事件，生成2-5跳路径
  SELECT
    o.event_date,
    o.Roleid,
    CONCAT(r.path, '->', o.Pathcode) AS path,
    r.hop_count + 1 AS hop_count
  FROM path_recursive r
  JOIN event_order o 
    ON r.Roleid = o.Roleid 
    AND r.event_date = o.event_date 
    AND o.event_seq = r.hop_count + 1  -- 下一个事件序号
  WHERE r.hop_count < 5  -- 限制最长5跳
)
```

<h2>对递归生成的路径，按日期、路径分组，统计</h2>

```sql
-- 结果表：统计路径的人数和次数
SELECT
  event_date,
  path,
  COUNT(DISTINCT Roleid) AS user_count,  -- 不同用户数
  COUNT(*) AS event_count  -- 路径出现总次数
FROM path_recursive
GROUP BY event_date, path
ORDER BY event_date, path;
```

<h2>合并所有SQL</h2>

```sql
WITH RECURSIVE event_order AS (
  SELECT
    DATE(Dteventtime) AS event_date,
    Roleid,
    Pathcode,
    ROW_NUMBER() OVER (
      PARTITION BY Roleid, DATE(Dteventtime) 
      ORDER BY Dteventtime
    ) AS event_seq
  FROM user_events
),
path_recursive AS (
  SELECT
    event_date,
    Roleid,
    Pathcode AS path,
    1 AS hop_count
  FROM event_order
  WHERE event_seq = 1

  UNION ALL

  SELECT
    o.event_date,
    o.Roleid,
    CONCAT(r.path, '->', o.Pathcode) AS path,
    r.hop_count + 1 AS hop_count
  FROM path_recursive r
  JOIN event_order o 
    ON r.Roleid = o.Roleid 
    AND r.event_date = o.event_date 
    AND o.event_seq = r.hop_count + 1
  WHERE r.hop_count < 5
)
SELECT
  event_date,
  path,
  COUNT(DISTINCT Roleid) AS user_count,
  COUNT(*) AS event_count
FROM path_recursive
GROUP BY event_date, path
ORDER BY event_date, path;
<img width="432" height="581" alt="image" src="https://github.com/user-attachments/assets/5e2536f7-a858-41a3-b7e3-8728331f73fc" />

```
