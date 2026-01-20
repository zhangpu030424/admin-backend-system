-- ============================================
-- 归因数据查询 SQL (2026-01-12 到 2026-01-19)
-- 可直接执行的查询语句
-- ============================================

-- ============================================
-- 方案1：Adjust 数据源 - 简化版（推荐，直接可用）
-- 按日期和事件类型汇总
-- ============================================
SELECT 
  DATE_FORMAT(created_at, '%Y-%m-%d') AS query_date,
  event_name,
  COUNT(DISTINCT user_id) AS unique_user_count,
  COUNT(*) AS total_event_count
FROM adjust_event_record
WHERE DATE(created_at) BETWEEN '2026-01-12' AND '2026-01-19'
  AND status = 1
GROUP BY DATE(created_at), event_name
ORDER BY query_date DESC, event_name ASC;

-- ============================================
-- 方案2：Adjust 数据源 - 按日期汇总（所有事件类型在一行）
-- ============================================
SELECT 
  DATE_FORMAT(created_at, '%Y-%m-%d') AS query_date,
  COUNT(DISTINCT CASE WHEN event_name = 'install' THEN user_id END) AS install_users,
  COUNT(DISTINCT CASE WHEN event_name = 'register' THEN user_id END) AS register_users,
  COUNT(DISTINCT CASE WHEN event_name = 'loan' THEN user_id END) AS loan_users,
  COUNT(DISTINCT CASE WHEN event_name = 'loan_success' THEN user_id END) AS loan_success_users,
  COUNT(DISTINCT user_id) AS total_unique_users,
  COUNT(*) AS total_events
FROM adjust_event_record
WHERE DATE(created_at) BETWEEN '2026-01-12' AND '2026-01-19'
  AND status = 1
GROUP BY DATE(created_at)
ORDER BY query_date DESC;

-- ============================================
-- 方案3：AppsFlyer 数据源 - 按日期、媒体渠道、广告序列汇总
-- ============================================
SELECT 
  DATE_FORMAT(created_at, '%Y-%m-%d') AS query_date,
  media_source,
  af_c_id AS ad_sequence,
  COALESCE(event_name, JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.event_name'))) AS event_name,
  COUNT(DISTINCT CASE 
    WHEN COALESCE(event_name, JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.event_name'))) = 'install' 
    THEN appsflyer_id 
    ELSE customer_user_id 
  END) AS unique_user_count,
  COUNT(*) AS total_event_count
FROM appsflyer_callback
WHERE DATE(created_at) BETWEEN '2026-01-12' AND '2026-01-19'
  AND callback_status = 'processed'
  AND customer_user_id IS NOT NULL
GROUP BY DATE(created_at), 
         media_source,
         af_c_id,
         COALESCE(event_name, JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.event_name')))
ORDER BY query_date DESC, media_source ASC, ad_sequence ASC, event_name ASC;

-- ============================================
-- 方案4：AppsFlyer 数据源 - 按日期汇总（所有媒体渠道和广告序列）
-- ============================================
SELECT 
  DATE_FORMAT(created_at, '%Y-%m-%d') AS query_date,
  media_source,
  af_c_id AS ad_sequence,
  COUNT(DISTINCT CASE 
    WHEN COALESCE(event_name, JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.event_name'))) = 'install' 
    THEN appsflyer_id 
    ELSE customer_user_id 
  END) AS install_count,
  COUNT(DISTINCT customer_user_id) AS total_unique_users,
  COUNT(*) AS total_events
FROM appsflyer_callback
WHERE DATE(created_at) BETWEEN '2026-01-12' AND '2026-01-19'
  AND callback_status = 'processed'
  AND customer_user_id IS NOT NULL
GROUP BY DATE(created_at), media_source, af_c_id
ORDER BY query_date DESC, media_source ASC, ad_sequence ASC;

-- ============================================
-- 方案5：AppsFlyer 数据源 - 按事件类型汇总（不区分媒体渠道）
-- ============================================
SELECT 
  DATE_FORMAT(created_at, '%Y-%m-%d') AS query_date,
  COALESCE(event_name, JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.event_name'))) AS event_name,
  COUNT(DISTINCT CASE 
    WHEN COALESCE(event_name, JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.event_name'))) = 'install' 
    THEN appsflyer_id 
    ELSE customer_user_id 
  END) AS unique_user_count,
  COUNT(*) AS total_event_count
FROM appsflyer_callback
WHERE DATE(created_at) BETWEEN '2026-01-12' AND '2026-01-19'
  AND callback_status = 'processed'
  AND customer_user_id IS NOT NULL
GROUP BY DATE(created_at), 
         COALESCE(event_name, JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.event_name')))
ORDER BY query_date DESC, event_name ASC;

-- ============================================
-- 方案6：获取日期范围内的所有事件类型（用于了解有哪些事件）
-- ============================================
-- Adjust 事件类型
SELECT DISTINCT event_name
FROM adjust_event_record
WHERE DATE(created_at) BETWEEN '2026-01-12' AND '2026-01-19'
  AND status = 1
  AND event_name IS NOT NULL
ORDER BY event_name ASC;

-- AppsFlyer 事件类型
SELECT DISTINCT 
  COALESCE(event_name, JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.event_name'))) AS event_name
FROM appsflyer_callback
WHERE DATE(created_at) BETWEEN '2026-01-12' AND '2026-01-19'
  AND callback_status = 'processed'
  AND (event_name IS NOT NULL OR JSON_EXTRACT(raw_data, '$.event_name') IS NOT NULL)
ORDER BY event_name ASC;

-- ============================================
-- 方案7：汇总统计（总体概览）
-- ============================================
-- Adjust 数据汇总
SELECT 
  'Adjust' AS data_source,
  COUNT(DISTINCT DATE(created_at)) AS date_count,
  COUNT(DISTINCT event_name) AS event_type_count,
  COUNT(DISTINCT user_id) AS total_unique_users,
  COUNT(*) AS total_events
FROM adjust_event_record
WHERE DATE(created_at) BETWEEN '2026-01-12' AND '2026-01-19'
  AND status = 1;

-- AppsFlyer 数据汇总
SELECT 
  'AppsFlyer' AS data_source,
  COUNT(DISTINCT DATE(created_at)) AS date_count,
  COUNT(DISTINCT COALESCE(event_name, JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.event_name')))) AS event_type_count,
  COUNT(DISTINCT customer_user_id) AS total_unique_users,
  COUNT(DISTINCT media_source) AS media_source_count,
  COUNT(DISTINCT af_c_id) AS ad_sequence_count,
  COUNT(*) AS total_events
FROM appsflyer_callback
WHERE DATE(created_at) BETWEEN '2026-01-12' AND '2026-01-19'
  AND callback_status = 'processed'
  AND customer_user_id IS NOT NULL;
