-- ============================================================
-- cust_mart: 고객 마트 요약 일별 스냅샷
-- ============================================================
-- 설명: 고객 통합 팩트를 기반으로 세그먼트, 활동성, 이탈위험 등
--       비즈니스 의미가 담긴 요약 지표를 생성하는 마트 테이블
-- 적재방식: incremental (일별 스냅샷 append)
-- 파라미터: run_date (필수) - YYYYMMDD 형식
-- 의존성: cust_fact
-- 실행예시:
--   dbt run -s cust_mart --vars '{"run_date": "20250308"}'
-- ============================================================

{{
  config(
    materialized='incremental',
    unique_key=['cust_id', 'base_dt'],
    dist='cust_id',
    sort='base_dt'
  )
}}

{% set run_date = var('run_date') %}

WITH fact AS (
    SELECT *
    FROM {{ ref('cust_fact') }}
    WHERE base_dt = '{{ run_date }}'::DATE
)

SELECT
    -- PK
    cust_id,
    base_dt,

    -- 고객 기본
    cust_nm,
    gender_cd,
    birth_dt,
    cust_status_cd,
    grade_cd,
    grade_nm,
    sido_nm,

    -- 가입 경과일
    DATEDIFF(day, join_dt, '{{ run_date }}'::DATE) AS days_since_join,

    -- 연령대
    CASE
        WHEN birth_dt IS NULL THEN 'UNKNOWN'
        WHEN DATEDIFF(year, birth_dt, '{{ run_date }}'::DATE) < 20 THEN '10대'
        WHEN DATEDIFF(year, birth_dt, '{{ run_date }}'::DATE) < 30 THEN '20대'
        WHEN DATEDIFF(year, birth_dt, '{{ run_date }}'::DATE) < 40 THEN '30대'
        WHEN DATEDIFF(year, birth_dt, '{{ run_date }}'::DATE) < 50 THEN '40대'
        WHEN DATEDIFF(year, birth_dt, '{{ run_date }}'::DATE) < 60 THEN '50대'
        ELSE '60대이상'
    END AS age_group,

    -- 구매 지표
    total_purchase_amt,
    total_purchase_cnt,
    last_purchase_dt,
    recent_3m_purchase_amt,
    recent_3m_purchase_cnt,
    CASE
        WHEN total_purchase_cnt > 0
        THEN ROUND(total_purchase_amt::DECIMAL / total_purchase_cnt, 0)
        ELSE 0
    END AS avg_purchase_amt,

    -- 고객 가치 세그먼트
    CASE
        WHEN total_purchase_amt >= 5000000 THEN 'VVIP'
        WHEN total_purchase_amt >= 1000000 THEN 'VIP'
        WHEN total_purchase_amt >= 500000  THEN 'GOLD'
        WHEN total_purchase_amt >= 100000  THEN 'SILVER'
        ELSE 'NORMAL'
    END AS value_segment,

    -- 최근 구매 경과일
    CASE
        WHEN last_purchase_dt IS NULL THEN -1
        ELSE DATEDIFF(day, last_purchase_dt, '{{ run_date }}'::DATE)
    END AS days_since_last_purchase,

    -- 활동 상태
    CASE
        WHEN last_purchase_dt IS NULL                                              THEN 'NO_PURCHASE'
        WHEN DATEDIFF(day, last_purchase_dt, '{{ run_date }}'::DATE) <= 30         THEN 'ACTIVE'
        WHEN DATEDIFF(day, last_purchase_dt, '{{ run_date }}'::DATE) <= 90         THEN 'DORMANT'
        WHEN DATEDIFF(day, last_purchase_dt, '{{ run_date }}'::DATE) <= 180        THEN 'AT_RISK'
        ELSE 'CHURNED'
    END AS activity_status,

    -- 최근 3개월 구매 추세 (직전 3개월 대비)
    CASE
        WHEN total_purchase_amt = 0 THEN 'NEW_OR_NONE'
        WHEN recent_3m_purchase_amt = 0 THEN 'DECLINING'
        WHEN recent_3m_purchase_amt >= total_purchase_amt * 0.5 THEN 'GROWING'
        ELSE 'STABLE'
    END AS purchase_trend

FROM fact

{% if is_incremental() %}
WHERE base_dt NOT IN (SELECT DISTINCT base_dt FROM {{ this }})
{% endif %}
