-- ============================================================
-- cust_fact: 고객 통합 팩트 일별 스냅샷
-- ============================================================
-- 설명: 고객 마스터 + 주소/등급/구매이력을 통합한 일별 전체 고객 스냅샷
-- 적재방식: incremental (일별 스냅샷 append)
-- 파라미터: run_date (필수) - YYYYMMDD 형식
-- 의존성: stg_cust_m, raw.cust_addr, raw.cust_grade, raw.purchase_hist
-- 참고: 스냅샷이므로 전일 변경분이 아닌 전체 고객을 대상으로 함.
--       stg_cust_m은 변경분만 담고 있으므로, 스냅샷 기준은
--       원천 cust_m의 전체 데이터를 사용함.
-- 실행예시:
--   dbt run -s cust_fact --vars '{"run_date": "20250308"}'
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

WITH cust AS (
    -- 전체 고객의 최신 상태 (스냅샷이므로 변경분이 아닌 전체)
    SELECT
        cust_id,
        cust_nm,
        gender_cd,
        birth_dt,
        phone_no,
        email_addr,
        join_dt,
        cust_status_cd
    FROM {{ source('raw', 'cust_m') }}
    WHERE cust_status_cd != 'D'  -- 탈퇴 고객 제외 (비즈니스 요건에 따라 조정)
),

addr AS (
    SELECT
        cust_id,
        addr,
        detail_addr,
        zip_cd,
        sido_nm,
        sigungu_nm
    FROM {{ source('raw', 'cust_addr') }}
    WHERE addr_type_cd = '01'  -- 기본주소
),

grade AS (
    SELECT
        cust_id,
        grade_cd,
        grade_nm,
        grade_start_dt,
        grade_end_dt
    FROM {{ source('raw', 'cust_grade') }}
    WHERE '{{ run_date }}'::DATE BETWEEN grade_start_dt AND grade_end_dt
),

purchase_summary AS (
    SELECT
        cust_id,
        SUM(purchase_amt)   AS total_purchase_amt,
        COUNT(*)            AS total_purchase_cnt,
        MAX(purchase_dt)    AS last_purchase_dt,
        -- 최근 3개월 구매
        SUM(CASE WHEN purchase_dt >= DATEADD(month, -3, '{{ run_date }}'::DATE)
                 THEN purchase_amt ELSE 0 END) AS recent_3m_purchase_amt,
        SUM(CASE WHEN purchase_dt >= DATEADD(month, -3, '{{ run_date }}'::DATE)
                 THEN 1 ELSE 0 END) AS recent_3m_purchase_cnt
    FROM {{ source('raw', 'purchase_hist') }}
    WHERE purchase_dt < '{{ run_date }}'::DATE
    GROUP BY cust_id
)

SELECT
    -- 고객 기본정보
    c.cust_id,
    c.cust_nm,
    c.gender_cd,
    c.birth_dt,
    c.phone_no,
    c.email_addr,
    c.join_dt,
    c.cust_status_cd,

    -- 주소정보
    a.addr,
    a.detail_addr,
    a.zip_cd,
    a.sido_nm,
    a.sigungu_nm,

    -- 등급정보
    g.grade_cd,
    g.grade_nm,

    -- 구매 집계
    COALESCE(p.total_purchase_amt, 0)       AS total_purchase_amt,
    COALESCE(p.total_purchase_cnt, 0)       AS total_purchase_cnt,
    p.last_purchase_dt,
    COALESCE(p.recent_3m_purchase_amt, 0)   AS recent_3m_purchase_amt,
    COALESCE(p.recent_3m_purchase_cnt, 0)   AS recent_3m_purchase_cnt,

    -- 메타
    '{{ run_date }}'::DATE AS base_dt

FROM cust c
LEFT JOIN addr a
    ON c.cust_id = a.cust_id
LEFT JOIN grade g
    ON c.cust_id = g.cust_id
LEFT JOIN purchase_summary p
    ON c.cust_id = p.cust_id

{% if is_incremental() %}
WHERE '{{ run_date }}'::DATE NOT IN (SELECT DISTINCT base_dt FROM {{ this }})
{% endif %}
