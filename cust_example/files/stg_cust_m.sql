-- ============================================================
-- stg_cust_m: 고객 마스터 전일 변경분 스테이징
-- ============================================================
-- 설명: 원천 고객 마스터(cust_m)에서 run_date 기준 전일 변경분을 추출
-- 적재방식: incremental (일별 append, 동일 날짜 재실행 시 idempotent)
-- 파라미터: run_date (필수) - YYYYMMDD 형식
-- 실행예시:
--   dbt run -s stg_cust_m --vars '{"run_date": "20250308"}'
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

SELECT
    cust_id,
    cust_nm,
    gender_cd,
    birth_dt,
    phone_no,
    email_addr,
    join_dt,
    cust_status_cd,
    last_chng_dttm,
    '{{ run_date }}'::DATE AS base_dt
FROM {{ source('raw', 'cust_m') }}
WHERE last_chng_dttm >= DATEADD(day, -1, '{{ run_date }}'::DATE)
  AND last_chng_dttm <  '{{ run_date }}'::DATE
