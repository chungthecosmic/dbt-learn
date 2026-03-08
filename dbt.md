astronomer-cosmos==1.9.2
dbt-duckdb==1.9.2
dbt-core==1.9.4


test
 - Data test: 생성된 데이터 값 기반 검증(데이터 품질 확인)
    - Generic test: 재사용 가능한 템플릿 테스트(범용적 검증)
    - Singular test: 특정 요구사항 맞춤 테스트(세밀한 검증)
 - Unit test: SQL 로직 기반 검증(코드 정합성 확인)


날짜별로 특정 기간 밑으로는 A로직, 특정 기간 이후로는 B로직이라면, jinja의 if else 문으로 구분해보기