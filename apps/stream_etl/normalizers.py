"""Spark column normalizers for phase 3 clean stream records."""

from pyspark.sql import Column
from pyspark.sql import functions as F


def epoch_ms_to_timestamp(column: Column) -> Column:
    return F.to_timestamp(F.from_unixtime((column.cast("double") / F.lit(1000.0))))


def normalize_city(location_array: Column) -> Column:
    location_text = F.lower(
        F.concat_ws(" ", F.coalesce(location_array, F.array().cast("array<string>")))
    )
    return (
        F.when(location_text.rlike("hồ chí minh|ho chi minh|hcm|tp\\. hcm"), F.lit("Ho Chi Minh"))
        .when(location_text.rlike("hà nội|ha noi|hanoi|hn"), F.lit("Ha Noi"))
        .when(location_text.rlike("đà nẵng|da nang"), F.lit("Da Nang"))
        .when(location_text.rlike("hải phòng|hai phong"), F.lit("Hai Phong"))
        .when(location_text.rlike("cần thơ|can tho"), F.lit("Can Tho"))
        .otherwise(F.lit("unknown"))
    )


def normalize_skills(skills_needed: Column, skills_should_have: Column) -> Column:
    merged = F.array_distinct(
        F.concat(
            F.coalesce(skills_needed, F.array()),
            F.coalesce(skills_should_have, F.array()),
        )
    )
    normalized = F.transform(merged, lambda skill: F.initcap(F.trim(skill)))
    return F.array_sort(
        F.array_distinct(F.filter(normalized, lambda skill: skill.isNotNull() & (skill != "")))
    )


def salary_min_million(salary_text: Column) -> Column:
    text = F.lower(F.coalesce(salary_text, F.lit("")))
    first_num = F.regexp_extract(text, r"(\d+(?:[.,]\d+)?)", 1)
    numeric = F.regexp_replace(first_num, ",", ".").cast("double")
    return F.when(text.rlike("thỏa thuận|thoả thuận|negotiable|cạnh tranh"), F.lit(None).cast("double")).otherwise(numeric)


def salary_max_million(salary_text: Column) -> Column:
    text = F.lower(F.coalesce(salary_text, F.lit("")))
    second_num = F.regexp_extract(text, r"\d+(?:[.,]\d+)?\D+(\d+(?:[.,]\d+)?)", 1)
    parsed_second = F.regexp_replace(second_num, ",", ".").cast("double")
    first = salary_min_million(salary_text)
    return F.when(parsed_second.isNotNull(), parsed_second).otherwise(first)


def salary_bin(avg_salary_million: Column, salary_text: Column) -> Column:
    text = F.lower(F.coalesce(salary_text, F.lit("")))
    return (
        F.when(text.rlike("thỏa thuận|thoả thuận|negotiable"), F.lit("negotiable"))
        .when(avg_salary_million.isNull(), F.lit("unknown"))
        .when(avg_salary_million < 10, F.lit("under_10m"))
        .when(avg_salary_million < 20, F.lit("10_20m"))
        .when(avg_salary_million < 30, F.lit("20_30m"))
        .when(avg_salary_million < 50, F.lit("30_50m"))
        .otherwise(F.lit("over_50m"))
    )
