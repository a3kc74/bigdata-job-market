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
        F.when(location_text.rlike("ho chi minh|hcm|tp\\. hcm|h\\S* ch\\S* minh"), F.lit("Ho Chi Minh"))
        .when(location_text.rlike("ha noi|hanoi|hn|h\\S* n\\S*i"), F.lit("Ha Noi"))
        .when(location_text.rlike("da nang|d\\S* n\\S*ng"), F.lit("Da Nang"))
        .when(location_text.rlike("hai phong|h\\S*i ph\\S*ng"), F.lit("Hai Phong"))
        .when(location_text.rlike("can tho|c\\S*n th\\S*"), F.lit("Can Tho"))
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


def _looks_negotiable(text: Column) -> Column:
    return text.rlike(r"th\S*a\s*thu\S*n|tho\S*a\s*thu\S*n|negotiable|canh\s*tranh|c\S*nh\s*tranh")


def salary_min_million(salary_text: Column) -> Column:
    text = F.lower(F.coalesce(salary_text, F.lit("")))
    first_num = F.regexp_extract(text, r"(\d+(?:[.,]\d+)?)", 1)
    numeric = F.regexp_replace(first_num, ",", ".").cast("double")
    looks_upper_bound = text.rlike(
        r"(?:dưới|duoi|tối\s*đa|toi\s*da|lên\s*đến|len\s*den|đến|den|tới|toi|up\s*to|<=|<)"
        r"\s*\d+(?:[.,]\d+)?"
    )

    # "Duoi/Toi da/Len den 40 trieu" means max=40 and min is unknown.
    return F.when(_looks_negotiable(text) | looks_upper_bound, F.lit(None).cast("double")).otherwise(numeric)


def salary_max_million(salary_text: Column) -> Column:
    text = F.lower(F.coalesce(salary_text, F.lit("")))
    second_num = F.regexp_extract(text, r"\d+(?:[.,]\d+)?\D+(\d+(?:[.,]\d+)?)", 1)
    parsed_second = F.regexp_replace(second_num, ",", ".").cast("double")
    first_num = F.regexp_extract(text, r"(\d+(?:[.,]\d+)?)", 1)
    parsed_first = F.regexp_replace(first_num, ",", ".").cast("double")
    looks_lower_bound = text.rlike(
        r"(?:trên|tren|lớn\s*hơn|lon\s*hon|hơn|hon|từ|tu|>=|>)\s*\d+(?:[.,]\d+)?"
    )

    # "Tren/Lon hon/Tu 40 trieu" means min=40 and max is unknown.
    return (
        F.when(_looks_negotiable(text) | looks_lower_bound, F.lit(None).cast("double"))
        .when(parsed_second.isNotNull(), parsed_second)
        .otherwise(parsed_first)
    )


def salary_bin(avg_salary_million: Column, salary_text: Column) -> Column:
    text = F.lower(F.coalesce(salary_text, F.lit("")))
    return (
        F.when(_looks_negotiable(text), F.lit("negotiable"))
        .when(avg_salary_million.isNull(), F.lit("unknown"))
        .when(avg_salary_million < 10, F.lit("under_10m"))
        .when(avg_salary_million < 20, F.lit("10_20m"))
        .when(avg_salary_million < 30, F.lit("20_30m"))
        .when(avg_salary_million < 50, F.lit("30_50m"))
        .otherwise(F.lit("over_50m"))
    )
