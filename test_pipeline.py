"""Automated tests for the pipeline itself.   Run:  python -m pytest -v

Testing the tester: these confirm the pipeline gives the exact numbers we planted in the data.
"""
import pytest

import pipeline


@pytest.fixture(scope="module")
def spark():
    session = pipeline.create_spark()
    yield session
    session.stop()


@pytest.fixture(scope="module")
def result(spark):
    raw = pipeline.load_raw(spark)
    clean, rejected, steps = pipeline.apply_rules(raw)
    return raw, clean, rejected, steps


def test_row_counts_match_the_planted_defects(result):
    raw, clean, rejected, _ = result
    assert raw.count() == 1025
    assert clean.count() == 942
    assert rejected.count() == 83


def test_each_rule_rejects_the_expected_number(result):
    _, _, _, steps = result
    rejected_by_rule = {s["rule"]: s["rejected"] for s in steps}
    assert rejected_by_rule == {
        "duplicate_transaction_id": 25,
        "missing_customer_id": 20,
        "negative_amount": 18,
        "inconsistent_date": 20,
    }


def test_raw_equals_clean_plus_rejected(result):
    raw, clean, rejected, _ = result
    assert raw.count() == clean.count() + rejected.count()


def test_clean_data_has_no_defects(result):
    _, clean, _, _ = result
    assert clean.select("transaction_id").distinct().count() == clean.count()
    assert clean.filter("customer_id IS NULL OR trim(customer_id) = ''").count() == 0
    assert clean.filter("amount < 0").count() == 0
    assert clean.filter("settlement_date < transaction_date").count() == 0


def test_every_rejected_row_has_a_reason(result):
    _, _, rejected, _ = result
    assert rejected.filter("reject_reason IS NULL").count() == 0
