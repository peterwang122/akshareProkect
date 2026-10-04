import pytest

from akshare_project.collectors.quant_index import build_exchange_option_pc_map
from akshare_project.db.db_tool import add_missing_szse_159922_contract_rows


def contract(code, kind, **changes):
    return {"exchange": "SZSE", "underlying_code": "159922", "contract_code": code,
            "contract_trade_code": "159922" + kind[0] + "2605M006000",
            "option_type": kind, "contract_month": "2605", "strike_price": 6,
            "listed_date": "2026-05-01", "last_trade_date": "2026-05-27", **changes}


def row(code, kind, amount, **changes):
    return {**contract(code, kind), "trade_date": "2026-05-08", "close_price": 1,
            "volume": 10, "turnover": amount, **changes}


def result(rows, product="159922"):
    payload = build_exchange_option_pc_map(rows, {("2026-05-08", product): 6})
    return payload[("2026-05-08", "中证500" if product == "159922" else "沪深300")]["szse:" + product]


def test_full_chain_uses_original_amounts():
    payload = result([row("1", "CALL", 123.45), row("2", "PUT", 246.9)])
    assert payload["option_turnover_pc_ratio"] == pytest.approx(2)
    assert payload["turnover_coverage"]["complete"]


@pytest.mark.parametrize("amount", [None, -1, float("nan"), float("inf")])
def test_partial_amounts_cannot_produce_a_ratio(amount):
    payload = result([row("1", "CALL", 100), row("2", "PUT", amount)])
    assert payload["option_turnover_pc_ratio"] is None
    assert not payload["turnover_coverage"]["complete"]


def test_missing_put_side_is_not_zero():
    assert result([row("1", "CALL", 100)])["option_turnover_pc_ratio"] is None


def test_original_zero_and_zero_denominator():
    assert result([row("1", "CALL", 100), row("2", "PUT", 0)])["option_turnover_pc_ratio"] == 0
    assert result([row("1", "CALL", 0), row("2", "PUT", 0)])["option_turnover_pc_ratio"] is None


def test_missing_contract_placeholder_invalidates_turnover_only():
    rows = [row("1", "CALL", 100), row("2", "PUT", 200)]
    infos = [contract("1", "CALL"), contract("2", "PUT"), contract("3", "CALL")]
    expanded = add_missing_szse_159922_contract_rows(rows, infos)
    assert len(rows) == 2 and len(expanded) == 3
    assert expanded[-1]["close_price"] is None
    assert expanded[-1]["turnover"] is None
    payload = result(expanded)
    assert payload["option_turnover_pc_ratio"] is None
    assert payload["option_pc_current_month"] == 1
    assert payload["turnover_coverage"]["missing_contracts"] == ["3"]


def test_listing_expiry_and_other_products_not_changed():
    rows = [row("1", "CALL", 100), row("2", "PUT", 200)]
    infos = [contract("3", "CALL", listed_date="2026-05-09"),
             contract("4", "PUT", last_trade_date="2026-05-08"),
             contract("5", "CALL", underlying_code="159919")]
    assert add_missing_szse_159922_contract_rows(rows, infos) == rows
    other = [row("1", "CALL", 100, underlying_code="159919"),
             row("2", "PUT", None, underlying_code="159919")]
    payload = result(other, "159919")
    assert "turnover_coverage" not in payload


def test_placeholder_expansion_is_idempotent_and_preserves_existing_zero():
    rows = [row("1", "CALL", 0)]
    infos = [contract("1", "CALL"), contract("2", "PUT")]
    once = add_missing_szse_159922_contract_rows(rows, infos)
    assert once == add_missing_szse_159922_contract_rows(once, infos)
    assert once[0]["turnover"] == 0
