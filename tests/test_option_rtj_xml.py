import pytest

from akshare_project.collectors.option import parse_rtj_option_rows_from_xml


def test_parse_rtj_option_rows_from_xml_builds_option_rows():
    xml_content = b"""
    <root>
      <dailydata>
        <productid>IO</productid>
        <instrumentid>IO2601-C-4000</instrumentid>
        <openprice>100.2</openprice>
        <highestprice>120.4</highestprice>
        <lowestprice>90.0</lowestprice>
        <closeprice>110.6</closeprice>
        <settlementprice>111.2</settlementprice>
        <presettlementprice>108.1</presettlementprice>
        <volume>123</volume>
        <turnover>4567890.12</turnover>
        <openinterest>321</openinterest>
        <preopeninterest>300</preopeninterest>
      </dailydata>
      <dailydata>
        <productid>IF</productid>
        <instrumentid>IF2601</instrumentid>
      </dailydata>
    </root>
    """

    rows = parse_rtj_option_rows_from_xml(xml_content, "2026-01-08")

    assert len(rows) == 1
    row = rows[0]
    assert row["index_type"] == "HS300"
    assert row["product_prefix"] == "IO"
    assert row["contract_code"] == "IO2601-C-4000"
    assert row["contract_month"] == "2601"
    assert row["option_type"] == "CALL"
    assert row["strike_price"] == pytest.approx(4000)
    assert row["close_price"] == pytest.approx(110.6)
    assert row["price_change_close"] == pytest.approx(2.5)
    assert row["price_change_settle"] == pytest.approx(3.1)
    assert row["open_interest_change"] == pytest.approx(21)


def test_parse_rtj_option_rows_from_xml_ignores_before_listed_date():
    xml_content = b"""
    <root>
      <dailydata>
        <productid>HO</productid>
        <instrumentid>HO2212-P-2600</instrumentid>
        <closeprice>10.0</closeprice>
      </dailydata>
    </root>
    """

    rows = parse_rtj_option_rows_from_xml(xml_content, "2022-12-16")

    assert rows == []
