from datetime import date, datetime

import pytest

from akshare_project.collectors.macro_cycle_parsing import (
    discover_page, parse_money_supply_table, parse_release, parse_tsf_table, period_from_title,
)


def article(title, body, published="2026/09/14 17:00"):
    return f'<html><meta name="ArticleTitle" content="{title}"><meta name="PubDate" content="{published}"><div id="zoom">{body}</div></html>'


def rows_by_key(rows):
    return {r["series_key"]: r for r in rows}


def test_finance_units_cumulative_and_m1_basis():
    html = article("2026年8月金融统计数据报告", "八月末，狭义货币(M1)余额115.77万亿元，同比增长4.1%。广义货币(M2)余额356.81万亿元，同比增长7.5%。前八个月社会融资规模增量累计为23.91万亿元。其中，对实体经济发放的人民币贷款增加10.23万亿元，政府债券净融资8.77万亿元。三、其他。")
    release, values = parse_release(html, "https://www.pbc.gov.cn/report", datetime(2026, 9, 20))
    rows = rows_by_key(values)
    assert rows["m1_balance"]["value"] == "115770000000000.00"
    assert rows["m1_yoy"]["basis_version"] == "m1_2025"
    assert rows["tsf_flow"]["period_kind"] == "ytd"
    assert rows["tsf_government_bond_flow"]["value"] == "8770000000000.00"
    assert release["available_at"] == datetime(2026, 9, 14, 17)


def test_date_only_conservative_next_day_and_period_not_publication():
    html = article("2024年12月金融统计数据报告", "狭义货币(M1)余额67.1万亿元，同比下降1.4%。广义货币(M2)余额300万亿元，同比增长7.5%。" + "说明" * 30, "2025-01-14")
    release, values = parse_release(html, "https://www.pbc.gov.cn/report")
    assert release["available_at"] == datetime(2025, 1, 15)
    row = rows_by_key(values)["m1_yoy"]
    assert row["value"] == "-1.4"
    assert row["period_end"] == "2024-12-31"
    assert row["basis_version"] == "m1_legacy"


def test_profit_official_growth_not_derived():
    html = article("2026年1—7月份全国规模以上工业企业利润增长17.6%", "1—7月份全国规模以上工业企业实现利润总额45820.6亿元，同比增长17.6%。实现营业收入80.92万亿元，同比增长6.5%。7月份，规模以上工业企业利润同比增长11.2%。" + "说明" * 10)
    _, values = parse_release(html, "https://www.stats.gov.cn/report")
    rows = rows_by_key(values)
    assert rows["industrial_profit_ytd_yoy"]["value"] == "17.6"
    assert rows["industrial_profit_month_yoy"]["value"] == "11.2"
    assert rows["industrial_profit_ytd"]["period_kind"] == "ytd"


def test_pmi_unique_month():
    html = article("2026年8月中国采购经理指数运行情况", "制造业采购经理指数（PMI）为49.8%。生产指数为50.4%。新订单指数为50.6%。二、中国非制造业商务活动指数为49.0%。" + "官方说明" * 15)
    _, values = parse_release(html, "https://www.stats.gov.cn/report")
    assert len(values) == 4
    assert {r["unit"] for r in values} == {"points"}


def test_discovery_pagination_and_unrelated_links():
    html = '<a href="/data/a.html">2026年8月金融统计数据报告</a><a href="/regional">2026年地区社会融资规模报告</a><a tagname="/list/11871-2.html">下一页</a>'
    rows, pages = discover_page(html, "https://www.pbc.gov.cn/list/index.html")
    assert len(rows) == 1
    assert pages == ["https://www.pbc.gov.cn/list/11871-2.html"]


def test_bad_page_fails_not_no_update():
    with pytest.raises(ValueError):
        parse_release("<html>系统维护</html>", "https://www.stats.gov.cn/")


@pytest.mark.parametrize("title,period", [("2025年上半年金融统计数据报告", date(2025, 6, 30)), ("2025年一季度社会融资规模增量统计数据报告", date(2025, 3, 31)), ("2026年1—2月份全国规模以上工业企业利润增长", date(2026, 2, 28))])
def test_periods(title, period):
    assert period_from_title(title) == period


def test_tsf_monthly_table_and_missing_cells():
    html = '<p>社会融资规模增量统计表 单位：亿元人民币</p><table><tr><td>月份</td><td>社会融资规模增量</td><td>政府债券</td><td>人民币贷款</td></tr><tr><td>2026.08</td><td>16577</td><td>10097</td><td>552</td></tr><tr><td>2026.07</td><td>14068</td><td>--</td><td>-5896</td></tr></table>'
    release, rows = parse_tsf_table(html, "https://www.pbc.gov.cn/table.htm", datetime(2026, 9, 20))
    assert len(rows) == 5
    assert {r["period_kind"] for r in rows} == {"monthly"}
    assert release["published_at"] is None
    assert release["available_at"] == datetime(2026, 9, 20)
    assert rows[0]["value"] == "1657700000000"


def test_revision_reversion_is_a_new_vintage():
    from akshare_project.db.macro_cycle import vintage_identity
    release = {"source_key": "url", "content_hash": "A"}
    first, n, new = vintage_identity(release, None)
    assert (n, new) == (1, True)
    latest = {"release_id": first, "revision_number": 1, "content_hash": "A"}
    assert vintage_identity(release, latest) == (first, 1, False)
    second, n, new = vintage_identity({**release, "content_hash": "B"}, latest)
    third, n, new = vintage_identity(release, {"release_id": second, "revision_number": 2, "content_hash": "B"})
    assert first != second != third and (n, new) == (3, True)


def test_ppi_mom_same_sentence_and_core_cpi_only():
    _, rows = parse_release(article("2026年8月份工业生产者出厂价格同比上涨3.8%", "全国工业生产者出厂价格同比上涨3.8%，环比上涨0.4%。" + "说明" * 50), "https://www.stats.gov.cn/ppi")
    assert rows_by_key(rows)["ppi_mom"]["value"] == "0.4"
    _, rows = parse_release(article("国家统计局解读2026年8月份CPI和PPI数据", "扣除食品和能源价格的核心CPI同比涨幅回升至1.0%。工业生产者出厂价格同比上涨3.8%。" + "说明" * 50), "https://www.stats.gov.cn/core")
    assert [r["series_key"] for r in rows] == ["core_cpi_yoy"]


def test_old_loan_report_contains_ytd_and_monthly_not_same_period_kind():
    body = "前两个月人民币贷款增加4.11万亿元。分部门看，住户部门贷款增加9192亿元，其中，中长期贷款增加9195亿元；非金融企业及机关团体贷款增加3.41万亿元，其中，中长期贷款增加1.92万亿元；非银行业金融机构贷款减少2165亿元。2月当月人民币贷款增加8858亿元。分部门看，住户部门贷款减少706亿元，其中，中长期贷款增加2226亿元；非金融企业及机关团体贷款增加8341亿元，其中，中长期贷款增加5127亿元；非银行业金融机构贷款增加1221亿元。"
    _, rows = parse_release(article("2019年2月金融统计数据报告", body, "2019-03-10"), "https://www.pbc.gov.cn/loans")
    values = {(r["series_key"], r["period_kind"]): float(r["value"]) for r in rows}
    assert values[("loan_household_mlt", "ytd")] == 9195e8
    assert values[("loan_household_mlt", "monthly")] == 2226e8
    assert values[("loan_corporate_mlt", "monthly")] == 5127e8


@pytest.mark.parametrize('sentence,expected', [
    ('核心CPI环比上涨0.2%，上月为下降0.6%；同比上涨0.7%，涨幅扩大。', '0.7'),
    ('核心CPI同比由上月持平转为上涨0.3%，一季度核心CPI与去年同期持平。', '0.3'),
    ('核心CPI连续第四个月回升，本月环比上涨0.5%，同比上涨0.6%。', '0.6'),
])
def test_core_cpi_sentence_variants(sentence, expected):
    _, rows = parse_release(article('国家统计局解读2025年1月份CPI和PPI数据', sentence + '说明' * 50), 'https://www.stats.gov.cn/core')
    assert rows_by_key(rows)['core_cpi_yoy']['value'] == expected


def test_core_not_disclosed_does_not_invent_observation():
    _, rows = parse_release(article('国家统计局解读2025年2月份CPI和PPI数据', '全国CPI同比上涨0.2%。' + '说明' * 50), 'https://www.stats.gov.cn/core')
    assert rows == []


def test_tsf_october_keeps_trailing_zero_and_annual_period():
    html = '<p>社会融资规模增量统计表 单位：亿元人民币</p><table><tr><td>月份</td><td>社会融资规模增量</td></tr><tr><td>2019.1</td><td>100</td></tr><tr><td>2019.10</td><td>200</td></tr><tr><td>2019.10</td><td>200</td></tr></table>'
    _, rows = parse_tsf_table(html, 'https://www.pbc.gov.cn/table.htm')
    assert [(r['period_end'], r['value']) for r in rows] == [('2019-01-31', '10000000000'), ('2019-10-31', '20000000000')]
    assert period_from_title('2024年社会融资规模存量统计数据报告') == date(2024, 12, 31)


def test_stock_footnote_is_not_current_total_and_parser_correction_is_append_only():
    from akshare_project.db.macro_cycle import vintage_identity
    body = '社会融资规模存量为403.45万亿元，同比增长7.8%。其中，对实体经济发放的人民币贷款余额为251.16万亿元，同比增长7.7%。注1：2023年1月末，上述三类机构对实体经济发放的人民币贷款余额8410亿元。'
    _, rows = parse_release(article('2024年10月社会融资规模存量统计数据报告', body), 'https://www.pbc.gov.cn/tsf')
    assert float(rows_by_key(rows)['tsf_rmb_loan_stock']['value']) == 251.16e12
    release = {'source_key': 'a', 'content_hash': 'b'}
    old, _, _ = vintage_identity(release, None)
    new, revision, created = vintage_identity(release, {'release_id': old, 'revision_number': 1, 'content_hash': 'b'}, force_new=True)
    assert new != old and revision == 2 and created


def test_tsf_mixed_amount_and_percentage_sections():
    html = '<p>社会融资规模增量统计表</p><table><tr><td>单位：亿元人民币</td><td></td></tr><tr><td>月份</td><td>社会融资规模增量</td></tr><tr><td>2019.01</td><td>46791</td></tr><tr><td>单位：%</td><td></td></tr><tr><td>月份</td><td>社会融资规模增量</td></tr><tr><td>2019.01</td><td>100.0</td></tr></table>'
    _, rows = parse_tsf_table(html, 'https://www.pbc.gov.cn/tsf.htm')
    assert len(rows) == 1 and float(rows[0]['value']) == 46791e8


def test_money_supply_table_and_comparable_m1_basis():
    html = '''<p>货币供应量 单位：亿元人民币</p><table>
      <tr><td>项目</td><td>2025.01</td><td>2025.02</td></tr>
      <tr><td>货币和准货币（M2）</td><td>3185247.18</td><td>3205173.24</td></tr>
      <tr><td>货币（M1）</td><td>1124457.45</td><td>1094370.01</td></tr>
      <tr><td>注：中国人民银行自统计2025年1月份数据起，启用新修订的狭义货币（M1）统计口径。按可比口径回溯后：</td><td></td><td></td></tr>
      <tr><td>项目</td><td>2024.01</td><td>2024.02</td></tr>
      <tr><td>余额（亿元）</td><td>1120120</td><td>1093158</td></tr>
      <tr><td>同比增速</td><td>3.3%</td><td>2.6%</td></tr></table>'''
    release, rows = parse_money_supply_table(html, 'https://www.pbc.gov.cn/money.htm')
    by_identity = {(r['series_key'], r['period_end'], r['basis_version']): r for r in rows}
    assert float(by_identity[('m2_balance', '2025-01-31', 'official')]['value']) == 3185247.18e8
    assert float(by_identity[('m1_balance', '2025-01-31', 'm1_2025')]['value']) == 1124457.45e8
    assert by_identity[('m1_yoy', '2024-01-31', 'm1_2025')]['value'] == '3.3'
    assert release['vintage_status'] == 'first_observed_only'
