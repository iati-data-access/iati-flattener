import os
import datetime
import pytest
import exchangerates
from iatiflattener import model
from lxml import etree

exchange_rates = exchangerates.CurrencyConverter(update=False, source="iatiflattener/tests/fixtures/rates.csv")

assert "GBP" in exchange_rates.known_currencies()

countries_currencies = {'BD': 'BDT'}


class TestActivityBudgetModel:
    """Tests for the ActivityBudget model, which is responsible for splitting budget values across quarters."""

    @classmethod
    def verify_budget_values(cls, activity_budget, expected_values_for_publisher):
        """Takes an ActivityBudget instance and verifies its values against a set of expected values"""
        # go through each of the budget items
        for idx in range(0, len(activity_budget.budgets.value)):
            # and then go through testing each value (this setup allows for different keys in 'expected_values'
            # so that we can add another test case which didn't use GBP as original currency
            for value_to_check in expected_values_for_publisher:
                assert activity_budget.budgets.value[idx][value_to_check] == pytest.approx(expected_values_for_publisher[value_to_check][idx])

    @pytest.fixture()
    def node(self, publisher):
        doc = etree.parse('iatiflattener/tests/fixtures/{}-activity.xml'.format(publisher))
        yield doc.xpath('//iati-activity')[0]

    @pytest.fixture
    def activity_budget(self, node):
        activity_cache = model.ActivityCache()
        _activity_node = node
        _activitybudget = model.ActivityBudget(_activity_node, activity_cache, exchange_rates, countries_currencies)
        _activitybudget.generate()
        return _activitybudget

    @pytest.fixture
    def flat_budget_json(self, activity_budget):
        _flat_budget = model.FlatBudget(activity_budget)
        return list(_flat_budget.flatten_json())[0]

    @pytest.mark.parametrize("publisher", ["3fi", "fcdo", "canada"])
    def test_correct_number_budgets_quarters(self, activity_budget, publisher):

        expected_budget_quarter_counts = { '3fi': 4,  # 1 budget, spanning 4 quarters
                                           'fcdo': 4,  # 4 revised budgets, each spans 1 quarter
                                           'canada': 6 * 4  # 6 (original) budgets in this file, each spans 4 quarters
                                           }

        assert len(activity_budget.budgets.value) == expected_budget_quarter_counts[publisher]

    @pytest.mark.parametrize("publisher", ["fcdo", "canada"])
    def test_activity_budget_titles_en(self, activity_budget, publisher):
        """Test the English title (basic sanity check)."""

        titles = {'fcdo': 'PROCOFSERVICES and P0220 for Civil Ser. Cap. Bldng. Liberia',
                  'canada': 'Partnerships for Municipal Innovation in Local Economic Development'}

        assert activity_budget.title.value['en'] == titles[publisher]

    @pytest.mark.parametrize("publisher", ["fcdo", "canada"])
    def test_activity_budget_sectors(self, activity_budget, publisher):

        expected_sectors = {'fcdo': [{'percentage': 100, 'code': '15110'}],
                            'canada': [{'percentage': 48.48, 'code': '15112'},
                                       {'percentage': 5, 'code': '15150'},
                                       {'percentage': 40, 'code': '25010'},
                                       {'percentage': 5, 'code': '32130'},
                                       {'percentage': 1.52, 'code': '41010'}
                                       ]
                            }

        for sector_index in range(0, len(activity_budget.sectors.value)-1):
            sector = activity_budget.sectors.value[sector_index]
            assert sector['code'] == expected_sectors[publisher][sector_index]['code']
            assert sector['percentage'] == pytest.approx(expected_sectors[publisher][sector_index]['percentage'])


    @pytest.mark.parametrize("publisher", ["fcdo"])
    def test_activity_budget_fiscal_year_quarter(self, activity_budget, publisher):

        expected_values = {'fcdo': { 'fiscal_year': [2007, 2008, 2009, 2010],
                                     'fiscal_quarter': ['Q2', 'Q2', 'Q2', 'Q2'],
                                     'fiscal_year_quarter': ['2007 Q2', '2008 Q2', '2009 Q2', '2010 Q2'] } }

        for idx in range(0, len(activity_budget.budgets.value)):
            for obj_attribute_to_check in expected_values[publisher]:
                assert activity_budget.budgets.value[idx][obj_attribute_to_check] == \
                       expected_values[publisher][obj_attribute_to_check][idx]


    @pytest.mark.parametrize("publisher", ["fcdo"])  # the FCDO test file doesn't require splitting
    def test_activity_budget_financial_values_no_split(self, activity_budget, publisher):
        """Test the calculation of the budget values when no splitting over quarters is required"""

        # the fcdo test file has four budgets, each applicable to a single quarter
        expected_values = {'fcdo': [514717, 939770, 1276100, -9077] }

        for idx in range(0, len(activity_budget.budgets.value)):
            assert activity_budget.budgets.value[idx]['value_original'] == expected_values[publisher][idx]

    @pytest.mark.parametrize("publisher", ["fcdo"])  # the FCDO test file doesn't require splitting
    def test_activity_budget_values_currency_exchange(self, activity_budget, publisher):
        """Test the calculation of the budget values in the non-base currency"""

        expected_values = {'fcdo': {'value_eur': [(514717 / 0.726269155) * 0.845022816,
                                                  (939770 / 0.726269155) * 0.845022816,
                                                  (1276100 / 0.726269155) * 0.845022816,
                                                  (-9077 / 0.726269155) * 0.845022816],
                                    'value_usd': [514717 / 0.726269155,
                                                  939770 / 0.726269155,
                                                  1276100 / 0.726269155,
                                                  -9077 / 0.726269155]}}

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])

    @pytest.mark.parametrize("publisher", ['3fi'])
    def test_activity_budget_values_split_within_single_year(self, activity_budget, publisher):
        """Tests the calculation of a budget entry which spans multiple quarters but only within same calendar year"""

        # 3fi budget spans the whole calendar year for 2022, so covers 4 quarters in different financial years
        # it should not be an even split, because the split methodology requires calculating budget by day, and
        # different quarters have different numbers of days.
        # total budget: 25653580
        # Days in each quarter: 90, 91, 92, 92
        # budget per day: 25653580 / 365 = ~70283.78
        # Budget per quarter (in DKK):

        budget_per_day = 25653580 / 365
        budget_per_quarter_orig = [budget_per_day * day_count for day_count in [90, 91, 92, 92]]
        budget_per_quarter_eur = [(orig_per_q / 6.82795) * 0.845022816 for orig_per_q in budget_per_quarter_orig]
        budget_per_quarter_usd = [orig_per_q / 6.82795 for orig_per_q in budget_per_quarter_orig]

        expected_values = {'3fi': { 'value_original': budget_per_quarter_orig,
                                    'value_eur': budget_per_quarter_eur,
                                    'value_usd': budget_per_quarter_usd}}

        total_activity_budgets = sum([budget['value_original'] for budget in activity_budget.budgets.value])
        assert total_activity_budgets == pytest.approx(25653580)

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])

    @pytest.mark.parametrize("publisher", ["gdihub"])
    def test_activity_budget_values_split_across_multiple_years(self, activity_budget, publisher):

        # gdihub has a single budget over standard financial year (April-March), so four budgets per year

        # gdihub is 2021 Q2 - 2022 Q1 inclusive: 91, 92, 92, 90
        gdi_budget_per_day = 202736.20 / 365
        gdi_budget_per_quarter = [gdi_budget_per_day * day_count for day_count in [91, 92, 92, 90]]

        expected_values = {'gdihub': {'value_original': gdi_budget_per_quarter}}

        total_activity_budgets = sum([budget['value_original'] for budget in activity_budget.budgets.value])
        assert total_activity_budgets == pytest.approx(202736.20)

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])

    @pytest.mark.parametrize("publisher", ["canada"])
    def test_activity_budget_values_split_across_multiple_years_with_leap_year(self, activity_budget, publisher):

        # canada has six budgets over standard financial year (April-March), so four budgets per year
        # budgets are consecutive years, starting in 2015/16
        # so first year (2015/16) and fifth year (2019/20) contain the leap day

        total_budget_values = [1979997, 3939997, 2409070, 4390716, 4275329, 2587224]
        is_leap_year = [True, False, False, False, True, False]
        year_type_to_days_mapping = {True: [91, 92, 92, 91], False: [91, 92, 92, 90]}
        budgets_per_day = [total / (366 if leap else 365) for total, leap in zip(total_budget_values, is_leap_year)]

        list_years = [[budget_per_day * days_in_quarter for days_in_quarter in year_type_to_days_mapping[is_leap]]
                      for budget_per_day, is_leap in zip(budgets_per_day, is_leap_year)]

        quarterly_budgets = [item for sublist in list_years for item in sublist]

        expected_values = {'canada': {'value_original': quarterly_budgets}}

        total_activity_budgets = sum([budget['value_original'] for budget in activity_budget.budgets.value])
        assert total_activity_budgets == pytest.approx(sum(total_budget_values))

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])

    @pytest.mark.parametrize("publisher", ["sr"])
    def test_activity_budget_values_split_for_budget_with_non_standard_boundaries(self, activity_budget, publisher):

        # sr has a budget which starts in the middle of a quarter
        sr_budget_per_day = 19504521 / 307
        sr_budget_per_quarter = [sr_budget_per_day * day_count for day_count in [32, 91, 92, 92]]

        expected_values = {'sr': {'value_original': sr_budget_per_quarter}}

        total_activity_budgets = sum([budget['value_original'] for budget in activity_budget.budgets.value])
        assert total_activity_budgets == 19504521

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])

    @pytest.mark.parametrize("publisher", ["budget-one-day"])
    def test_activity_budget_values_split_for_budget_with_one_day(self, activity_budget, publisher):

        # activity has a one day budget

        expected_values = {'budget-one-day': {'value_original': [100000],
                                              'fiscal_year': [2017],
                                              'fiscal_quarter': ['Q1']}}
 

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])


    @pytest.mark.parametrize("publisher", ["budget-dates-issue"])
    def test_activity_budget_values_split_for_budget_with_dates_issue(self, activity_budget, publisher):

        # activity has a budget which is all within the same quarter,
        # but with the end date before the start date
        # one possibility here would be to raise an exception
        # but at present, have taken the view that it is best to allocate entire budget
        # to the single quarter; that way, these values will get counted for any 
        # extant bad data.
        # see e.g. planned disbursements in `XI-IATI-EC_INTPA-2019/405-854`
        od_budget_per_quarter = [100000]

        expected_values = {'budget-dates-issue': {'value_original': od_budget_per_quarter}}

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])


    @pytest.mark.parametrize("publisher", ["ec-intpa"])
    def test_activity_budget_values_split_for_ec_budget_with_dates_issue(self, activity_budget, publisher):

        # EC international partnerships has 0-value budgets and >0 value
        # planned disbursements, though the latter have issues (e.g. start date
        # often after end date)
        # see e.g. planned disbursements in `XI-IATI-EC_INTPA-2019/405-854`
        # there are 0 value budgets running from 2019 Q1 to 2050 Q4
        quarters = (2050-2018)*4
        od_budget_per_quarter = [0 for quarter in range(0, quarters+1)]

        expected_values = {'ec-intpa': {'value_original': od_budget_per_quarter}}

        one_quarter = activity_budget.budgets.value[0]
        assert one_quarter['fiscal_year'] == 2019
        assert one_quarter['fiscal_quarter'] == 'Q1'

        total_activity_budgets = sum([budget['value_original'] for budget in activity_budget.budgets.value])
        assert total_activity_budgets == 0

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])
