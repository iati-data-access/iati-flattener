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
                assert activity_budget.budgets.value[idx][value_to_check] == expected_values_for_publisher[value_to_check][idx]

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
                            'canada' : [{'percentage': 48.48, 'code': '15112'},
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

        expected_values = {'fcdo': { 'fiscal_year' : [2007, 2008, 2009, 2010],
                                     'fiscal_quarter' : ['Q2', 'Q2', 'Q2', 'Q2'],
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

        expected_values = {'fcdo': { 'value_eur': [(514717 / 0.726269155) * 0.845022816,
                                                   (939770 / 0.726269155) * 0.845022816,
                                                   (1276100 / 0.726269155) * 0.845022816,
                                                   (-9077 / 0.726269155) * 0.845022816],
                                     'value_usd': [514717 / 0.726269155,
                                                   939770 / 0.726269155,
                                                   1276100 / 0.726269155,
                                                   -9077 / 0.726269155] } }

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])

    @pytest.mark.parametrize("publisher", ['3fi'])
    def test_activity_budget_values_split_within_single_year(self, activity_budget, publisher):
        """Tests the calculation of a budget entry which spans multiple quarters but only within same calendar year"""

        # spans 4 quarters; even split, so construct arrays with same value repeated 4 times
        expected_values = {'3fi': { 'value_original' : [25653580 / 4] * 4,
                                    'value_eur' : [((25653580 / 6.82795) * 0.845022816) / 4] * 4,
                                    'value_usd': [(25653580 / 6.82795) / 4] * 4 } }

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])


    @pytest.mark.parametrize("publisher", ["canada", "gdihub"])
    def test_activity_budget_values_split_across_multiple_years(self, activity_budget, publisher):

        # both canada and gdihub have budget over standard financial year (April-March), so four budgets per year
        # the gdihub file has one budget, the canada file has six budgets
        expected_values = {'gdihub': { 'value_original': [202736.20 / 4] * 4 },
                           'canada': { 'value_original': ([1979997 / 4] * 4) + ([3939997 / 4] * 4) +
                                                         ([2409070 / 4] * 4) + ([4390716 / 4] * 4) +
                                                         ([4275329 / 4] * 4) + ([2587224 / 4] * 4)
                                       }
                           }

        TestActivityBudgetModel.verify_budget_values(activity_budget, expected_values[publisher])
