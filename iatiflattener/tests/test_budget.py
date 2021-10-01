import os
import datetime
import pytest
import exchangerates
from iatiflattener import model
from lxml import etree

exchange_rates = exchangerates.CurrencyConverter(
    update=False, source="iatiflattener/tests/fixtures/rates.csv")

assert "GBP" in exchange_rates.known_currencies()

countries_currencies = {'BD': 'BDT'}

def write_outputs(publisher, budget, flat_budget_json):
    with open('iatiflattener/tests/artefacts/{}-budget.json'.format(publisher), 'w') as json_file:
        json_file.write(budget.jsonify())
    with open('iatiflattener/tests/artefacts/{}-budget-flat.json'.format(publisher), 'w') as json_file:
        json_file.write(flat_budget_json)

@pytest.mark.parametrize("publisher", ["fcdo", "canada"])
class TestModel():
    @pytest.fixture()
    def node(self, publisher):
        doc = etree.parse('iatiflattener/tests/fixtures/{}-activity.xml'.format(publisher))
        yield doc.xpath('//iati-activity')[0]

    @pytest.fixture
    def budget(self, node):
        activity_cache = model.ActivityCache()
        _activity_node = node
        _activitybudget = model.ActivityBudget(_activity_node,
            activity_cache, exchange_rates, countries_currencies)
        _activitybudget.generate()
        return _activitybudget

    @pytest.fixture
    def flat_budget(self, budget):
        _flat_budget = model.FlatBudget(budget)
        return list(_flat_budget.flatten())[0]

    @pytest.fixture
    def flat_budget_json(self, budget):
        _flat_budget = model.FlatBudget(budget)
        return list(_flat_budget.flatten_json())[0]

    def _test_budget_values(self, budget):
        """
        Test the output of a budget
        """
        assert budget.as_dict() == {}
        assert budget.title.value == "Hello"

    def test_flat_budget_json(self, publisher, flat_budget_json):
        """
        Test the output of a budget
        """
        with open('iatiflattener/tests/artefacts/{}-budget-flat.json'.format(publisher), 'r') as json_file:
            assert flat_budget_json == json_file.read()

    def _test_write_outputs(self, publisher, budget, flat_budget_json):
        """
        Remove the underscore to enable writing, if you update any of the outputs.
        """
        write_outputs(publisher, budget, flat_budget_json)
