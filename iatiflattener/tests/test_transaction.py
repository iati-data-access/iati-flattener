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


def write_outputs(publisher, transaction, flat_transaction_json):
    with open('iatiflattener/tests/artefacts/{}-transaction.json'.format(publisher), 'w') as json_file:
        json_file.write(transaction.jsonify())
    with open('iatiflattener/tests/artefacts/{}-transaction-flat.json'.format(publisher), 'w') as json_file:
        json_file.write(flat_transaction_json)


@pytest.mark.parametrize("publisher", ["fcdo", "canada", "usaid"])
class TestModel():

    @pytest.fixture()
    def node(self, publisher):
        doc = etree.parse('iatiflattener/tests/fixtures/{}-activity.xml'.format(publisher))
        yield doc.xpath('//transaction')[0]

    @pytest.fixture
    def transaction(self, node):
        activity_cache = model.ActivityCache()
        _transaction_node = node
        _activity_node = _transaction_node.getparent()
        _transaction = model.Transaction(_activity_node, _transaction_node,
            activity_cache, exchange_rates, countries_currencies)
        _transaction.generate()
        return _transaction

    @pytest.fixture
    def flat_transaction(self, transaction):
        _flat_transaction = model.FlatTransaction(transaction)
        return list(_flat_transaction.flatten())[0]

    @pytest.fixture
    def flat_transaction_json(self, transaction):
        _flat_transaction = model.FlatTransaction(transaction)
        return list(_flat_transaction.flatten_json())[0]

    def test_as_dict(self, publisher, transaction):
        with open('iatiflattener/tests/artefacts/{}-transaction.json'.format(publisher), 'r') as json_file:
            assert transaction.jsonify() == json_file.read()

    def test_flat_transaction(self, publisher, transaction, flat_transaction_json):
        with open('iatiflattener/tests/artefacts/{}-transaction-flat.json'.format(publisher), 'r') as json_file:
            assert flat_transaction_json == json_file.read()

    def test_flat_transaction_values(self, transaction, flat_transaction):
        """
        Confirm that the flat transaction value is the USD value *
        country percentage * sector percentage
        """
        country = flat_transaction['country_code']
        transaction_dict = transaction.as_dict()
        country_pct = list(filter(lambda _country: _country['code'] == country, transaction_dict['countries']))[0]['percentage']
        sector = flat_transaction['sector_code']
        sector_pct = list(filter(lambda _sector: _sector['code'] == sector, transaction_dict['sectors']))[0]['percentage']
        assert flat_transaction['value_usd'] == transaction_dict['value_usd'] * (country_pct/100) * (sector_pct/100)

    def _test_write_outputs(self, publisher, transaction, flat_transaction_json):
        """
        Remove the underscore to enable writing, if you update any of the outputs.
        """
        write_outputs(publisher, transaction, flat_transaction_json)

