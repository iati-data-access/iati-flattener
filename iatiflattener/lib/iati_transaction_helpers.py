from .iati_helpers import value_in_usd
from .utils import get_first
from collections import defaultdict

def get_codes_from_transactions(transactions, exchange_rates):
    # If there is only one code, we just return that one as 100%
    unique_codes = set(list(map(lambda transaction: transaction[0], transactions)))
    if len(unique_codes) == 1:
        return [{'percentage': 100.0, 'code': next(iter(unique_codes))}]
    # If there is only one currency, we can just split by value, ignoring currency conversion
    unique_currencies = set(list(map(lambda transaction: transaction[1], transactions)))
    if len(unique_currencies) != 1:
        transactions = list(map(lambda transaction: (
            transaction[0],
            'USD',
            value_in_usd(
                value=transaction[2],
                currency=t[1],
                value_date=get_date(value_date=t[3]),
                exchange_rates=exchange_rates
            ),
            transaction[3]
        ), transactions))

    transactionSum = sum(map(lambda t: float(t[2]), transactions))
    _out = defaultdict(dict)
    for code, transaction_currency, transaction_value, transaction_value_date in transactions:
        if _out.get(code) is None:
            _out[code] = 0
        _out[code] += (transaction_value/transactionSum)*100.0

    return list(map(lambda _code: {'code': _code[0], 'percentage': _code[1]}, _out.items()))


def get_sectors_from_transactions(activity,
        default_currency, exchange_rates):
    # Try outgoing commitments
    transactions_with_sectors = activity.xpath(
        "transaction[sector[not(@vocabulary) or @vocabulary='1']][transaction-type/@code='2']"
    )
    # If no outgoing commitments, try incoming commitments
    if len(transactions_with_sectors) == 0:
        transactions_with_sectors = activity.xpath(
            "transaction[sector[not(@vocabulary) or @vocabulary='1']][transaction-type/@code='11']"
        )
    # If there are no transactions, return
    if len(transactions_with_sectors) == 0: return [{'code': '', 'percentage': 100.0}]
    transactions = list(map(lambda transaction: (
        transaction.xpath("sector[not(@vocabulary) or @vocabulary='1']")[0].get('code'),
        transaction.find("value").get('currency', default_currency),
        float(transaction.find("value").text),
        transaction.find("value").get('value-date'),
    ), transactions_with_sectors))

    return get_codes_from_transactions(transactions, exchange_rates)


def get_countries_from_transactions(activity,
        default_currency, exchange_rates):
    # Try outgoing commitments
    transactions_with_countries = activity.xpath(
        "transaction[recipient-country or recipient-region[not(@vocabulary) or @vocabulary='1']][transaction-type/@code='2']"
    )
    # If no outgoing commitments, then try incoming commitments
    if len(transactions_with_countries) == 0:
        transactions_with_countries = activity.xpath(
            "transaction[recipient-country or recipient-region[not(@vocabulary) or @vocabulary='1']][transaction-type/@code='11']"
        )
    # If there are no transactions, return
    if len(transactions_with_countries) == 0: return []
    transactions = list(map(lambda transaction: (
        get_first((
            transaction.xpath("recipient-country"),
            transaction.xpath("recipient-region[not(@vocabulary) or @vocabulary='1']")
        ))[0].get('code'),
        transaction.find("value").get('currency', default_currency),
        float(transaction.find("value").text),
        transaction.find("value").get('value-date'),
    ), transactions_with_countries))

    return get_codes_from_transactions(transactions, exchange_rates)


def get_aid_type_from_transactions(activity,
    default_currency, exchange_rates):
    transactions_with_aid_types = activity.xpath(
        "transaction[aid-type[not(@vocabulary) or @vocabulary='1']][transaction-type/@code='2']"
    )

    if len(transactions_with_aid_types) == 0:
        transactions_with_aid_types = activity.xpath(
            "transaction[aid-type[not(@vocabulary) or @vocabulary='1']][transaction-type/@code='11']"
        )

    if len(transactions_with_aid_types) == 0: return [{'code': '', 'percentage': 100.0}]
    transactions = list(map(lambda transaction: (
        transaction.xpath("aid-type[not(@vocabulary) or @vocabulary='1']")[0].get('code'),
        transaction.find("value").get('currency', default_currency),
        float(transaction.find("value").text),
        transaction.find("value").get('value-date'),
    ), transactions_with_aid_types))

    return get_codes_from_transactions(transactions, exchange_rates)


def get_finance_type_from_transactions(activity,
    default_currency, exchange_rates):
    transactions_with_finance_types = activity.xpath(
        "transaction[finance-type][transaction-type/@code='2']"
    )
    if len(transactions_with_finance_types) == 0:
        transactions_with_finance_types = activity.xpath(
            "transaction[finance-type][transaction-type/@code='2']"
        )
    if len(transactions_with_finance_types) == 0: return [{'code': '', 'percentage': 100.0}]
    transactions = list(map(lambda transaction: (
        transaction.find("finance-type").get('code'),
        transaction.find("value").get('currency', default_currency),
        float(transaction.find("value").text),
        transaction.find("value").get('value-date'),
    ), transactions_with_finance_types))

    return get_codes_from_transactions(transactions, exchange_rates)
