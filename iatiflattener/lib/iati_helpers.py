from .utils import get_first

TRANSACTION_TYPES_RULES = {
    "1": {'provider': '1', 'receiver': 'reporter'},
    "2": {'provider': 'reporter', 'receiver': '4'},
    "3": {'provider': 'reporter', 'receiver': '4'},
    "4": {'provider': 'reporter', 'receiver': '4'},
    "5": {'provider': '4', 'receiver': 'reporter'},
    "6": {'provider': '4', 'receiver': 'reporter'},
    "7": {'provider': 'reporter', 'receiver': '4'},
    "8": {'provider': 'reporter', 'receiver': '4'},
    "11": {'provider': '1', 'receiver': 'reporter'},
    "12": {'provider': 'reporter', 'receiver': '4'},
    "13": {'provider': '1', 'receiver': 'reporter'},
    "activity": {'provider': 'reporter', 'receiver': '4'}
}


def fix_narrative(ref, text):
    return text.strip()


def get_narrative(container, lang='en'):
    narratives = container.xpath("narrative")
    if len(narratives) == 0: return ""
    if len(narratives) == 1:
        if narratives[0].text:
            return fix_narrative(container.get('ref'), narratives[0].text.strip())
        else: return ""

    def filter_lang_non_en(element):
        el_lang = element.get("{http://www.w3.org/XML/1998/namespace}lang")
        if lang != 'en':
            return el_lang in (lang, lang.upper())
        else:
            return el_lang in (None, 'en', 'EN')

    def filter_lang(element):
        el_lang = element.get("{http://www.w3.org/XML/1998/namespace}lang")
        return el_lang in (None, 'en', 'EN')

    filtered = list(filter(filter_lang_non_en, narratives))
    if len(filtered) == 0:
        filtered = list(filter(filter_lang, narratives))
        if len(filtered)==0:
            return fix_narrative(container.get('ref'), narratives[0].text.strip())
    return fix_narrative(container.get('ref'), filtered[0].text.strip())


def get_org_name(organisations, ref, text=None):
    if (ref == None) or (ref.strip() == ""):
        return text
    if ref in organisations:
        return organisations[ref]
    if (text == None):
        return ""
    return text


def get_narrative_text(element):
    if element.find("narrative") is not None:
        return element.find("narrative").text
    return None


def filter_none(item):
    return item is not None


def clean_sectors(_sectors):
    sectors = list(filter(lambda sector: sector.get('percentage', 100) not in ["", "0", "0.0"], _sectors))
    _total_pct = sum(list(map(lambda sector: float(sector.get('percentage', 100.0)), sectors)))
    _pct = 100.0
    return [{
        'percentage': float(sector.get('percentage', 100))/(_total_pct/100),
        'code': sector.get('code')
    } for sector in sectors]


def clean_countries(_countries, _regions=[]):
    countries = list(filter(lambda item: item.get('percentage', 100) not in ["", "0", "0.0"], _countries))
    _total_pct = sum(list(map(lambda item: float(item.get('percentage', 100.0)), countries)))

    # We take regions only if
    #  a) they exist
    #  b) country percentages don't sum to around 100%
    #  c) country percentages aren't all 100%

    if (
        (len(_regions) > 0) and
        (round(_total_pct) != 100) and
        ((_total_pct == 0) or (round(_total_pct)!=(100*len(countries))))
        ):
        regions = list(filter(lambda item2: item2.get('percentage', 100) not in ["", "0", "0.0"], _regions))
        _total_pct_2 = sum(list(map(lambda item2: float(item2.get('percentage', 100.0)), regions)))
        countries += regions
        _total_pct += _total_pct_2

    _pct = 100.0
    return [{
        'percentage': float(item.get('percentage', 100))/(_total_pct/100),
        'code': item.get('code')
    } for item in countries]


def get_countries(activity, transaction):
    countries = get_first((
        transaction.xpath("recipient-country"),
        activity.xpath("recipient-country")),
        [])
    regions = get_first((
        transaction.xpath("recipient-region[not(@vocabulary) or @vocabulary='1']"),
        activity.xpath("recipient-region[not(@vocabulary) or @vocabulary='1']")),
        [])
    return clean_countries(countries, regions)


def get_sectors(activity, transaction):
    sectors = get_first((
        transaction.xpath("sector[not(@vocabulary) or @vocabulary='1']"),
        activity.xpath("sector[not(@vocabulary) or @vocabulary='1']")),
        [])
    tr_sectors = clean_sectors(sectors)
    if len(tr_sectors) > 0:
        return tr_sectors
    return [{'percentage': 100.0, 'code': ''}]


def get_sector_category(code, category_group):
    if code == None: return ""
    return category_group.get(code[0:3], "")


def value_in_usd(value, currency, value_date, exchange_rates):
    closest_exchange_rate = exchange_rates.closest_rate(
        currency, value_date
    )
    exchange_rate = closest_exchange_rate.get('conversion_rate')
    value_usd = value / exchange_rate
    return value_usd
