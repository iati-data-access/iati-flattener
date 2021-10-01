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


def get_org(organisations, activity_data, transaction_or_activity, provider=True, lang='en'):
    def _make_org_output(_text, _ref, _type):
        _display = ""
        if _text is not None:
            _display += _text
        if (_display != "") and (_ref is not None):
            _display += " "
        if (_ref is not None) and (_ref != ""):
            _display += '[{}]'.format(_ref)
        return {
            'text': get_first((_text, _ref)),
            'ref': get_first((_ref, _text)),
            'type': _type,
            'display': _display
        }

    provider_receiver = {True: 'provider', False: 'receiver'}[provider]
    if (transaction_or_activity.tag == 'transaction'):
        transaction = transaction_or_activity
        activity = transaction.getparent()
        transaction_type = transaction.find("transaction-type").get("code")
        if transaction.find('{}-org'.format(provider_receiver)) is not None:
            _el = transaction.find('{}-org'.format(provider_receiver))
            _text = get_org_name(
                organisations=organisations,
                ref=_el.get("ref"),
                text=get_narrative(_el, lang)
            )
            _ref = _el.get("ref")
            _type = _el.get("type")
            if (_ref is not None) or (_text is not None):
                return _make_org_output(_text, _ref, _type)
    else:
        activity = transaction_or_activity
        transaction_type = 'activity'

    role = {
        True: TRANSACTION_TYPES_RULES[transaction_type]['provider'],
        False: TRANSACTION_TYPES_RULES[transaction_type]['receiver']}[provider]
    if ((role == "reporter")
        or (provider==True and transaction_type in ['3', '4'])
        or (provider==False and transaction_type in ['1', '11', '13'])):

        if activity_data.get('reporting_org') is None:
            if transaction_or_activity == 'transaction':
                _ro = transaction.getparent().find("reporting-org")
            else:
                _ro = activity.find("reporting-org")
            _text = get_org_name(
                organisations=organisations,
                ref=_ro.get("ref"),
                text=get_narrative(_ro, lang)
            )
            _type = _ro.get('type')
            _ref = _ro.get('ref')
            _display = "{} [{}]".format(_text, _ref)
            activity_data['reporting_org'] = {
                'text': _text,
                'type': _type,
                'ref': _ref,
                'display': _display
            }
        return activity_data.get('reporting_org').get(lang)

    if activity_data.get("participating_org_{}".format(role)) is None:
        if transaction_or_activity == 'transaction':
            activity_participating = transaction.getparent().findall("participating-org[@role='{}']".format(role))
        else:
            activity_participating = activity.findall("participating-org[@role='{}']".format(role))
        if len(activity_participating) == 1:
            _text = get_org_name(
                    organisations=organisations,
                    ref=activity_participating[0].get('ref'),
                    text=get_narrative_text(activity_participating[0])
                )
            _ref = activity_participating[0].get("ref")
            _type = activity_participating[0].get("type")

            activity_data["participating_org_{}".format(role)] = _make_org_output(_text, _ref, _type)
        elif len(activity_participating) > 1:
            _orgs = list(map(lambda _org: _make_org_output(
                _text=get_org_name(
                        organisations=organisations,
                        ref=_org.get("ref"),
                        text=get_narrative(_org, lang)
                ),
                _ref=_org.get('ref'),
                _type=_org.get('type')), activity_participating)
                )

            _text = "; ".join(filter(filter_none, [org.get('text') for org in _orgs]))
            _ref = "; ".join(filter(filter_none, [org.get('ref') for org in _orgs]))
            _type = "; ".join(filter(filter_none, [org.get('type') for org in _orgs]))
            _display = "; ".join(filter(filter_none, [org.get('display') for org in _orgs]))

            activity_data["participating_org_{}".format(role)] = {
                'text': _text,
                'ref': _ref,
                'type': _type,
                'display': _display
            }

    if activity_data.get('participating_org_{}'.format(role)) is not None:
        return activity_data.get('participating_org_{}'.format(role))
    return {
        'text': None,
        'ref': None,
        'type': None,
        'display': None
    }


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
