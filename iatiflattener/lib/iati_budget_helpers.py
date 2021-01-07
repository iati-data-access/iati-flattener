from .utils import get_date, get_fy_fq_numeric


def get_budget_data(budget_element, default_currency, original_revised):
    budget_currency = budget_element.find('value').get('currency')
    if budget_currency is not None:
        currency = budget_currency
    else:
        currency = default_currency
    period_start = get_date(budget_element.find('period-start').get('iso-date'))
    period_end = get_date(budget_element.find('period-end').get('iso-date'))
    return ((period_start, period_end), {
        'period_start': period_start,
        'period_end': period_end,
        'currency_original': currency,
        'value_original': float(budget_element.find('value').text),
        'value_date': get_date(budget_element.find('value').get('value-date')),
        'original_revised': original_revised
    })


def get_budget_periods(exchange_rates, budgets):
    out = []
    for budget in budgets:
        if (budget['value_original'] == 0): continue
        period_start_fy, period_start_fq = get_fy_fq_numeric(budget['period_start'])
        period_end_fy, period_end_fq = get_fy_fq_numeric(budget['period_end'])
        year_range = range(period_start_fy, period_end_fy+1)

        closest_exchange_rate = exchange_rates.closest_rate(
            budget['currency_original'], budget['value_date']
        )
        exchange_rate = closest_exchange_rate.get('conversion_rate')
        value_usd = budget['value_original'] / exchange_rate
        for year in year_range:
            if (year == period_start_fy) and (year==period_end_fy):
                quarter_range = range(period_start_fq, period_end_fq+1)
            elif year == period_start_fy:
                quarter_range = range(period_start_fq, 4+1)
            elif year == period_end_fy:
                quarter_range = range(1, period_end_fq+1)
            else:
                quarter_range = range(1, 4+1)
            for quarter in quarter_range:
                out.append({
                    'fiscal_year': year,
                    'fiscal_quarter': quarter,
                    'value_usd': value_usd/len(quarter_range)/len(year_range),
                    'value_original': budget['value_original']/len(quarter_range)/len(year_range),
                    'value_date': budget['value_date'],
                    'exchange_rate': exchange_rate,
                    'currency_original': budget['currency_original'],
                    'original_revised': budget['original_revised']
                })
    return out


def get_budgets(activity, currency_original, exchange_rates):
    original_budget_els = activity.xpath("budget[not(@type) or @type='1']")
    revised_budget_els = activity.findall("budget[@type='2']")

    original_budgets = dict(map(lambda budget: get_budget_data(budget, currency_original, 'original'), original_budget_els))
    revised_budgets = dict(map(lambda budget: get_budget_data(budget, currency_original, 'revised'), revised_budget_els))

    revised_budget_start_dates = list(map(lambda budget: budget[0], revised_budgets))
    def filter_budgets(budget_item):
        for start_date in revised_budget_start_dates:
            if (budget_item[0][0] <= start_date) and (budget_item[0][0] >= start_date): return False
        return True

    budgets = list(dict(filter(filter_budgets, original_budgets.items())).values())
    budgets += list(revised_budgets.values())

    return get_budget_periods(exchange_rates, budgets)
