from iatiflattener.lib.iati_helpers import get_narrative, get_org_name

class ActivityDataSetter():

    def set_data(self):

        def get_data(attr):
            if attr == 'title': return get_activity_title()
            if attr == 'reporting_org': return get_reporting_org()
            if attr == 'aid_type': return get_aid_type()
            if attr == 'finance_type': return get_finance_type()
            if attr == 'flow_type': return get_flow_type()
            if attr == 'currency_original': return get_currency()

        def get_activity_title():
            return get_narrative(self.activity.find("title"))

        def get_reporting_org():
            _ro = self.activity.find("reporting-org")
            _text = get_org_name(
                organisations=self.flattener.organisations,
                ref=_ro.get("ref"),
                text=get_narrative(_ro)
            )
            _type = _ro.get('type')
            _ref = _ro.get('ref')
            _display = "{} [{}]".format(_text, _ref)
            return {
                'text': _text,
                'type': _type,
                'ref': _ref,
                'display': _display
            }

        def get_reporting_org_type():
            return self.activity.find('reporting-org').get('type')

        def get_aid_type():
            el = self.activity.find('default-aid-type')
            if el is not None: return el
            else: return {}

        def get_finance_type():
            el = self.activity.find('default-finance-type')
            if el is not None: return el
            else: return {}

        def get_flow_type():
            el = self.activity.find('default-flow-type')
            if el is not None: return el
            else: return {}

        def get_currency():
            return self.activity.get('default-currency')

        activity_functions = ['title', 'reporting_org', 'reporting_org_type',
            'aid_type', 'finance_type', 'flow_type', 'currency_original']

        # Read data from activity in order to avoid having to get this for
        # each transaction
        for key in activity_functions:
            if getattr(self.transaction_budget, key) is not None: continue
            if self.iati_identifier in self.flattener.activity_data:
                if self.flattener.activity_data.get(self.iati_identifier).get(key) is not None:
                    setattr(self.transaction_budget, key, self.flattener.activity_data.get(self.iati_identifier).get(key))
                else:
                    setattr(self.transaction_budget, key, get_data(key))
                    self.flattener.activity_data[self.iati_identifier][key] = getattr(self.transaction_budget, key)
            else:
                setattr(self.transaction_budget, key, get_data(key))
                self.flattener.activity_data[self.iati_identifier] = {
                    key: getattr(self.transaction_budget, key)
                }

    def __init__(self, transaction_or_budget_flattener):
        self.iati_identifier = transaction_or_budget_flattener.iati_identifier
        self.activity = transaction_or_budget_flattener.activity
        self.transaction_budget = transaction_or_budget_flattener
        self.flattener = transaction_or_budget_flattener.flattener
        self.set_data()
