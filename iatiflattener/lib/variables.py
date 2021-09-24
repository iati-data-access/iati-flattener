import numpy as np

CSV_HEADERS = [
    'iati_identifier',
    'title',
    'reporting_org',
    'reporting_org_type',
    'aid_type',
    'finance_type',
    'flow_type',
    'provider_org',
    'provider_org_type',
    'receiver_org',
    'receiver_org_type',
    'transaction_type',
    'value_original', 'currency_original',
    'value_usd', 'exchange_rate_date',
    'exchange_rate', 'transaction_date',
    'country_code', 'multi_country',
    'sector_category', 'sector_code',
    'fiscal_year', 'fiscal_quarter',
    'fiscal_year_quarter']

_DTYPES = [str, str, str, str,
                 str, str, str, str, str,
                 str, str, str, str,
                 str, np.float64, str, str,
                 str, str,
                 np.int32, str, str,
                 np.int32, str, str]

GROUP_BY_HEADERS = [
   'iati_identifier',
   'title',
   'reporting_org',
   'reporting_org_type',
   'aid_type',
   'finance_type',
   'flow_type',
   'provider_org',
   'provider_org_type',
   'receiver_org',
   'receiver_org_type',
   'transaction_type',
   'country_code',
   'multi_country',
   'sector_category',
   'sector_code',
   'fiscal_year',
   'fiscal_quarter',
   'fiscal_year_quarter']

OUTPUT_HEADERS = [
   'IATI Identifier',
   'Title',
   'Reporting Organisation',
   'Reporting Organisation Type',
   'Aid Type',
   'Finance Type',
   'Flow Type',
   'Provider Organisation',
   'Provider Organisation Type',
   'Receiver Organisation',
   'Receiver Organisation Type',
   'Transaction Type',
   'Recipient Country or Region',
   'Multi Country',
   'Sector Category',
   'Sector',
   'Calendar Year',
   'Calendar Quarter',
   'Calendar Year and Quarter',
   'Value (USD)']
