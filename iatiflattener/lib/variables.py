from collections import OrderedDict
import numpy as np

DPORTAL_URL = "https://d-portal.org/q.html?aid={}"


HEADERS = OrderedDict({
   'iati_identifier': str,
   'title': str,
   'reporting_org': str,
   'reporting_org_type': str,
   'aid_type': str,
   'finance_type': str,
   'flow_type': str,
   'provider_org': str,
   'provider_org_type': str,
   'receiver_org': str,
   'receiver_org_type': str,
   'transaction_type': str,
   'value_original': str,
   'currency_original': str,
   'value_usd': np.float64,
   'exchange_rate_date': str,
   'exchange_rate': str,
   'value_eur': np.float64,
   'value_local': np.float64,
   'transaction_date': str,
   'country_code': str,
   'multi_country': np.int32,
   'sector_category': str,
   'sector_code': str,
   'humanitarian': np.int32,
   'fiscal_year': np.int32,
   'fiscal_quarter': str,
   'fiscal_year_quarter': str,
   'url': str
})


MULTILANG_HEADERS = [
   'title',
   'reporting_org',
   'provider_org',
   'receiver_org'
]


def headers(langs):
   out = []
   for header in HEADERS.keys():
      if header in MULTILANG_HEADERS:
         out += ['{}#{}'.format(header, lang) for lang in langs]
      else:
         out += [header]
   return out


def dtypes(langs):
   out = []
   for header, dtype in HEADERS.items():
      if header in MULTILANG_HEADERS:
         out += [dtype for lang in langs]
      else:
         out += [dtype]
   return out


def headers_dtypes(langs):
   out = []
   for header, dtype in HEADERS.items():
      if dtype in MULTILANG_HEADERS:
         out += [{header: dtype} for lang in langs]
      else:
         out += [{header: dtype}]
   return out

def headers_with_langs(langs):
   return ['iati_identifier'] + ['{}#{}'.format(header, lang) for lang in langs for header in MULTILANG_HEADERS]


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
   'humanitarian',
   'fiscal_year',
   'fiscal_quarter',
   'fiscal_year_quarter',
   'url']


def group_by_headers_with_langs(langs):
   out = []
   for header in GROUP_BY_HEADERS:
      if header in MULTILANG_HEADERS:
         out += ['{}#{}'.format(header, lang) for lang in langs]
      else:
         out += [header]
   return out


def group_by_headers_with_lang(lang):
   return [
   'iati_identifier',
   'title#{}'.format(lang),
   'reporting_org#{}'.format(lang),
   'reporting_org_type',
   'aid_type',
   'finance_type',
   'flow_type',
   'provider_org#{}'.format(lang),
   'provider_org_type',
   'receiver_org#{}'.format(lang),
   'receiver_org_type',
   'transaction_type',
   'country_code',
   'multi_country',
   'sector_category',
   'sector_code',
   'humanitarian',
   'fiscal_year',
   'fiscal_quarter',
   'fiscal_year_quarter',
   'url']


OUTPUT_HEADERS = {
   'en': [
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
      'Humanitarian',
      'Calendar Year',
      'Calendar Quarter',
      'Calendar Year and Quarter',
      'URL',
      'Value (USD)',
      'Value (EUR)',
      'Value (Local currrency)'
   ],
   'fr': [
      'Identifiant de l’IITA',
      'Titre',
      'Organisme déclarant',
      'Type d’organisme déclarant',
      'Type d’aide',
      'Type de financement',
      'Type de flux',
      'Organisme prestataire',
      'Type d’organisme prestataire',
      'Organisme bénéficiaire',
      'Type d’organisme bénéficiaire',
      'Type de transaction',
      'Pays ou région bénéficiaire',
      'Multipays',
      'Catégorie de secteur',
      'Secteur',
      'Humanitaire',
      'Année civile',
      'Trimestre civil',
      'Année et trimestre civils',
      'URL',
      'Valeur (USD)',
      'Valeur (EUR)',
      'Valeur (Monnaie locale)'
   ],
   'es': [
      'Identificador de la IATI',
      'Título',
      'Organización informante',
      'Tipo de organización informante',
      'Tipo de ayuda',
      'Tipo de financiación',
      'Tipo de flujo',
      'Organización proveedora',
      'Tipo de organización proveedora',
      'Organización beneficiaria',
      'Tipo de organización beneficiaria',
      'Tipo de transacción',
      'País o región beneficiario',
      'Multinacional',
      'Categoría del sector',
      'Sector',
      'Humanitario',
      'Año natural',
      'Trimestre natural',
      'Año y trimestre naturales',
      'URL',
      'Valor (USD)',
      'Valor (EUR)',
      'Valor (Divisa local)'
   ],
   'pt': [
      'Identificador da IATI',
      'Título',
      'Organização relatora',
      'Tipo de organização relatora',
      'Tipo de ajuda',
      'Tipo de financiamento',
      'Tipo de fluxo',
      'Organização provedora',
      'Tipo de organização provedora',
      'Organização destinatária',
      'Tipo de organização destinatária',
      'Tipo de transação',
      'País/região destinatário',
      'Plurinacional',
      'Categoria de setor',
      'Setor',
      'Humanitária',
      'Ano civil',
      'Trimestre civil',
      'Ano e trimestre civis',
      'URL',
      'Valor (USD)',
      'Valor (EUR)',
      'Valor (Moeda local)'
   ],
}

TRANSLATIONS = {
   'en': {
      'no-data': 'No data'
   },
   'fr': {
      'no-data': 'Aucune donnée'
   },
   'es': {
      'no-data': 'Ningún dato'
   },
   'pt': {
      'no-data': 'Sem dados'
   }
}
