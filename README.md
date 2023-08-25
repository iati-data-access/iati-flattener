# IATI Flattener

A set of tools to flatten IATI XML transactional and budget data into clean, denormalised data.

IATI Flattener groups the data in three ways:
  - activities (``//iati-activity``) are grouped into files by publisher; each row is an iati activity.
  - budgets (IATI Activity XML Path: ``//iati-activity[budget]``) are grouped into files by region or country
    (stored as ``budget-CODE.csv``, where CODE is a region or country code); budgets are split over sectors,
    quarters, aid types, and/or flow types, so there may be multiple budget rows in a single file for the
    same IATI budget.
    The original value will be prorated out; the sum of the various rows will add up to the original value
    of the budget. Because budgets may be applicable to multiple countries or regions, the same budget may
    appear in multiple region/country files.
  - transactions (IATI Activity XML Path: //transaction) are grouped by region or country (stored as
    ``transaction-CODE.csv``, where CODE is a region or country code); transactions are split over sectors and
    quarters, so there may be multiple transaction rows for a given IATI transaction (they can also be split
    over regions/countries). The original value will be prorated out; the sum of the various rows will add up
    to the original value of the transaction.

Nightly generated Excel files can also be found at [Country Development Finance Data](https://countrydata.iatistandard.org/), using the "Access data files" option.

## Installation

You can install this package via PyPI

```
pip3 install iatiflattener
```
## Developers

You can install this as an editable install in the standard way. Once you have your virtual environment setup for
the client application, activate it, then change to the directory where you have cloned the repository and run:

```
pip install --editable .
```
