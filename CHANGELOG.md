# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [0.10.11] - 2023-06-13

### Fixed

- Output of Excel files altered so that an Excel file is created even when there
  is no data. This ensures no un-registered datasets are included in the output.


## [0.10.9] - 2023-11-14

### Added

- Several unit tests have been added to cover budget and transaction calculations.

### Fixed

- Bug in calculation of value_local for some transactions: https://github.com/iati-data-access/iati-flattener/issues/9
- Budget splitting: budgets are now split by calculating a day rate, which is then used
to work out the amount for each budget period. https://github.com/iati-data-access/iati-flattener/issues/16

## [0.10.8] - 2023-08-31

### Fixed

- Date quarters: incorrect start dates of quarters: https://github.com/iati-data-access/iati-flattener/issues/7
- Budget splitting: budgets were not split correctly across quarters when budget periods spanned calendar years:
https://github.com/iati-data-access/iati-flattener/issues/8


