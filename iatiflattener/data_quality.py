from pyexcelerate import Workbook
import pandas as pd
import datetime
import os
import json
import openpyxl

def report():
    def write_dataframe_to_excel(_dataframe, filename):
        """
        Function to write a pandas dataframe to Excel.
        This uses pyExcelerate, which is about 2x as fast as the built-in
        pandas library.
        """
        def fix_h(_h):
            h = str(_h)
            if h.startswith("budget"): return "Budget"
            return h

        def make_header(header):
            header = list(filter(lambda h: h!='', header))
            if type(header) in (list, tuple):
                return " - ".join([fix_h(h) for h in header])
            return header

        wb = Workbook()
        dataframe = _dataframe.reset_index()
        headers = list(map(lambda header: make_header(header), dataframe.columns.values))
        data = dataframe.values.tolist()
        data.insert(0, headers)
        ws = wb.new_sheet("Data", data=data)
        wb.save(filename)

    def one_report(filename, values):
        df = pd.read_excel(os.path.join('output', 'xlsx', 'en', filename), engine='openpyxl')
        if not "IATI Identifier" in df.columns.values:
            return values
        this_year = datetime.datetime.now().year
        required_years = list(range(this_year-2, this_year+3))
        df = df[df["Calendar Year"].isin(required_years)]
        out = df.fillna("").groupby([
                   'Reporting Organisation',
                   'Reporting Organisation Type',
                   'Transaction Type',
                   'Calendar Year',
                   'Calendar Quarter'])
        out = out["Value (USD)"].agg("sum").reset_index()
        values += out.values.tolist()
        return values

    def all_reports():
        xlsx_files = os.listdir(os.path.join('output', 'xlsx', 'en'))
        xlsx_files.sort()
        headers = ['Reporting Organisation',
                   'Reporting Organisation Type',
                   'Transaction Type',
                   'Calendar Year',
                   'Calendar Quarter',
                   'Value (USD)']
        values = []
        for xlsx_file in xlsx_files:
            if xlsx_file.endswith(".xlsx"):
                #try:
                print("Summarising {}".format(xlsx_file))
                values = one_report(xlsx_file, values)
                #except Exception:
                #    print("Exception with file {}".format(xlsx_file))
        df = pd.DataFrame(values, columns=headers)
        year_summaries = df.pivot_table(
            index=['Reporting Organisation', 'Reporting Organisation Type'],
            columns=['Transaction Type',
            'Calendar Year'],
            values='Value (USD)',
            aggfunc=sum).fillna(0.0)
        write_dataframe_to_excel(year_summaries, os.path.join('output', 'xlsx', 'summary_year.xlsx'))
        year_summaries = dict(map(lambda org: (org[0][0], {
            'type': org[0][1],
            'data': list(map(lambda year_transaction_type: {
                'transaction_type': year_transaction_type[0][0],
                'year': year_transaction_type[0][1],
                'value': year_transaction_type[1]
            }, org[1].items()))
        }), year_summaries.to_dict(orient='index').items()))

        with open(os.path.join('output', 'xlsx', 'summary_year.json'), 'w') as json_file:
            json.dump({
                'summary': year_summaries
            }, json_file)

        last_year = datetime.datetime.now().year-1
        df = pd.DataFrame(values, columns=headers)
        quarter_summaries = df[df["Calendar Year"]==last_year].pivot_table(
            index=['Reporting Organisation', 'Reporting Organisation Type'],
            columns=['Transaction Type',
            'Calendar Year', 'Calendar Quarter'],
            values='Value (USD)',
            aggfunc=sum).fillna(0.0)
        write_dataframe_to_excel(quarter_summaries, os.path.join('output', 'xlsx', 'summary_quarter.xlsx'))
        quarter_summaries = dict(map(lambda org: (org[0][0], {
            'type': org[0][1],
            'data': list(map(lambda year_transaction_type: {
                'transaction_type': year_transaction_type[0][0],
                'year': year_transaction_type[0][1],
                'value': year_transaction_type[1]
            }, org[1].items()))
        }), quarter_summaries.to_dict(orient='index').items()))
        with open(os.path.join('output', 'xlsx', 'summary_quarter.json'), 'w') as json_file:
            json.dump({
                'summary': quarter_summaries
            }, json_file)
    all_reports()
