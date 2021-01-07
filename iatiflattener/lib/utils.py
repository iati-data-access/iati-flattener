import datetime
import math


def get_date(_date):
    return datetime.datetime.strptime(_date, "%Y-%m-%d").date()


def get_fy_fq(_date):
    date = get_date(_date)
    return date.year, "Q{}".format(math.ceil(date.month/3))


def get_fy_fq_numeric(_date):
    return _date.year, math.ceil(_date.month/3)


def get_first(args, default=None):
    for arg in args:
        if arg not in [None, []]: return arg
    return default


def float_int_string(_item):
    try:
        return str(int(_item))
    except ValueError:
        return str(_item)


def get_if_exists(_from, _item):
    try:
        _item = str(int(_item))
    except ValueError:
        _item = str(_item)
    return _from.get(_item, "")
