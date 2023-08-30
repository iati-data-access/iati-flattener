import datetime
import math


def get_date(date_str: str):
    """Returns a datetime.date object from a given string.

    :param :
    :return: date object corresponding to the string specified
    :rtype: datetime.date
    """

    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()


def get_fy_fq(_date):
    date = get_date(_date)
    return date.year, "Q{}".format(math.ceil(date.month/3))


def get_fy_fq_numeric(date_obj: datetime.date):
    """Given a date it returns the year and the quarter as a tuple

    :param date_obj: the date to convert
    :type date_obj: datetime.date
    :return: a tuple with year and quarter
    :rtype: (year, quarter)"""

    return date_obj.year, math.ceil(date_obj.month / 3)


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
