from Rmoney_bhavcopy.Bhavcopy_Reteriver import get_CM_bhavcopy
from Rmoney_bhavcopy.Bhavcopy_Reteriver import get_FO_bhavcopy
import pytest
from datetime import datetime

def test_bhavcopy ():
    start_date = '2024-12-01'
    end_date = '2024-12-05'
    symbols = ['TCS']
    series = ['EQ']
    data = get_CM_bhavcopy(start_date, end_date, symbols, series)
    print(data)


def test_bhavcopy2 ():
    start_date = ''
    end_date = '2024-12-05'
    symbols = ['TCS', 'HDFCBANK', 'SBIN']
    series = ['EQ','BE']
    data2 = get_CM_bhavcopy(start_date, end_date, symbols, series)
    print(data2)

def test_bhavcopy3():
    end_date = 2024-12-5
    symbols = ['TCS', 'HDFCBANK']
    series = ['EQ']
    data3 = get_CM_bhavcopy(end_date, symbols,series)
    print(data3)

def test_bhavcopy4 ():
    start_date = ''
    end_date = ''
    symbols = ['']
    series = ['']
    data = get_CM_bhavcopy(start_date=start_date, end_date=end_date, symbols=symbols, series=series)
    print(data)

def test_bhavcopy5():
    start_date = datetime(2023,12,1)
    end_date = datetime(2023,12,10)
    symbols = ['BANKNIFTY']
    data = get_FO_bhavcopy(start_date,end_date,symbols)
    print(data)

