from bhavcopy.reterive import get_bhavcopy
import pytest

def test_bhavcopy ():


    start_date = '2022-12-01'
    end_date = '2024-12-05'
    symbols = ['TCS','HDFCBANK','SBIN']
    series = 'EQ'

    data = get_bhavcopy(start_date, end_date, symbols, series)
    assert(len(data)>0)
    print(data)
