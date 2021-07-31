from typing import Any
import re

from src.utils.mindex import MultiIndex


def remove_lowercase(string: str) -> str:
    string = re.sub('[a-z]', '', string)
    return string


def nn(value: Any) -> bool:
    return value is not None


def convert_letters_to_string_numbers(string: str) -> str:
    new_string = ''
    for c in string:
        if c.isalpha():
            char = c.upper()
            char = str(ord(c) - 55)
            new_string += char
        else:
            new_string += c
    return new_string


def check_ticker(ticker: str) -> bool:
    if len(ticker) == 0: return False
    elif len(ticker) > 5: return False
    elif '-' in ticker: return False
    elif '.' in ticker: return False
    elif any([c.islower() for c in ticker]): return False
    else: return True


def validate_ticker(ticker: str) -> str:
    ticker = str(ticker)
    ticker = ticker.replace('-', '')
    ticker = ticker.replace('.', '')
    ticker = remove_lowercase(ticker)
    return ticker


def get_mapping_identifier_stats(mapping_index: MultiIndex) -> dict:
    count_dict = {
        'no_ciks': 0,
        'no_cusips': 0,
        'no_lei': 0,
        'no_figi': 0,
        'no_isin': 0
    }
    
    for obj in mapping_index:
        if 'cik' not in obj: count_dict['no_ciks'] += 1
        if 'cusip' not in obj: count_dict['no_cusips'] += 1
        if 'lei' not in obj: count_dict['no_lei'] += 1
        if 'figi' not in obj: count_dict['no_figi'] += 1
        if 'isin' not in obj: count_dict['no_isin'] += 1
    
    count_dict['no_ciks'] = count_dict['no_ciks'] / len(mapping_index)
    count_dict['no_cusips'] = count_dict['no_cusips'] / len(mapping_index)
    count_dict['no_lei'] = count_dict['no_lei'] / len(mapping_index)
    count_dict['no_figi'] = count_dict['no_figi'] / len(mapping_index)
    count_dict['no_isin'] = count_dict['no_isin'] / len(mapping_index)
    return count_dict
