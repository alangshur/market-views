import re


def remove_lowercase(string: str) -> str:
    string = re.sub('[a-z]', '', string)
    return string


def validate_ticker(ticker: str) -> str:
    ticker = str(ticker)
    ticker = ticker.replace('-', '')
    ticker = ticker.replace('.', '')
    ticker = remove_lowercase(ticker)
    return ticker
    