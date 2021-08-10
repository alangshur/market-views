

def parse_price(price: float) -> float:
    assert price > 0, 'invalid price'
    price = float(price)
    price = round(price, 4)
    return price