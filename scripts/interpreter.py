import cachelib

from src.api.polygon import PolygonAPIConnector
from src.api.raf import RankAndFiledAPIConnector
from src.api.sec import SECAPIConnector
from src.api.secgov import SECGovAPIConnector
from src.api.gleif import GLEIFAPIConnector

redis = cachelib.RedisCache(
    host='localhost',
    port=7000,
    db=0,
    default_timeout=0,
    socket_keepalive=True
)