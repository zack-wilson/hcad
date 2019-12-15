from pkg_resources import DistributionNotFound, get_distribution

try:
    dist = get_distribution(__name__)
    __version__ = dist.version
except DistributionNotFound:
    __version__ = "unknown"
finally:
    del get_distribution, DistributionNotFound
