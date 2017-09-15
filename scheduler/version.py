from pkg_resources import DistributionNotFound, get_distribution


def get_version()->str:
    try:
        return get_distribution('simple_drmaa_scheduler').version
    except DistributionNotFound:
        return '<unknown>'
