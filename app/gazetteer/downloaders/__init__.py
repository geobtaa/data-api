"""
Gazetteer Downloaders

This package contains classes for downloading gazetteer data from various sources.
Each downloader is designed to handle a specific gazetteer format.
"""

from .base_downloader import BaseDownloader
from .fast_downloader import FastDownloader
from .geonames_downloader import GeonamesDownloader
from .wof_downloader import WofDownloader

__all__ = [
    "BaseDownloader",
    "GeonamesDownloader",
    "WofDownloader",
    "FastDownloader",
]
