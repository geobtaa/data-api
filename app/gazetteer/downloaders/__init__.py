"""
Gazetteer Downloaders

This package contains classes for downloading gazetteer data from various sources.
Each downloader is designed to handle a specific gazetteer format.
"""

from .base_downloader import BaseDownloader
from .wof_downloader import WofDownloader
from .geonames_downloader import GeoNamesDownloader

__all__ = ["BaseDownloader", "WofDownloader", "GeoNamesDownloader"]
