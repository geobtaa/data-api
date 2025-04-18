"""
Gazetteer Downloaders

This package contains classes for downloading gazetteer data from various sources.
Each downloader is designed to handle a specific gazetteer format.
"""

from .base_downloader import BaseDownloader
from .geonames_downloader import GeoNamesDownloader
from .wof_downloader import WofDownloader

__all__ = ["BaseDownloader", "WofDownloader", "GeoNamesDownloader"]
