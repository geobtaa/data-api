#!/usr/bin/env python3
"""
Base Downloader for Gazetteer Data

This module provides a base class for gazetteer data downloaders to inherit from.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from abc import ABC, abstractmethod

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class BaseDownloader(ABC):
    """Base class for gazetteer data downloaders."""

    def __init__(self, data_dir=None, gazetteer_name=None):
        """
        Initialize the base downloader.

        Args:
            data_dir: Optional path to the data directory. If not provided, will use default.
            gazetteer_name: Name of the gazetteer. Required for subclasses.
        """
        if not gazetteer_name:
            raise ValueError("Gazetteer name must be provided")

        self.gazetteer_name = gazetteer_name

        # Project paths
        self.base_dir = Path(__file__).resolve().parent.parent.parent.parent
        self.data_dir = (
            Path(data_dir)
            if data_dir
            else self.base_dir / "data" / "gazetteers" / self.gazetteer_name
        )

        # Ensure the directory exists
        self.ensure_directories()

        # Statistics
        self.start_time = None
        self.end_time = None

    def ensure_directories(self):
        """Ensure all required directories exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured directory exists: {self.data_dir}")

    @abstractmethod
    def download(self):
        """Download gazetteer data. To be implemented by subclasses."""
        pass

    @abstractmethod
    def export(self):
        """Export downloaded data. To be implemented by subclasses."""
        pass

    def run(self, download=False, export=False, all=False):
        """
        Run the downloader operations based on provided options.

        Args:
            download: Whether to download data.
            export: Whether to export data (for downloaders that support this).
            all: Whether to perform all operations.
        """
        self.start_time = datetime.now()

        try:
            if all or download:
                self.download()

            if all or export:
                self.export()

            self.end_time = datetime.now()
            elapsed_time = (self.end_time - self.start_time).total_seconds()
            logger.info(f"All operations completed in {elapsed_time:.2f} seconds.")

            return {
                "status": "success",
                "gazetteer": self.gazetteer_name,
                "elapsed_time": elapsed_time,
            }

        except KeyboardInterrupt:
            logger.info("Operation interrupted by user.")
            return {
                "status": "interrupted",
                "gazetteer": self.gazetteer_name,
                "elapsed_time": (datetime.now() - self.start_time).total_seconds(),
            }
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {
                "status": "error",
                "gazetteer": self.gazetteer_name,
                "error": str(e),
                "elapsed_time": (datetime.now() - self.start_time).total_seconds(),
            }
