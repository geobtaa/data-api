import os
import csv
import logging
from sqlalchemy import create_engine, insert, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from contextlib import asynccontextmanager

from db.config import DATABASE_URL

logger = logging.getLogger(__name__)

class BaseImporter:
    """Base class for gazetteer data importers."""
    
    # Default data directory
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'data', 'gazetteers')
    
    # Chunk size for batch inserts (adjust based on memory availability)
    CHUNK_SIZE = 5000
    
    def __init__(self, 
                 data_directory: Optional[str] = None, 
                 database_url: Optional[str] = None,
                 logger_name: Optional[str] = None):
        """
        Initialize the importer.
        
        Args:
            data_directory: Directory containing the CSV files.
            database_url: Database connection URL.
            logger_name: Custom logger name (defaults to the class module).
        """
        self.data_directory = data_directory or self.DATA_DIR
        self.database_url = database_url or DATABASE_URL
        
        # Convert for async use if needed
        self.async_database_url = None
        if self.database_url.startswith('postgresql://'):
            self.async_database_url = self.database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        
        # Configure logging
        if logger_name:
            self.logger = logging.getLogger(logger_name)
        else:
            self.logger = logger
        
        # Create SQLAlchemy engine
        self.engine = create_engine(self.database_url) if self.database_url else None
        
        # Other common attributes
        self.csv_files = []
        self.table = None
        self.table_name = None
        self.total_rows = 0
        self.processed_rows = 0
        self.errors = []
        
    def find_csv_files(self) -> List[str]:
        """Find CSV files in the specified directory."""
        if not os.path.exists(self.data_directory):
            self.logger.error(f"Data directory does not exist: {self.data_directory}")
            return []
        
        csv_files = []
        for root, _, files in os.walk(self.data_directory):
            for file in files:
                if file.lower().endswith('.csv'):
                    csv_files.append(os.path.join(root, file))
        
        self.csv_files = csv_files
        self.logger.info(f"Found {len(csv_files)} CSV files")
        return csv_files
    
    def read_csv(self, filepath: str, delimiter: str = ',', fieldnames: Optional[List[str]] = None) -> List[Dict]:
        """
        Read a CSV file and return a list of dictionaries.
        
        Args:
            filepath: Path to the CSV file.
            delimiter: CSV delimiter (default: comma).
            fieldnames: Optional list of field names (if not in the first row).
            
        Returns:
            List of dictionaries, each representing a row.
        """
        try:
            records = []
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=delimiter)
                for row in reader:
                    records.append(row)
            
            # If fieldnames were not provided, the first row is headers
            if not fieldnames and records:
                records = records[1:]
                
            return records
        except Exception as e:
            self.logger.error(f"Error reading CSV file {filepath}: {e}")
            self.errors.append({"file": filepath, "error": str(e)})
            return []
    
    def clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and transform a record before insertion.
        
        Args:
            record: The raw record from the CSV.
            
        Returns:
            Cleaned record ready for database insertion.
        """
        # Default implementation - add created_at and updated_at timestamps
        now = datetime.now()
        record['created_at'] = now
        record['updated_at'] = now
        
        # Remove empty strings or convert to None
        for key, value in record.items():
            if value == '':
                record[key] = None
        
        return record
    
    def chunk_data(self, data: List[Dict], chunk_size: int = None) -> List[List[Dict]]:
        """
        Split a large list of records into smaller chunks.
        
        Args:
            data: List of records.
            chunk_size: Size of each chunk.
            
        Returns:
            List of chunks, each containing a subset of records.
        """
        chunk_size = chunk_size or self.CHUNK_SIZE
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
    async def import_data(self) -> Dict[str, Any]:
        """
        Import data from CSV files to the database.
        This is the main method that should be implemented by subclasses.
        
        Returns:
            Dictionary with import statistics.
        """
        raise NotImplementedError("Subclasses must implement this method")
    
    async def truncate_table(self, table_name: str) -> bool:
        """
        Truncate the specified table (delete all rows).
        
        Args:
            table_name: Name of the table to truncate.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            if not self.async_database_url:
                self.logger.error("Async database URL not configured")
                return False
            
            async_engine = create_async_engine(self.async_database_url)
            
            @asynccontextmanager
            async def get_session():
                async with AsyncSession(async_engine) as session:
                    try:
                        yield session
                        await session.commit()
                    except Exception as e:
                        await session.rollback()
                        raise e
            
            async with get_session() as session:
                await session.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            
            self.logger.info(f"Truncated table: {table_name}")
            return True
        
        except Exception as e:
            self.logger.error(f"Error truncating table {table_name}: {e}")
            self.errors.append({"table": table_name, "error": str(e)})
            return False
    
    async def bulk_insert(self, table, records: List[Dict]) -> int:
        """
        Perform a bulk insert operation.
        
        Args:
            table: SQLAlchemy table object.
            records: List of records to insert.
            
        Returns:
            Number of successfully inserted records.
        """
        if not records:
            return 0
        
        try:
            if not self.async_database_url:
                self.logger.error("Async database URL not configured")
                return 0
            
            async_engine = create_async_engine(self.async_database_url)
            
            @asynccontextmanager
            async def get_session():
                async with AsyncSession(async_engine) as session:
                    try:
                        yield session
                        await session.commit()
                    except Exception as e:
                        await session.rollback()
                        raise e
            
            count = 0
            for chunk in self.chunk_data(records):
                async with get_session() as session:
                    stmt = insert(table).values(chunk)
                    await session.execute(stmt)
                    count += len(chunk)
            
            return count
        
        except Exception as e:
            self.logger.error(f"Error during bulk insert: {e}")
            self.errors.append({"operation": "bulk_insert", "error": str(e)})
            return 0
    
    async def upsert(self, table, records: List[Dict], constraint_columns: List[str]) -> int:
        """
        Perform an upsert operation (insert or update).
        
        Args:
            table: SQLAlchemy table object.
            records: List of records to upsert.
            constraint_columns: List of column names that form the unique constraint.
            
        Returns:
            Number of successfully upserted records.
        """
        if not records:
            return 0
        
        try:
            if not self.async_database_url:
                self.logger.error("Async database URL not configured")
                return 0
            
            async_engine = create_async_engine(self.async_database_url)
            
            @asynccontextmanager
            async def get_session():
                async with AsyncSession(async_engine) as session:
                    try:
                        yield session
                        await session.commit()
                    except Exception as e:
                        await session.rollback()
                        raise e
            
            count = 0
            for chunk in self.chunk_data(records):
                async with get_session() as session:
                    # Create a PostgreSQL-specific upsert statement
                    stmt = pg_insert(table).values(chunk)
                    
                    # Determine which columns to update
                    update_dict = {
                        c.name: stmt.excluded[c.name]
                        for c in table.columns
                        if c.name not in constraint_columns and c.name != 'created_at'
                    }
                    
                    # Always update updated_at timestamp
                    update_dict['updated_at'] = datetime.now()
                    
                    # Create the "ON CONFLICT DO UPDATE" clause
                    on_conflict_stmt = stmt.on_conflict_do_update(
                        constraint=constraint_columns,
                        set_=update_dict
                    )
                    
                    await session.execute(on_conflict_stmt)
                    count += len(chunk)
            
            return count
        
        except Exception as e:
            self.logger.error(f"Error during upsert: {e}")
            self.errors.append({"operation": "upsert", "error": str(e)})
            return 0 