# Scripts Documentation

This document provides an overview of the utility scripts available in the project.

## Overview

The `scripts/` directory contains various utility scripts for managing the application's data, testing functionality, and performing maintenance tasks.

## Available Scripts

### 1. `process_allmaps.py`

**Purpose**: Processes and generates Allmaps annotations for items in the database.

**Key Features**:
- Processes individual items or all items in the database
- Generates Allmaps IDs and annotations
- Updates item records with Allmaps attributes
- Supports reprocessing of existing items
- Implements logging and error handling

**Usage**:
```bash
# Process a specific item
python process_allmaps.py --item-id "9139578d-7803-4f4f-9ed3-a62ab810a256"

# Process all items
python process_allmaps.py --all
```

**Requirements**:
- Database connection must be properly configured
- Item must have a valid manifest URL
- Item must not already have Allmaps attributes (unless reprocessing)

**Output**:
- Updates the `item_allmaps` table with generated Allmaps data
- Logs processing status and any errors encountered

### 2. `populate_relationships.py`

**Purpose**: Manages and populates relationship data between documents in the database.

**Key Features**:
- Processes various types of document relationships (isPartOf, hasMember, isVersionOf, etc.)
- Maintains bidirectional relationships
- Clears existing relationships before populating new ones
- Implements logging to both console and file

**Usage**:
```bash
python scripts/populate_relationships.py
```

### 3. `generate_fast_embeddings.py`

**Purpose**: Generates and stores embeddings for FAST gazetteer data using OpenAI's API.

**Key Features**:
- Uses OpenAI's text-embedding-3-small model
- Processes records in batches
- Stores embeddings in the database
- Implements error handling and logging

**Requirements**:
- OpenAI API key must be set in environment variables

**Usage**:
```bash
python scripts/generate_fast_embeddings.py
```

### 4. `run_migration.py`

**Purpose**: Executes database migrations.

**Key Features**:
- Supports multiple migration types
- Implements command-line argument parsing
- Provides logging of migration progress

**Available Migrations**:
- `add_fast_gazetteer`: Adds FAST gazetteer data to the database

**Usage**:
```bash
python scripts/run_migration.py add_fast_gazetteer
```

### 5. `import_fast.py`

**Purpose**: Imports OCLC FAST Dataset Geographic entries into the database.

**Key Features**:
- Asynchronous data import
- Progress tracking and reporting
- Error handling and logging
- Performance metrics (records processed, elapsed time)

**Usage**:
```bash
python scripts/import_fast.py
```

### 6. `clear_cache.py`

**Purpose**: Clears the Redis cache used by the application.

**Key Features**:
- Clears all Redis databases
- Reports memory usage after clearing
- Configurable Redis connection parameters
- Error handling and logging

**Usage**:
```bash
python scripts/clear_cache.py
```

### 7. `test_gazetteer_api.py`

**Purpose**: Tests the functionality of gazetteer API endpoints.

**Key Features**:
- Tests multiple gazetteer sources (GeoNames, Who's on First, BTAA)
- Provides detailed output of test results
- Configurable base URL for testing different environments
- Pretty-prints JSON responses

**Usage**:
```bash
python scripts/test_gazetteer_api.py [--base-url URL]
```

## Common Features

All scripts share some common features:
- Logging configuration
- Error handling
- Environment variable support
- Python path configuration for module imports

## Environment Variables

Several scripts require specific environment variables:

- `OPENAI_API_KEY`: Required by `generate_fast_embeddings.py`
- `REDIS_HOST` and `REDIS_PORT`: Used by `clear_cache.py`
- `LOG_PATH`: Optional path for log files

## Logging

All scripts implement logging with the following characteristics:
- Log level: INFO by default
- Format: Timestamp, logger name, level, and message
- Output: Console and/or file depending on the script 