import psycopg2
import pandas as pd
import os
from tqdm import tqdm

# List available CSV files in the fixtures directory
fixtures_dir = "fixtures"
csv_files = [f for f in os.listdir(fixtures_dir) if f.endswith(".csv")]

if not csv_files:
    print("No CSV files found in the fixtures directory.")
    exit()

print("Available CSV files:")
for i, file in enumerate(csv_files, start=1):
    print(f"{i}: {file}")

# Prompt the user to select a file
file_choice = int(input("Enter the number of the file you want to import: ")) - 1

if file_choice < 0 or file_choice >= len(csv_files):
    print("Invalid choice.")
    exit()

# Read the selected CSV file
csv_file_path = os.path.join(fixtures_dir, csv_files[file_choice])
print(f"Selected file: {csv_file_path}")

data = pd.read_csv(csv_file_path, low_memory=False)

# Connect to PostgreSQL using environment variables
conn = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB", "geoblacklight_development"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    host=os.getenv("POSTGRES_HOST", "paradedb"),  # Use the Docker service name
    port=os.getenv("POSTGRES_PORT", "5432"),  # Use the internal Docker port
)
cursor = conn.cursor()

# Drop indexes before import
drop_indexes_query = """
DROP INDEX IF EXISTS idx_dct_title_s;
DROP INDEX IF EXISTS idx_dct_alternative_sm;
-- Add more DROP INDEX statements for other indexes as needed
"""
cursor.execute(drop_indexes_query)
conn.commit()

# Create table without the generated column first
create_table_query = """
CREATE TABLE IF NOT EXISTS geoblacklight_development (
    id VARCHAR PRIMARY KEY,
    dct_title_s VARCHAR,
    dct_alternative_sm VARCHAR[],
    dct_description_sm TEXT[],
    dct_language_sm VARCHAR[],
    gbl_displayNote_sm TEXT[],
    dct_creator_sm VARCHAR[],
    dct_publisher_sm VARCHAR[],
    schema_provider_s VARCHAR,
    gbl_resourceClass_sm VARCHAR[],
    gbl_resourceType_sm VARCHAR[],
    dct_subject_sm VARCHAR[],
    dcat_theme_sm VARCHAR[],
    dcat_keyword_sm VARCHAR[],
    dct_temporal_sm VARCHAR[],
    dct_issued_s VARCHAR,
    gbl_indexYear_im INTEGER[],
    gbl_dateRange_drsim VARCHAR[],
    dct_spatial_sm VARCHAR[],
    locn_geometry TEXT,
    dcat_bbox VARCHAR,
    dcat_centroid VARCHAR,
    dct_relation_sm VARCHAR[],
    pcdm_memberOf_sm VARCHAR[],
    dct_isPartOf_sm VARCHAR[],
    dct_source_sm VARCHAR[],
    dct_isVersionOf_sm VARCHAR[],
    dct_replaces_sm VARCHAR[],
    dct_isReplacedBy_sm VARCHAR[],
    dct_rights_sm VARCHAR[],
    dct_rightsHolder_sm VARCHAR[],
    dct_license_sm TEXT[],
    dct_accessRights_s VARCHAR,
    dct_format_s VARCHAR,
    gbl_fileSize_s VARCHAR,
    gbl_wxsIdentifier_s VARCHAR,
    dct_references_s TEXT,
    dct_identifier_sm VARCHAR[],
    gbl_mdModified_dt TIMESTAMP,
    gbl_mdVersion_s VARCHAR,
    gbl_suppressed_b BOOLEAN,
    gbl_georeferenced_b BOOLEAN
);
"""
cursor.execute(create_table_query)
conn.commit()

# Function to convert pipe-delimited strings to arrays
def convert_to_array(value):
    if pd.isna(value):
        return None
    return value.split("|")

# Function to convert pipe-delimited strings to integer arrays
def convert_to_int_array(value):
    if pd.isna(value):
        return None
    try:
        return [int(x) for x in value.split("|") if x.strip().isdigit()]
    except (ValueError, AttributeError):
        return None

# Function to handle NaN values
def handle_nan(value):
    if pd.isna(value):
        return None
    return value

# Function to convert string boolean values to actual booleans
def convert_to_boolean(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)

# Define the insert query with ON CONFLICT to skip duplicates
insert_query = """
INSERT INTO geoblacklight_development (
    id, dct_title_s, dct_alternative_sm, dct_description_sm, dct_language_sm,
    gbl_displayNote_sm, dct_creator_sm, dct_publisher_sm, schema_provider_s,
    gbl_resourceClass_sm, gbl_resourceType_sm, dct_subject_sm, dcat_theme_sm,
    dcat_keyword_sm, dct_temporal_sm, dct_issued_s, gbl_indexYear_im,
    gbl_dateRange_drsim, dct_spatial_sm, locn_geometry, dcat_bbox, dcat_centroid,
    dct_relation_sm, pcdm_memberOf_sm, dct_isPartOf_sm, dct_source_sm,
    dct_isVersionOf_sm, dct_replaces_sm, dct_isReplacedBy_sm, dct_rights_sm,
    dct_rightsHolder_sm, dct_license_sm, dct_accessRights_s, dct_format_s,
    gbl_fileSize_s, gbl_wxsIdentifier_s, dct_references_s, dct_identifier_sm,
    gbl_mdModified_dt, gbl_mdVersion_s, gbl_suppressed_b, gbl_georeferenced_b
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
) ON CONFLICT (id) DO NOTHING;
"""

# Insert data into the table with batch processing
batch_size = 1000  # Adjust the batch size as needed
batch = []

print("Inserting records into the database...")
for _, row in tqdm(data.iterrows(), total=len(data)):
    batch.append(
        (
            row["id"],
            handle_nan(row["dct_title_s"]),
            convert_to_array(row["dct_alternative_sm"]),
            convert_to_array(row["dct_description_sm"]),
            convert_to_array(row["dct_language_sm"]),
            convert_to_array(row["gbl_displayNote_sm"]),
            convert_to_array(row["dct_creator_sm"]),
            convert_to_array(row["dct_publisher_sm"]),
            handle_nan(row["schema_provider_s"]),
            convert_to_array(row["gbl_resourceClass_sm"]),
            convert_to_array(row["gbl_resourceType_sm"]),
            convert_to_array(row["dct_subject_sm"]),
            convert_to_array(row["dcat_theme_sm"]),
            convert_to_array(row["dcat_keyword_sm"]),
            convert_to_array(row["dct_temporal_sm"]),
            handle_nan(row["dct_issued_s"]),
            convert_to_int_array(row["gbl_indexYear_im"]),
            convert_to_array(row["gbl_dateRange_drsim"]),
            convert_to_array(row["dct_spatial_sm"]),
            handle_nan(row["locn_geometry"]),
            handle_nan(row["dcat_bbox"]),
            handle_nan(row["dcat_centroid"]),
            convert_to_array(row["dct_relation_sm"]),
            convert_to_array(row["pcdm_memberOf_sm"]),
            convert_to_array(row["dct_isPartOf_sm"]),
            convert_to_array(row["dct_source_sm"]),
            convert_to_array(row["dct_isVersionOf_sm"]),
            convert_to_array(row["dct_replaces_sm"]),
            convert_to_array(row["dct_isReplacedBy_sm"]),
            convert_to_array(row["dct_rights_sm"]),
            convert_to_array(row["dct_rightsHolder_sm"]),
            convert_to_array(row["dct_license_sm"]),
            handle_nan(row["dct_accessRights_s"]),
            handle_nan(row["dct_format_s"]),
            handle_nan(row["gbl_fileSize_s"]),
            handle_nan(row["gbl_wxsIdentifier_s"]),
            handle_nan(row["dct_references_s"]),
            convert_to_array(row["dct_identifier_sm"]),
            handle_nan(row["gbl_mdModified_dt"]),
            handle_nan(row["gbl_mdVersion_s"]),
            convert_to_boolean(row["gbl_suppressed_b"]),
            convert_to_boolean(row["gbl_georeferenced_b"]),
        )
    )

    if len(batch) >= batch_size:
        cursor.executemany(insert_query, batch)
        conn.commit()
        batch = []

# Insert any remaining records
if batch:
    cursor.executemany(insert_query, batch)
    conn.commit()

print("Data insertion complete.")

# Recreate indexes after import
create_indexes_query = """
CREATE INDEX idx_dct_title_s ON geoblacklight_development (dct_title_s);
CREATE INDEX idx_dct_alternative_sm ON geoblacklight_development USING gin (dct_alternative_sm);
-- Add more CREATE INDEX statements for other indexes as needed
"""
cursor.execute(create_indexes_query)
conn.commit()

# Close the connection
cursor.close()
conn.close()
