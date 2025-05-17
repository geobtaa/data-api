import os
import sys
from pathlib import Path

# Set the correct database URL for migrations
os.environ["DATABASE_URL"] = "postgresql://postgres:postgres@localhost:2345/btaa_ogm_api"

# Import and run each migration
from db.migrations.create_gazetteer_tables import create_gazetteer_tables
from db.migrations.create_item_relationships import create_relationships_table
from db.migrations.create_fast_embeddings import create_fast_embeddings_table
from db.migrations.create_ai_enrichments import create_ai_enrichments_table
from db.migrations.add_enrichment_type import add_enrichment_type_column
from db.migrations.add_fast_gazetteer import add_fast_gazetteer
from db.migrations.update_fast_gazetteer import update_fast_gazetteer
from db.migrations.rename_ai_enrichments import rename_ai_enrichments_table
from db.migrations.rename_document_id_to_item_id import rename_document_id_to_item_id

def run_migrations():
    print("Running database migrations...")
    
    print("\nCreating gazetteer tables...")
    create_gazetteer_tables()
    
    print("\nCreating item relationships table...")
    create_relationships_table()
    
    print("\nCreating FAST embeddings table...")
    create_fast_embeddings_table()
    
    print("\nCreating AI enrichments table...")
    create_ai_enrichments_table()
    
    print("\nAdding enrichment type column...")
    add_enrichment_type_column()
    
    print("\nAdding FAST gazetteer...")
    add_fast_gazetteer()
    
    print("\nUpdating FAST gazetteer...")
    update_fast_gazetteer()
    
    print("\nRenaming AI enrichments table...")
    rename_ai_enrichments_table()
    
    print("\nRenaming document_id to item_id in item_ai_enrichments table...")
    rename_document_id_to_item_id()
    
    print("\nAll migrations completed successfully!")

if __name__ == "__main__":
    run_migrations() 