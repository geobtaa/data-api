import os
import sys
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from databases import Database
from elasticsearch import AsyncElasticsearch
import json

# Add the project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import test settings before importing app
from tests.test_settings import *  # This sets up the environment variables 

@pytest.fixture(autouse=True)
async def setup_test_db():
    """Setup test database and Elasticsearch."""
    # Setup database
    database = Database(os.getenv("DATABASE_URL"))
    await database.connect()
    
    # Setup Elasticsearch
    es = AsyncElasticsearch(os.getenv("ELASTICSEARCH_URL"))
    index_name = os.getenv("ELASTICSEARCH_INDEX")
    
    # Create test index with mappings
    if await es.indices.exists(index=index_name):
        await es.indices.delete(index=index_name)
    
    # Define mappings based on real document structure
    mappings = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "dct_title_s": {"type": "text"},
                "dct_alternative_sm": {"type": "keyword"},
                "dct_description_sm": {"type": "text"},
                "dct_language_sm": {"type": "keyword"},
                "gbl_displaynote_sm": {"type": "keyword"},
                "dct_creator_sm": {"type": "keyword"},
                "dct_publisher_sm": {"type": "keyword"},
                "schema_provider_s": {"type": "keyword"},
                "gbl_resourceclass_sm": {"type": "keyword"},
                "gbl_resourcetype_sm": {"type": "keyword"},
                "dct_subject_sm": {"type": "keyword"},
                "dcat_theme_sm": {"type": "keyword"},
                "dcat_keyword_sm": {"type": "keyword"},
                "dct_temporal_sm": {"type": "keyword"},
                "dct_issued_s": {"type": "keyword"},
                "gbl_indexyear_im": {"type": "keyword"},
                "gbl_daterange_drsim": {"type": "keyword"},
                "dct_spatial_sm": {"type": "keyword"},
                "locn_geometry": {"type": "geo_shape"},
                "dcat_bbox": {"type": "keyword"},
                "dcat_centroid": {"type": "keyword"},
                "dct_relation_sm": {"type": "keyword"},
                "pcdm_memberof_sm": {"type": "keyword"},
                "dct_ispartof_sm": {"type": "keyword"},
                "dct_source_sm": {"type": "keyword"},
                "dct_isversionof_sm": {"type": "keyword"},
                "dct_replaces_sm": {"type": "keyword"},
                "dct_isreplacedby_sm": {"type": "keyword"},
                "dct_rights_sm": {"type": "keyword"},
                "dct_rightsholder_sm": {"type": "keyword"},
                "dct_license_sm": {"type": "keyword"},
                "dct_accessrights_s": {"type": "keyword"},
                "dct_format_s": {"type": "keyword"},
                "gbl_filesize_s": {"type": "keyword"},
                "gbl_wxsidentifier_s": {"type": "keyword"},
                "dct_references_s": {"type": "keyword"},
                "dct_identifier_sm": {"type": "keyword"},
                "gbl_mdmodified_dt": {"type": "date"},
                "gbl_mdversion_s": {"type": "keyword"},
                "gbl_suppressed_b": {"type": "boolean"},
                "gbl_georeferenced_b": {"type": "boolean"},
                "searchable_text": {"type": "text"}
            }
        }
    }
    
    # Create index with mappings
    await es.indices.create(index=index_name, body=mappings)
    
    # Index test document using the real structure
    test_doc = {
        "id": "p16022coll230:2910",
        "dct_title_s": "A new description of Kent ...",
        "dct_description_sm": "Relief shown pictorially.; Includes tables and text.",
        "dct_language_sm": "eng",
        "dct_creator_sm": "Symonson, Philip",
        "dct_publisher_sm": "Ordnance Survey Office (Southampton)",
        "schema_provider_s": "University of Minnesota",
        "gbl_resourceclass_sm": "Maps",
        "dcat_keyword_sm": "2022-creator-sprint",
        "dct_temporal_sm": "1914",
        "dct_issued_s": "1914",
        "gbl_indexyear_im": "1914",
        "gbl_daterange_drsim": "[1914 TO 1914]",
        "dct_spatial_sm": "Europe",
        "locn_geometry": "POLYGON((-0.117 51.500, 1.450 51.500, 1.450 50.867, -0.117 50.867, -0.117 51.500))",
        "dcat_bbox": "ENVELOPE(-0.117,1.450,51.500,50.867)",
        "dcat_centroid": "51.183499999999995,0.6665",
        "pcdm_memberof_sm": "64bd8c4c-8e60-4956-b43d-bdc3f93db488",
        "dct_ispartof_sm": "05d-01,p16022coll230",
        "dct_rights_sm": "Use of this item may be governed by US and international copyright laws.",
        "dct_accessrights_s": "Public",
        "dct_format_s": "JPEG",
        "dct_references_s": json.dumps({
            "http://iiif.io/api/image": "https://cdm16022.contentdm.oclc.org/digital/iiif/p16022coll230/2910/info.json",
            "http://schema.org/url": "https://umedia.lib.umn.edu/item/p16022coll230:2910",
            "http://iiif.io/api/presentation#manifest": "https://cdm16022.contentdm.oclc.org/iiif/info/p16022coll230/2910/manifest.json"
        }),
        "dct_identifier_sm": "UMN_ALMA:9944916610001701",
        "gbl_mdversion_s": "Aardvark"
    }
    
    await es.index(index=index_name, id=test_doc["id"], document=test_doc)
    await es.indices.refresh(index=index_name)
    
    # Create database tables and insert test data
    # Drop table if exists to ensure clean state
    await database.execute("DROP TABLE IF EXISTS geoblacklight_development")
    
    # Create tables with complete schema
    query = text("""
        CREATE TABLE geoblacklight_development (
            id TEXT PRIMARY KEY,
            dct_title_s TEXT,
            dct_alternative_sm TEXT,
            dct_description_sm TEXT,
            dct_language_sm TEXT,
            dct_creator_sm TEXT,
            dct_publisher_sm TEXT,
            schema_provider_s TEXT,
            gbl_resourceClass_sm TEXT,
            dct_subject_sm TEXT,
            dcat_theme_sm TEXT,
            dcat_keyword_sm TEXT,
            dct_temporal_sm TEXT,
            dct_issued_s TEXT,
            gbl_indexYear_im TEXT,
            dct_spatial_sm TEXT,
            locn_geometry TEXT,
            dct_references_s TEXT,
            gbl_mdModified_dt TIMESTAMP,
            gbl_mdVersion_s TEXT,
            gbl_suppressed_b INTEGER,
            b1g_image_ss TEXT,
            b1g_dct_accrualMethod_s TEXT,
            b1g_dct_accrualPeriodicity_s TEXT,
            gbl_displayNote_sm TEXT,
            gbl_resourceType_sm TEXT,
            dct_format_s TEXT,
            gbl_wxsIdentifier_s TEXT,
            dct_accessRights_s TEXT,
            dct_rights_sm TEXT,
            dct_license_sm TEXT,
            dct_rightsHolder_sm TEXT,
            foaf_primaryTopic_sm TEXT,
            pcdm_memberOf_sm TEXT,
            schema_additionalType_sm TEXT,
            schema_isPartOf_s TEXT,
            schema_url_s TEXT,
            schema_thumbnailUrl_s TEXT,
            aardvark_specs_version_s TEXT,
            gbl_dateRange_drsim TEXT,
            b1g_status_s TEXT,
            b1g_code_s TEXT,
            b1g_dateAccessioned_s TEXT,
            b1g_dateRetired_s TEXT,
            dcat_bbox TEXT,
            dcat_centroid TEXT,
            gbl_georeferenced_b INTEGER,
            gbl_fileSize_s TEXT,
            dct_relation_sm TEXT,
            dct_isVersionOf_sm TEXT,
            dct_replaces_sm TEXT,
            dct_isReplacedBy_sm TEXT,
            dct_source_sm TEXT,
            dct_isRequiredBy_sm TEXT,
            dct_requires_sm TEXT,
            dct_isPartOf_sm TEXT,
            dct_hasPart_sm TEXT,
            dct_conformsTo_sm TEXT,
            dct_identifier_sm TEXT,
            dct_type_sm TEXT,
            dct_accrualMethod_s TEXT,
            dct_accrualPeriodicity_s TEXT,
            dct_accrualPolicy_s TEXT,
            dct_available_s TEXT,
            dct_bibliographicCitation_sm TEXT,
            dct_contributor_sm TEXT,
            dct_coverage_sm TEXT,
            dct_dateAccepted_s TEXT,
            dct_dateCopyrighted_s TEXT,
            dct_dateSubmitted_s TEXT,
            dct_educationLevel_sm TEXT,
            dct_extent_sm TEXT,
            dct_hasFormat_sm TEXT,
            dct_hasVersion_sm TEXT,
            dct_instructionalMethod_sm TEXT,
            dct_isFormatOf_sm TEXT,
            dct_isReferencedBy_sm TEXT,
            dct_medium_sm TEXT,
            dct_modified_s TEXT,
            dct_provenance_sm TEXT,
            dct_references_sm TEXT,
            dct_tableOfContents_sm TEXT,
            dct_valid_sm TEXT
        )
    """)
    await database.execute(query=str(query))
    
    # Insert test data
    query = text("""
        INSERT INTO geoblacklight_development (
            id,
            dct_title_s,
            dct_description_sm,
            dct_creator_sm,
            dct_publisher_sm,
            schema_provider_s,
            dct_references_s,
            locn_geometry,
            gbl_mdModified_dt,
            gbl_resourceClass_sm,
            gbl_resourceType_sm,
            dct_format_s,
            gbl_displayNote_sm,
            gbl_dateRange_drsim,
            b1g_status_s,
            dct_temporal_sm,
            dcat_bbox,
            dcat_centroid,
            gbl_georeferenced_b,
            gbl_fileSize_s,
            dct_relation_sm,
            dct_source_sm,
            dct_isPartOf_sm,
            dct_identifier_sm,
            dct_type_sm
        ) VALUES (
            'test-123',
            'Test Document',
            'A test description',
            'Test Creator',
            'Test Publisher',
            'Test Provider',
            '{"http://schema.org/url": "http://example.com"}',
            'ENVELOPE(-88.0, -87.0, 42.0, 41.0)',
            CURRENT_TIMESTAMP,
            'Maps',
            'Physical Map',
            'Paper',
            'Test display note',
            '[1990 TO 2000]',
            'Active',
            '1990-2000',
            'ENVELOPE(-88.0, -87.0, 42.0, 41.0)',
            'POINT(-87.5 41.5)',
            1,
            '1.5 MB',
            'Related Document 1',
            'Original Source',
            'Parent Collection',
            'test-123-identifier',
            'Dataset'
        )
    """)
    await database.execute(query=str(query))
    
    yield database
    
    # Cleanup
    await es.indices.delete(index=index_name)
    await es.close()
    await database.execute("DROP TABLE IF EXISTS geoblacklight_development")
    await database.disconnect() 