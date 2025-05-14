from sqlalchemy import (
    ARRAY,
    JSON,
    TIMESTAMP,
    BigInteger,
    Boolean,
    Column,
    Date,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
)

metadata = MetaData()

items = Table(
    "items",
    metadata,
    Column("id", String, primary_key=True),
    Column("dct_title_s", String),
    Column("dct_alternative_sm", ARRAY(String)),
    Column("dct_description_sm", ARRAY(Text)),
    Column("dct_language_sm", ARRAY(String)),
    Column("gbl_displaynote_sm", ARRAY(Text)),
    Column("dct_creator_sm", ARRAY(String)),
    Column("dct_publisher_sm", ARRAY(String)),
    Column("schema_provider_s", String),
    Column("gbl_resourceclass_sm", ARRAY(String)),
    Column("gbl_resourcetype_sm", ARRAY(String)),
    Column("dct_subject_sm", ARRAY(String)),
    Column("dcat_theme_sm", ARRAY(String)),
    Column("dcat_keyword_sm", ARRAY(String)),
    Column("dct_temporal_sm", ARRAY(String)),
    Column("dct_issued_s", String),
    Column("gbl_indexyear_im", ARRAY(Integer)),
    Column("gbl_daterange_drsim", ARRAY(String)),
    Column("dct_spatial_sm", ARRAY(String)),
    Column("locn_geometry", Text),
    Column("dcat_bbox", String),
    Column("dcat_centroid", String),
    Column("dct_relation_sm", ARRAY(String)),
    Column("pcdm_memberof_sm", ARRAY(String)),
    Column("dct_ispartof_sm", ARRAY(String)),
    Column("dct_source_sm", ARRAY(String)),
    Column("dct_isversionof_sm", ARRAY(String)),
    Column("dct_replaces_sm", ARRAY(String)),
    Column("dct_isreplacedby_sm", ARRAY(String)),
    Column("dct_rights_sm", ARRAY(String)),
    Column("dct_rightsholder_sm", ARRAY(String)),
    Column("dct_license_sm", ARRAY(Text)),
    Column("dct_accessrights_s", String),
    Column("dct_format_s", String),
    Column("gbl_filesize_s", String),
    Column("gbl_wxsidentifier_s", String),
    Column("dct_references_s", Text),
    Column("dct_identifier_sm", ARRAY(String)),
    Column("gbl_mdmodified_dt", TIMESTAMP),
    Column("gbl_mdversion_s", String),
    Column("gbl_suppressed_b", Boolean),
    Column("gbl_georeferenced_b", Boolean),
)

item_relationships = Table(
    "item_relationships",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("subject_id", String, nullable=False),
    Column("predicate", String, nullable=False),
    Column("object_id", String, nullable=False),
)

# Gazetteer Models

# GeoNames gazetteer
gazetteer_geonames = Table(
    "gazetteer_geonames",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("geonameid", BigInteger, nullable=False, unique=True, index=True),
    Column("name", String, nullable=False),
    Column("asciiname", String, nullable=False),
    Column("alternatenames", Text),
    Column("latitude", Numeric(10, 7), nullable=False),
    Column("longitude", Numeric(10, 7), nullable=False),
    Column("feature_class", String(1)),
    Column("feature_code", String(10)),
    Column("country_code", String(2)),
    Column("cc2", String(200)),
    Column("admin1_code", String(20)),
    Column("admin2_code", String(80)),
    Column("admin3_code", String(20)),
    Column("admin4_code", String(20)),
    Column("population", BigInteger),
    Column("elevation", Integer),
    Column("dem", Integer),
    Column("timezone", String(40)),
    Column("modification_date", Date),
    Column("created_at", TIMESTAMP),
    Column("updated_at", TIMESTAMP),
)

# Who's on First gazetteer tables
gazetteer_wof_spr = Table(
    "gazetteer_wof_spr",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wok_id", BigInteger, nullable=False, unique=True, index=True),
    Column("parent_id", BigInteger),
    Column("name", String, nullable=False),
    Column("placetype", String),
    Column("country", String(2)),
    Column("repo", String),
    Column("latitude", Numeric(10, 7)),
    Column("longitude", Numeric(10, 7)),
    Column("min_latitude", Numeric(10, 7)),
    Column("min_longitude", Numeric(10, 7)),
    Column("max_latitude", Numeric(10, 7)),
    Column("max_longitude", Numeric(10, 7)),
    Column("is_current", Integer),
    Column("is_deprecated", Integer),
    Column("is_ceased", Integer),
    Column("is_superseded", Integer),
    Column("is_superseding", Integer),
    Column("superseded_by", Integer),
    Column("supersedes", Integer),
    Column("lastmodified", Integer),
    Column("created_at", TIMESTAMP),
    Column("updated_at", TIMESTAMP),
)

gazetteer_wof_ancestors = Table(
    "gazetteer_wof_ancestors",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wok_id", BigInteger, nullable=False, index=True),
    Column("ancestor_id", Integer, nullable=False),
    Column("ancestor_placetype", String),
    Column("lastmodified", Integer),
    Column("created_at", TIMESTAMP),
    Column("updated_at", TIMESTAMP),
)

gazetteer_wof_concordances = Table(
    "gazetteer_wof_concordances",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wok_id", BigInteger, nullable=False, index=True),
    Column("other_id", String, nullable=False),
    Column("other_source", String, nullable=False),
    Column("lastmodified", Integer),
    Column("created_at", TIMESTAMP),
    Column("updated_at", TIMESTAMP),
)

gazetteer_wof_geojson = Table(
    "gazetteer_wof_geojson",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wok_id", BigInteger, nullable=False, index=True),
    Column("body", Text, nullable=False),
    Column("source", String),
    Column("alt_label", String),
    Column("is_alt", Boolean),
    Column("lastmodified", Integer),
    Column("created_at", TIMESTAMP),
    Column("updated_at", TIMESTAMP),
)

gazetteer_wof_names = Table(
    "gazetteer_wof_names",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wok_id", BigInteger, nullable=False, index=True),
    Column("placetype", String),
    Column("country", String(2)),
    Column("language", String),
    Column("extlang", String),
    Column("script", String),
    Column("region", String),
    Column("variant", String),
    Column("extension", String),
    Column("privateuse", String),
    Column("name", String, nullable=False),
    Column("lastmodified", Integer),
    Column("created_at", TIMESTAMP),
    Column("updated_at", TIMESTAMP),
)

# BTAA gazetteer
gazetteer_btaa = Table(
    "gazetteer_btaa",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("fast_area", String, nullable=False, index=True),
    Column("bounding_box", String),
    Column("geometry", Text),
    Column("geonames_id", String),
    Column("state_abbv", String(2), index=True),
    Column("state_name", String),
    Column("county_fips", String, index=True),
    Column("statefp", String),
    Column("namelsad", String),
    Column("created_at", TIMESTAMP),
    Column("updated_at", TIMESTAMP),
)

# FAST gazetteer
# Data source: OCLC ResearchWorks (https://researchworks.oclc.org/researchdata/fast/)
# Attribution: OCLC FAST data is provided by OCLC under the OCLC ResearchWorks license.
gazetteer_fast = Table(
    "gazetteer_fast",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("fast_id", String, nullable=False, unique=True, index=True),
    Column("uri", String, nullable=False),
    Column("type", String, nullable=False),
    Column("label", String, nullable=False),
    Column("geoname_id", String),
    Column("viaf_id", String),
    Column("wikipedia_id", String),
    Column("created_at", TIMESTAMP),
    Column("updated_at", TIMESTAMP),
)

# FAST gazetteer embeddings
# Stores vector embeddings for FAST gazetteer entries
gazetteer_fast_embeddings = Table(
    "gazetteer_fast_embeddings",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("fast_id", String, nullable=False, unique=True, index=True),
    Column("label", String, nullable=False),
    Column("geoname_id", String),
    Column("viaf_id", String),
    Column("wikipedia_id", String),
    Column("embeddings", String, nullable=False),  # Will be cast to vector(1536) in the database
    Column("created_at", TIMESTAMP),
    Column("updated_at", TIMESTAMP),
)

# AI Enrichments table
item_ai_enrichments = Table(
    "item_ai_enrichments",
    metadata,
    Column("enrichment_id", Integer, primary_key=True, autoincrement=True),
    Column("item_id", String, nullable=False, index=True),
    Column("ai_provider", String, nullable=False),
    Column("model", String, nullable=False),
    Column("enrichment_type", String(50), nullable=False),
    Column("prompt", JSON, nullable=True),
    Column("output_parser", JSON, nullable=True),
    Column("response", JSON, nullable=True),
    Column("created_at", TIMESTAMP, nullable=False),
    Column("updated_at", TIMESTAMP, nullable=False),
)
