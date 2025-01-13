INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "dct_title_s": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "normalizer": "lowercase"
                    }
                }
            },
            "dct_spatial_sm": {"type": "keyword"},
            "gbl_resourceclass_sm": {"type": "keyword"},
            "gbl_resourcetype_sm": {"type": "keyword"},
            "gbl_indexyear_im": {
                "type": "integer"
            },
            "dct_language_sm": {"type": "keyword"},
            "dct_creator_sm": {"type": "keyword"},
            "schema_provider_s": {"type": "keyword"},
            "dct_accessrights_sm": {"type": "keyword"},
            "gbl_georeferenced_b": {"type": "boolean"},
            "dct_alternative_sm": {
                "type": "text",
                "store": False
            },
            "dct_description_sm": {
                "type": "text",
                "store": False
            },                
            "gbl_displaynote_sm": {
                "type": "text",
                "store": False
            },
            "dct_publisher_sm": {
                "type": "text",
                "store": False
            },
            "dct_subject_sm": {
                "type": "text",
                "store": False
            },
            "dcat_theme_sm": {
                "type": "text",
                "store": False
            },
            "dcat_keyword_sm": {
                "type": "text",
                "store": False
            },
            "dct_temporal_sm": {
                "type": "text",
                "store": False
            },
            "dct_issued_s": {
                "type": "text",
                "store": False
            },
            "gbl_daterange_drsim": {
                "type": "text",
                "store": False
            },
            "locn_geometry": {"type": "geo_shape"},
            "dcat_bbox": {"type": "geo_shape"},
            "dcat_centroid": {"type": "geo_point"},
            "dct_references_s": {"type": "object"},
            "gbl_mdmodified_dt": {"type": "date"},
            "suggest": {
                "type": "completion",
                "analyzer": "simple",
                "preserve_separators": True,
                "preserve_position_increments": True,
                "max_input_length": 50
            }
        }
    },
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "normalizer": {
                    "lowercase": {
                        "type": "custom",
                        "char_filter": [],
                        "filter": ["lowercase"]
                    }
                }
            }
        }
    }
} 