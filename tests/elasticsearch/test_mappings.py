from app.elasticsearch.mappings import INDEX_MAPPING


def test_index_mapping_structure():
    """Test the structure of the INDEX_MAPPING."""
    # Check that INDEX_MAPPING has the expected top-level keys
    assert "mappings" in INDEX_MAPPING
    assert "settings" in INDEX_MAPPING

    # Check that mappings has the expected structure
    assert "properties" in INDEX_MAPPING["mappings"]

    # Check that settings has the expected structure
    assert "index" in INDEX_MAPPING["settings"]
    assert "number_of_shards" in INDEX_MAPPING["settings"]["index"]
    assert "number_of_replicas" in INDEX_MAPPING["settings"]["index"]
    assert "analysis" in INDEX_MAPPING["settings"]["index"]


def test_required_fields():
    """Test that all required fields are present in the mapping."""
    required_fields = [
        "id",
        "dct_title_s",
        "dct_spatial_sm",
        "gbl_resourceclass_sm",
        "gbl_resourcetype_sm",
        "locn_geometry",
        "dct_references_s",
    ]

    for field in required_fields:
        assert field in INDEX_MAPPING["mappings"]["properties"], f"Missing required field: {field}"


def test_field_types():
    """Test that fields have the expected types."""
    field_types = {
        "id": "keyword",
        "dct_title_s": "text",
        "locn_geometry": "geo_shape",
        "dcat_centroid": "geo_point",
        "dct_references_s": "object",
        "gbl_mdmodified_dt": "date",
        "suggest": "completion",
    }

    for field, expected_type in field_types.items():
        assert field in INDEX_MAPPING["mappings"]["properties"], f"Missing field: {field}"
        assert INDEX_MAPPING["mappings"]["properties"][field]["type"] == expected_type, (
            f"Field {field} has type {INDEX_MAPPING['mappings']['properties'][field]['type']}, expected {expected_type}"
        )
