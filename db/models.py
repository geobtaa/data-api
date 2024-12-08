from sqlalchemy import Table, Column, String, Text, ARRAY, Boolean, TIMESTAMP, MetaData

metadata = MetaData()

geoportal_development = Table(
    'geoportal_development',
    metadata,
    Column('id', String, primary_key=True),
    Column('dct_title_s', Text),
    Column('dct_publisher_sm', Text),
    Column('dct_spatial_sm', ARRAY(Text)),
    Column('gbl_resourceclass_sm', ARRAY(Text)),
    Column('gbl_resourcetype_sm', ARRAY(Text)),
    Column('b1g_language_sm', ARRAY(Text)),
    Column('dct_creator_sm', ARRAY(Text)),
    Column('schema_provider_s', Text),
    Column('dct_accessrights_s', Text),
    Column('gbl_georeferenced_b', Boolean),
    Column('b1g_georeferenced_allmaps_b', Boolean),
    Column('dct_temporal_sm', ARRAY(Text)),
    Column('dct_rightsholder_sm', Text),
    Column('dct_license_sm', Text),
    Column('dct_subject_sm', ARRAY(Text)),
    Column('dct_references_s', Text),
    Column('date_created_dtsi', TIMESTAMP),
    Column('date_modified_dtsi', TIMESTAMP)
)
