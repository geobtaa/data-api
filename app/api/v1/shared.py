from enum import Enum


class SortOption(str, Enum):
    RELEVANCE = "relevance"
    YEAR_NEWEST = "year_desc"
    YEAR_OLDEST = "year_asc"
    TITLE_AZ = "title_asc"
    TITLE_ZA = "title_desc"


SORT_MAPPINGS = {
    SortOption.RELEVANCE: [{"_score": "desc"}],
    SortOption.YEAR_NEWEST: [{"gbl_indexyear_im": "desc"}, {"_score": "desc"}],
    SortOption.YEAR_OLDEST: [{"gbl_indexyear_im": "asc"}, {"_score": "desc"}],
    SortOption.TITLE_AZ: [{"dct_title_s.keyword": "asc"}, {"_score": "desc"}],
    SortOption.TITLE_ZA: [{"dct_title_s.keyword": "desc"}, {"_score": "desc"}],
}
