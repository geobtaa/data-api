from .client import es, init_elasticsearch, close_elasticsearch
from .index import index_documents
from .search import search_documents

__all__ = [
    'es',
    'init_elasticsearch',
    'close_elasticsearch',
    'index_documents',
    'search_documents'
]
