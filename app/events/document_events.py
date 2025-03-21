import logging
import asyncio
from typing import List, Dict, Any, Optional
from ..services.cache_service import CacheService, invalidate_cache_with_prefix, ENDPOINT_CACHE

logger = logging.getLogger(__name__)

async def invalidate_document_caches(doc_ids: List[str]) -> bool:
    """
    Invalidate caches related to specified documents.
    
    This should be called whenever documents are created, updated, or deleted.
    """
    if not ENDPOINT_CACHE:
        return True
    
    try:
        logger.info(f"Invalidating caches for documents: {doc_ids}")
        
        # Invalidate search cache (affects all searches)
        await invalidate_cache_with_prefix("app.api.v1.endpoints:search")
        
        # Invalidate suggest cache (affects all suggestions)
        await invalidate_cache_with_prefix("app.api.v1.endpoints:suggest")
        
        # Invalidate specific document caches
        for doc_id in doc_ids:
            # Create a cache key pattern similar to how the cached_endpoint decorator does it
            document_prefix = f"app.api.v1.endpoints:get_document:{doc_id}"
            await invalidate_cache_with_prefix(document_prefix)
        
        logger.info("Cache invalidation completed successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to invalidate caches: {str(e)}")
        return False

async def on_document_created(document: Dict[str, Any]) -> None:
    """Event handler for document creation."""
    doc_id = document.get("id")
    if doc_id:
        await invalidate_document_caches([doc_id])

async def on_document_updated(document: Dict[str, Any]) -> None:
    """Event handler for document update."""
    doc_id = document.get("id")
    if doc_id:
        await invalidate_document_caches([doc_id])

async def on_document_deleted(doc_id: str) -> None:
    """Event handler for document deletion."""
    if doc_id:
        await invalidate_document_caches([doc_id])

async def on_documents_bulk_operation(doc_ids: List[str]) -> None:
    """Event handler for bulk operations on documents."""
    if doc_ids:
        await invalidate_document_caches(doc_ids) 