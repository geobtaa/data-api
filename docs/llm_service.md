# LLM Service Documentation

This document provides comprehensive documentation for the LLM (Large Language Model) service used in the data-api project. The service uses OpenAI's GPT models to provide AI-powered summarization, OCR, and geographic entity identification capabilities for historical maps and geographic datasets.

## Table of Contents
- [Overview](#overview)
- [Setup](#setup)
- [API Endpoints](#api-endpoints)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)

## Overview

The LLM service provides AI-powered capabilities for historical maps and geographic datasets, including:
- Text summarization
- OCR (Optical Character Recognition) for extracting text from images
- Geographic entity identification and mapping to local gazetteer entries

The service uses OpenAI's GPT models as the backend LLM service, with support for various models like `gpt-3.5-turbo` and other OpenAI models.

## Setup

The LLM service requires an OpenAI API key to function. To get started:

1. Obtain an OpenAI API key from [OpenAI's platform](https://platform.openai.com/)
2. Set up your environment variables (see [Environment Variables](#environment-variables))
3. Ensure the service has access to the gazetteer service for geographic entity identification

## API Endpoints

### Generate Summary

Generate a summary for a document:

```http
POST /api/v1/documents/{id}/summarize
```

Parameters:
- `id` (path parameter): The document ID
- `callback` (query parameter, optional): JSONP callback name

Example CURL request:
```bash
curl -X POST \
  'http://localhost:8000/api/v1/documents/123/summarize' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer your-api-key'
```

Response:
```json
{
    "status": "success",
    "message": "Summary generation started",
    "task_id": "task-id-here"
}
```

### Get Document Summaries

Retrieve all summaries for a document:

```http
GET /api/v1/documents/{id}/summaries
```

Parameters:
- `id` (path parameter): The document ID
- `callback` (query parameter, optional): JSONP callback name

Response:
```json
{
    "data": {
        "type": "summaries",
        "id": "document-id",
        "attributes": {
            "summaries": [
                {
                    "enrichment_id": "id",
                    "ai_provider": "OpenAI",
                    "model": "gpt-3.5-turbo",
                    "response": {
                        "summary": "Generated summary text",
                        "timestamp": "2024-03-22T12:00:00Z"
                    },
                    "created_at": "2024-03-22T12:00:00Z"
                }
            ]
        }
    }
}
```

### Generate OCR

Extract text from images using OCR:

```http
POST /api/v1/documents/{id}/ocr
```

Parameters:
- `id` (path parameter): The document ID
- `callback` (query parameter, optional): JSONP callback name

Response:
```json
{
    "status": "success",
    "message": "OCR generation started",
    "task_id": "task-id-here"
}
```

### Identify Geographic Entities

Identify and map geographic entities in text:

```http
POST /api/v1/documents/{id}/identify-geo-entities
```

Parameters:
- `id` (path parameter): The document ID
- `callback` (query parameter, optional): JSONP callback name

Response:
```json
{
    "status": "success",
    "message": "Geographic entity identification started",
    "task_id": "task-id-here"
}
```

## Environment Variables

The LLM service uses the following environment variables:

- `OPENAI_API_KEY`: Your OpenAI API key (required)
- `OPENAI_MODEL`: The default model to use (default: "gpt-3.5-turbo")
- `LOG_PATH`: Path to store service logs (default: "logs")

These should be set in your environment or in the service configuration.

## Troubleshooting

### Common Issues

1. **API Authentication Failures**
   - Verify your OpenAI API key is correct and valid
   - Check if the API key has sufficient permissions
   - Ensure the API key is properly set in the environment variables

2. **Model Not Available**
   - Verify the specified model is available in your OpenAI account
   - Check if you have access to the requested model
   - Ensure the model name is spelled correctly

3. **Summary Generation Fails**
   - Check the API logs for error messages
   - Verify the document exists in the database
   - Ensure the input data is properly formatted

4. **OCR Generation Issues**
   - Verify the image is accessible and in a supported format
   - Check if the image contains readable text
   - Ensure the image size is within OpenAI's limits

### Getting Help

If you encounter issues not covered here:
1. Check the application logs in the specified log directory
2. Review the OpenAI API documentation
3. Contact the development team 