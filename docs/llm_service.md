# LLM Service Documentation

This document provides comprehensive documentation for the LLM (Large Language Model) service used in the data-api project. The service uses Ollama to provide AI-powered summarization capabilities for historical maps and geographic datasets.

## Table of Contents
- [Overview](#overview)
- [Setup](#setup)
- [Managing Ollama](#managing-ollama)
- [API Endpoints](#api-endpoints)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)

## Overview

The LLM service provides AI-powered summarization capabilities for historical maps and geographic datasets. It uses Ollama as the backend LLM service and supports various models like gemma3:1b and gemma3:4b. The service is integrated with the main API and provides endpoints for generating and retrieving summaries.

## Setup

The LLM service is part of the Docker Compose setup. To get started:

1. Ensure you have Docker and Docker Compose installed
2. Clone the repository
3. Start the services:
```bash
docker-compose up -d
```

This will start all required services, including Ollama.

## Managing Ollama

### Starting and Stopping Ollama

The Ollama service is managed through Docker Compose:

```bash
# Start Ollama
docker-compose up -d ollama

# Stop Ollama
docker-compose stop ollama

# Restart Ollama
docker-compose restart ollama
```

### Managing Models

#### Installing a New Model

To install a new model in Ollama:

```bash
# Format
docker exec data-api-ollama ollama pull <model-name>

# Example: Install gemma3:1b
docker exec data-api-ollama ollama pull gemma3:1b
```

#### Removing a Model

To remove a model:

```bash
# Format
docker exec data-api-ollama ollama rm <model-name>

# Example: Remove gemma3:1b
docker exec data-api-ollama ollama rm gemma3:1b
```

#### List Available Models

To see all installed models:

```bash
docker exec data-api-ollama ollama list
```

## API Endpoints

### Generate Summary

Generate a summary for a document:

```http
POST /api/v1/documents/{id}/summarize
```

Parameters:
- `id` (path parameter): The document ID
- `callback` (query parameter, optional): JSONP callback name

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
                    "ai_provider": "Ollama",
                    "model": "gemma3:1b",
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

## Environment Variables

The LLM service uses the following environment variables:

- `OLLAMA_HOST`: The URL of the Ollama service (default: "http://ollama:11434")
- `OLLAMA_MODEL`: The default model to use (default: "gemma3:1b")

These can be set in your `.env` file or in the Docker Compose environment section.

## Troubleshooting

### Common Issues

1. **Ollama Service Not Responding**
   - Check if the Ollama container is running: `docker-compose ps`
   - Check Ollama logs: `docker-compose logs ollama`
   - Ensure the OLLAMA_HOST environment variable is set correctly

2. **Model Not Found**
   - Verify the model is installed: `docker exec data-api-ollama ollama list`
   - Reinstall the model if needed: `docker exec data-api-ollama ollama pull <model-name>`

3. **Summary Generation Fails**
   - Check the API logs: `docker-compose logs api`
   - Verify the document exists in the database
   - Ensure the Ollama service is running and accessible

### Getting Help

If you encounter issues not covered here:
1. Check the application logs
2. Review the Ollama documentation
3. Contact the development team 