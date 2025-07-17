# CQC Document Layout Chunker (Azure Function)

HTTP-triggered Azure Function used as a custom Web API skill in Azure AI Search.

## Configuration

Set these **Application settings** in the Function App (portal > Configuration):

| Setting | Example |
| --- | --- |
| DOC_ENDPOINT | https://your-multiservice.cognitiveservices.azure.com |
| DOC_KEY | (key value) |
| DOC_API_VER | 2024-07-31-preview |
| CHUNK_MAX | 2000 |
| CHUNK_OVERLAP | 200 |
| FOOT_Y_HIGH | 0.85 |
| FOOT_Y_LOW | 0.05 |

## Local development

1. Copy `local.settings.json.example` to `local.settings.json`.
2. Fill in your own values.
3. Create a venv, install requirements: `pip install -r requirements.txt`.
4. Start Functions host: `func start`.

## Deploy (GitHub Actions)

Add the provided workflow and set `AZURE_FUNCTIONAPP_PUBLISH_PROFILE` secret.
