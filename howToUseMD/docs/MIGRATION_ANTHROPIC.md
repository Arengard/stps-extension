# Migration Guide: OpenAI to Anthropic Claude

This document provides a comprehensive guide for migrating from OpenAI's API to Anthropic's Claude API in the STPS DuckDB extension.

## Table of Contents

- [Overview](#overview)
- [Breaking Changes](#breaking-changes)
- [Migration Steps](#migration-steps)
- [API Key Configuration](#api-key-configuration)
- [Model Mapping](#model-mapping)
- [Code Changes](#code-changes)
- [Testing](#testing)
- [Rollback Plan](#rollback-plan)

## Overview

The STPS extension has migrated from OpenAI's API to Anthropic's Claude API to leverage:

- **Better performance**: Claude models offer improved reasoning and accuracy
- **Larger context windows**: 200K tokens across all models
- **Competitive pricing**: More cost-effective for many use cases
- **Enhanced safety**: Anthropic's constitutional AI approach

## Breaking Changes

### 1. API Key Changes

**Before (OpenAI):**
```sql
-- Environment variable
export OPENAI_API_KEY='sk-...'

-- Config file
~/.stps/openai_api_key
```

**After (Anthropic):**
```sql
-- Environment variable
export ANTHROPIC_API_KEY='sk-ant-...'

-- Config file
~/.stps/anthropic_api_key
```

### 2. Model Name Changes

| OpenAI Model | Anthropic Claude Model | Use Case |
|--------------|------------------------|----------|
| `gpt-3.5-turbo` | `claude-3-5-haiku-20241022` | Fast, simple tasks |
| `gpt-4o-mini` | `claude-3-5-sonnet-20241022` | Default - balanced performance |
| `gpt-4o` | `claude-3-5-sonnet-20241022` | General purpose |
| `gpt-4` | `claude-opus-4-5-20251101` | Complex reasoning |

### 3. API Endpoint Changes

**Before:** `https://api.openai.com/v1/chat/completions`

**After:** `https://api.anthropic.com/v1/messages`

### 4. Response Format Changes

The internal response format has changed, but the SQL function interface remains the same. No changes required in your SQL queries.

## Migration Steps

### Step 1: Update API Key

Choose one of the following methods:

#### Option A: Environment Variable (Recommended for Production)

```bash
# Remove old key
unset OPENAI_API_KEY

# Set new key
export ANTHROPIC_API_KEY='sk-ant-your-key-here'

# Add to your shell profile for persistence
echo "export ANTHROPIC_API_KEY='sk-ant-your-key-here'" >> ~/.bashrc
```

#### Option B: Config File (Recommended for Development)

```bash
# Remove old config file
rm ~/.stps/openai_api_key

# Create new config file
mkdir -p ~/.stps
echo 'sk-ant-your-key-here' > ~/.stps/anthropic_api_key
chmod 600 ~/.stps/anthropic_api_key
```

#### Option C: Session Variable (For Testing)

```sql
-- In your SQL session
SELECT stps_set_api_key('sk-ant-your-api-key-here');
```

### Step 2: Update Model References in SQL Queries

Review your existing SQL queries and update model names:

**Before:**
```sql
-- Using GPT-3.5 Turbo
SELECT stps_ask_ai(
    company_name,
    'Classify this company',
    'gpt-3.5-turbo'
) AS result
FROM companies;

-- Using GPT-4
SELECT stps_ask_ai(
    financial_data,
    'Analyze this data',
    'gpt-4'
) AS analysis
FROM reports;
```

**After:**
```sql
-- Using Claude Haiku (equivalent to GPT-3.5 Turbo)
SELECT stps_ask_ai(
    company_name,
    'Classify this company',
    'claude-3-5-haiku-20241022'
) AS result
FROM companies;

-- Using Claude Opus (equivalent to GPT-4)
SELECT stps_ask_ai(
    financial_data,
    'Analyze this data',
    'claude-opus-4-5-20251101'
) AS analysis
FROM reports;
```

### Step 3: Update the Extension

Download and install the latest version with Anthropic support:

```bash
# Download from GitHub Actions
# https://github.com/Arengard/stps-extension/actions

# Load in DuckDB
duckdb -unsigned
```

```sql
INSTALL './stps.duckdb_extension';
LOAD stps;
```

### Step 4: Verify the Migration

Test the AI functions with your new API key:

```sql
-- Test basic functionality
SELECT stps_ask_ai('Test', 'Say hello');

-- Test address lookup
SELECT stps_ask_ai_address('Deutsche Bank AG');

-- Verify model parameter
SELECT stps_ask_ai('Data', 'Summarize', 'claude-3-5-sonnet-20241022');
```

## API Key Configuration

### Priority Order

The extension checks for API keys in this order:

1. **Session key** (set via `stps_set_api_key()`) - Highest priority
2. **Environment variable** (`ANTHROPIC_API_KEY`)
3. **Config file** (`~/.stps/anthropic_api_key`) - Lowest priority

### Getting an Anthropic API Key

1. Visit [https://console.anthropic.com/](https://console.anthropic.com/)
2. Sign up or log in to your account
3. Navigate to **API Keys** section
4. Click **Create Key**
5. Copy the key (starts with `sk-ant-`)
6. Store it securely using one of the methods above

## Model Mapping

### Detailed Comparison

#### Claude 3.5 Haiku (Replaces GPT-3.5 Turbo)

```sql
-- Model: claude-3-5-haiku-20241022
-- Speed: Fastest
-- Cost: Lowest ($0.0005 input / $0.0015 output per 1K tokens)
-- Context: 200K tokens
-- Best for: Classification, validation, simple queries
```

**Example:**
```sql
SELECT stps_ask_ai(
    product_name,
    'Is this a food or drink item?',
    'claude-3-5-haiku-20241022',
    10
) AS category
FROM products;
```

#### Claude 3.5 Sonnet (Default - Replaces GPT-4o)

```sql
-- Model: claude-3-5-sonnet-20241022
-- Speed: Fast
-- Cost: Medium ($0.003 input / $0.015 output per 1K tokens)
-- Context: 200K tokens
-- Best for: Most use cases, balanced performance
```

**Example:**
```sql
SELECT stps_ask_ai(
    company_description,
    'Summarize the company business model',
    'claude-3-5-sonnet-20241022'
) AS summary
FROM companies;
```

#### Claude Opus 4.5 (Replaces GPT-4)

```sql
-- Model: claude-opus-4-5-20251101
-- Speed: Slower
-- Cost: Highest ($0.03 input / $0.06 output per 1K tokens)
-- Context: 200K tokens
-- Best for: Complex reasoning, critical accuracy
```

**Example:**
```sql
SELECT stps_ask_ai(
    financial_statements,
    'Provide detailed financial analysis with risks',
    'claude-opus-4-5-20251101',
    500
) AS analysis
FROM financials;
```

## Code Changes

### Function Signatures (Unchanged)

The SQL function signatures remain the same:

```sql
-- Address lookup (no changes needed)
stps_ask_ai_address(company_name VARCHAR[, model VARCHAR]) → STRUCT

-- General AI query (no changes needed)
stps_ask_ai(context VARCHAR, prompt VARCHAR[, model VARCHAR][, max_tokens INTEGER]) → VARCHAR

-- API key configuration (no changes needed)
stps_set_api_key(api_key VARCHAR) → VARCHAR
```

### Internal Changes (For Developers)

If you're contributing to the extension:

1. **API endpoint**: Updated to Anthropic's Messages API
2. **Request format**: Changed to Anthropic's message format
3. **Response parsing**: Updated to handle Anthropic's response structure
4. **Headers**: Now includes `anthropic-version: 2023-06-01`

## Testing

### Test Plan

Run these queries to verify the migration:

```sql
-- 1. Test API key configuration
SELECT stps_set_api_key('sk-ant-your-key-here');

-- 2. Test basic query
SELECT stps_ask_ai('Hello', 'Reply with "Working"') AS test;
-- Expected: "Working" or similar affirmative response

-- 3. Test with different models
SELECT stps_ask_ai('Test', 'Say hi', 'claude-3-5-haiku-20241022') AS haiku_test;
SELECT stps_ask_ai('Test', 'Say hi', 'claude-3-5-sonnet-20241022') AS sonnet_test;
SELECT stps_ask_ai('Test', 'Say hi', 'claude-opus-4-5-20251101') AS opus_test;

-- 4. Test address function
SELECT stps_ask_ai_address('Tax Network GmbH');
-- Expected: Struct with city, postal_code, street_name, street_nr

-- 5. Test with real data
SELECT
    company_name,
    stps_ask_ai(company_name, 'What industry?') AS industry
FROM (VALUES ('Apple Inc.'), ('Goldman Sachs')) AS t(company_name);

-- 6. Test error handling (invalid model)
SELECT stps_ask_ai('Test', 'Hello', 'invalid-model');
-- Expected: Error message indicating invalid model
```

### Performance Testing

Compare response times between models:

```sql
-- Time a query with Haiku
SELECT
    NOW() AS start_time,
    stps_ask_ai('Data', 'Classify', 'claude-3-5-haiku-20241022') AS result,
    NOW() AS end_time;

-- Time a query with Sonnet
SELECT
    NOW() AS start_time,
    stps_ask_ai('Data', 'Classify', 'claude-3-5-sonnet-20241022') AS result,
    NOW() AS end_time;
```

## Rollback Plan

If you need to rollback to OpenAI:

### Option 1: Use Previous Extension Version

```bash
# Download the last OpenAI-compatible version
# From GitHub Actions: Find the last build before migration

# Install old version
duckdb -unsigned
```

```sql
INSTALL './stps-old-version.duckdb_extension';
LOAD stps;
```

### Option 2: Restore Environment Variables

```bash
# Remove Anthropic key
unset ANTHROPIC_API_KEY
rm ~/.stps/anthropic_api_key

# Restore OpenAI key
export OPENAI_API_KEY='sk-your-old-key'
echo 'sk-your-old-key' > ~/.stps/openai_api_key
chmod 600 ~/.stps/openai_api_key
```

## Troubleshooting

### Error: "API key not configured"

**Solution:**
```sql
-- Verify environment variable
SELECT getenv('ANTHROPIC_API_KEY');

-- Or set session key
SELECT stps_set_api_key('sk-ant-...');
```

### Error: "Invalid API key"

**Causes:**
- Key doesn't start with `sk-ant-`
- Key is expired or revoked
- Wrong key format

**Solution:**
1. Go to [https://console.anthropic.com/](https://console.anthropic.com/)
2. Generate a new API key
3. Update your configuration

### Error: "Model not found"

**Solution:**
Use valid model names:
- `claude-3-5-haiku-20241022`
- `claude-3-5-sonnet-20241022`
- `claude-opus-4-5-20251101`

### Different Results Than OpenAI

This is expected. Claude may provide different (often better) responses than GPT models. Review and adjust your prompts if needed for optimal results with Claude.

### Rate Limiting

Anthropic has different rate limits than OpenAI. If you encounter rate limiting:

```sql
-- Process in smaller batches
SELECT * FROM (
    SELECT *, stps_ask_ai(text, prompt) AS result
    FROM data
    LIMIT 10  -- Reduce batch size
);
```

## Cost Comparison

### Pricing Overview (as of 2024)

| Model | Input (per 1M tokens) | Output (per 1M tokens) |
|-------|----------------------|------------------------|
| **Anthropic** | | |
| Claude 3.5 Haiku | $0.50 | $1.50 |
| Claude 3.5 Sonnet | $3.00 | $15.00 |
| Claude Opus 4.5 | $30.00 | $60.00 |
| **OpenAI (Previous)** | | |
| GPT-3.5 Turbo | $0.50 | $1.50 |
| GPT-4o | $5.00 | $15.00 |
| GPT-4 | $30.00 | $60.00 |

### Cost Optimization Tips

1. **Use Claude Haiku** for simple classification tasks (same cost as GPT-3.5)
2. **Use Claude Sonnet** as default (40% cheaper than GPT-4o for input)
3. **Use Claude Opus** only for complex reasoning (same cost as GPT-4)
4. **Limit max_tokens** to reduce output costs
5. **Cache frequently used prompts** at application level

## Additional Resources

- **Anthropic Documentation**: [https://docs.anthropic.com/](https://docs.anthropic.com/)
- **API Reference**: [https://docs.anthropic.com/en/api](https://docs.anthropic.com/en/api)
- **Pricing**: [https://www.anthropic.com/pricing](https://www.anthropic.com/pricing)
- **Status Page**: [https://status.anthropic.com/](https://status.anthropic.com/)
- **Support**: [https://support.anthropic.com/](https://support.anthropic.com/)

## Support

For migration issues:

- **Extension Issues**: [GitHub Issues](https://github.com/Arengard/stps-extension/issues)
- **Anthropic API**: [Support Portal](https://support.anthropic.com/)
- **Documentation**: [AI Functions Guide](../AI_FUNCTIONS_GUIDE.md)

## Changelog

### Version X.X.X (Migration Release)

**Added:**
- Anthropic Claude API integration
- Support for Claude 3.5 Haiku, Sonnet, and Opus 4.5 models
- 200K token context window support

**Changed:**
- API endpoint from OpenAI to Anthropic
- Environment variable from `OPENAI_API_KEY` to `ANTHROPIC_API_KEY`
- Config file from `~/.stps/openai_api_key` to `~/.stps/anthropic_api_key`
- Default model from `gpt-3.5-turbo` to `claude-3-5-sonnet-20241022`

**Removed:**
- OpenAI API integration
- Support for GPT models

**Migration Required:**
- Update API key configuration
- Update model names in SQL queries
- See migration guide above for details
