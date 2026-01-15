# AI Functions Guide - ChatGPT Integration

The STPS extension now includes powerful AI functions powered by OpenAI's ChatGPT API. These functions allow you to query ChatGPT directly from SQL, making it easy to enhance your data with AI-generated insights, summaries, classifications, and more.

## Table of Contents

- [Quick Start](#quick-start)
- [Functions Overview](#functions-overview)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Best Practices](#best-practices)
- [Cost Considerations](#cost-considerations)
- [Troubleshooting](#troubleshooting)

## Quick Start

### 1. Get an OpenAI API Key

1. Sign up at [https://platform.openai.com/](https://platform.openai.com/)
2. Navigate to API Keys section
3. Create a new API key
4. Copy the key (starts with `sk-...`)

### 2. Configure the API Key

**Option A: Using SQL** (recommended for session-based use)
```sql
SELECT stps_set_api_key('sk-your-api-key-here');
```

**Option B: Using Environment Variable** (recommended for production)
```bash
export OPENAI_API_KEY='sk-your-api-key-here'
```

**Option C: Using Config File** (recommended for persistent use)
```bash
mkdir -p ~/.stps
echo 'sk-your-api-key-here' > ~/.stps/openai_api_key
chmod 600 ~/.stps/openai_api_key
```

### 3. Start Querying

```sql
SELECT stps_ask_ai('Tax Network GmbH', 'What industry is this company in?');
-- Returns: "Tax Network GmbH is in the financial services and tax consulting industry..."
```

## Functions Overview

### `stps_ask_ai_address(company_name[, model])`

Get structured address data using AI - automatically formats response into organized address components.

**Parameters:**
- `company_name` (VARCHAR, required): Company name or location to look up
- `model` (VARCHAR, optional): OpenAI model to use (default: `gpt-3.5-turbo`)

**Returns:** STRUCT with fields:
- `city` (VARCHAR): City name
- `postal_code` (VARCHAR): Postal/ZIP code
- `street_name` (VARCHAR): Street name
- `street_nr` (VARCHAR): Street/house number

**Example:**
```sql
-- Basic usage
SELECT stps_ask_ai_address('Tax Network GmbH');
-- Returns: {city: 'München', postal_code: '80331', street_name: 'Leopoldstraße', street_nr: '244'}

-- Access individual fields
SELECT
    (stps_ask_ai_address('Tax Network GmbH')).city AS city,
    (stps_ask_ai_address('Tax Network GmbH')).postal_code AS plz;

-- Use with table data
SELECT
    company_name,
    (stps_ask_ai_address(company_name)).city AS city,
    (stps_ask_ai_address(company_name)).postal_code AS postal_code
FROM companies;

-- Use GPT-4 for better accuracy
SELECT stps_ask_ai_address('Deutsche Bank AG', 'gpt-4');
```

### `stps_ask_ai(context, prompt[, model][, max_tokens])`

Query OpenAI's ChatGPT with context and a prompt - returns free-form text response.

**Parameters:**
- `context` (VARCHAR, required): Background information or data to provide context
- `prompt` (VARCHAR, required): The question or instruction for ChatGPT
- `model` (VARCHAR, optional): OpenAI model to use (default: `gpt-3.5-turbo`)
- `max_tokens` (INTEGER, optional): Maximum response length (default: 1000)

**Returns:** VARCHAR - The AI-generated response

**Available Models:**
- `gpt-3.5-turbo` - Fast, cost-effective (recommended for most use cases)
- `gpt-4` - More capable but slower and more expensive
- `gpt-4-turbo` - Latest GPT-4 with improved performance
- `gpt-4o` - Optimized GPT-4 variant

### `stps_set_api_key(api_key)`

Configure the OpenAI API key for the current session.

**Parameters:**
- `api_key` (VARCHAR, required): Your OpenAI API key

**Returns:** VARCHAR - Confirmation message

## Configuration

### Priority Order

The extension looks for API keys in this order:

1. **Session key** set via `stps_set_api_key()` (highest priority)
2. **Environment variable** `OPENAI_API_KEY`
3. **Config file** at `~/.stps/openai_api_key`

### Security Best Practices

⚠️ **Never commit API keys to version control!**

**For development:**
```sql
-- Set once per session
SELECT stps_set_api_key('sk-...');
```

**For production:**
```bash
# In your deployment script or .bashrc
export OPENAI_API_KEY='sk-...'
```

**For persistent local use:**
```bash
# One-time setup
mkdir -p ~/.stps
echo 'sk-...' > ~/.stps/openai_api_key
chmod 600 ~/.stps/openai_api_key  # Restrict permissions
```

## Usage Examples

### Example 1: Company Address Lookup with AI

```sql
-- Use AI to extract structured address data
SELECT
    company_name,
    stps_ask_ai(
        company_name,
        'Extract and format the address in this format: Street, Number, PLZ, City. If you cannot find the exact address, return NULL.'
    ) AS ai_address
FROM companies
LIMIT 5;
```

**Output:**
```
┌──────────────────┬────────────────────────────────────────┐
│  company_name    │            ai_address                  │
├──────────────────┼────────────────────────────────────────┤
│ Deutsche Bank AG │ Taunusanlage, 12, 60325, Frankfurt     │
│ Siemens AG       │ Werner-von-Siemens-Straße, 1, 80333... │
└──────────────────┴────────────────────────────────────────┘
```

### Example 2: Data Classification

```sql
-- Classify companies by industry
SELECT
    company_name,
    stps_ask_ai(
        company_name,
        'Classify this company into one industry category: Technology, Finance, Manufacturing, Retail, Healthcare, or Other. Reply with just the category name.'
    ) AS industry
FROM companies;
```

### Example 3: Text Summarization

```sql
-- Summarize long descriptions
SELECT
    product_name,
    stps_ask_ai(
        description,
        'Summarize this product description in one sentence (max 15 words).',
        'gpt-3.5-turbo',
        50  -- Shorter response
    ) AS summary
FROM products
WHERE LENGTH(description) > 200;
```

### Example 4: Data Validation

```sql
-- Validate email addresses with AI context
SELECT
    email,
    stps_ask_ai(
        'Email: ' || email || ', Domain: ' || company_domain,
        'Does this email address look legitimate for this company domain? Answer only YES or NO.'
    ) AS is_valid
FROM customer_contacts
WHERE email IS NOT NULL;
```

### Example 5: Sentiment Analysis

```sql
-- Analyze customer feedback sentiment
SELECT
    customer_id,
    feedback_text,
    stps_ask_ai(
        feedback_text,
        'Analyze the sentiment of this feedback. Respond with: POSITIVE, NEGATIVE, or NEUTRAL.',
        'gpt-3.5-turbo',
        10
    ) AS sentiment
FROM customer_feedback
WHERE feedback_date >= CURRENT_DATE - INTERVAL '7 days';
```

### Example 6: Translation

```sql
-- Translate product descriptions
SELECT
    product_id,
    description_de AS original,
    stps_ask_ai(
        description_de,
        'Translate this German text to English. Provide only the translation.',
        'gpt-3.5-turbo',
        500
    ) AS description_en
FROM products
WHERE description_en IS NULL;
```

### Example 7: Using GPT-4 for Complex Reasoning

```sql
-- Use GPT-4 for complex financial analysis
SELECT
    company_name,
    revenue_2023,
    revenue_2022,
    profit_margin,
    stps_ask_ai(
        'Company: ' || company_name || ', Revenue 2023: ' || revenue_2023::VARCHAR ||
        ', Revenue 2022: ' || revenue_2022::VARCHAR || ', Profit Margin: ' || profit_margin::VARCHAR,
        'Analyze this company''s financial health and growth trajectory. Provide a brief assessment (2-3 sentences).',
        'gpt-4',  -- Use more capable model
        200
    ) AS financial_analysis
FROM financial_data
WHERE revenue_2023 IS NOT NULL;
```

### Example 8: Batch Processing with Caching

```sql
-- Process data in batches to manage costs
CREATE TEMP TABLE ai_results AS
SELECT
    id,
    data,
    stps_ask_ai(data, 'Extract key insights from this data.') AS insights
FROM
    (SELECT * FROM large_dataset LIMIT 100) AS sample;  -- Process in batches

-- Use results
SELECT * FROM ai_results WHERE insights LIKE '%significant%';
```

### Example 9: Combining Multiple AI Calls

```sql
-- Chain AI operations for multi-step analysis
WITH classified AS (
    SELECT
        id,
        text,
        stps_ask_ai(text, 'Is this text about: A) Technology, B) Business, or C) Other? Answer with just the letter.') AS category
    FROM documents
)
SELECT
    id,
    text,
    category,
    CASE
        WHEN category = 'A' THEN stps_ask_ai(text, 'Extract the main technology mentioned.')
        WHEN category = 'B' THEN stps_ask_ai(text, 'What is the business model described?')
        ELSE 'N/A'
    END AS detailed_analysis
FROM classified;
```

## Best Practices

### 1. Performance Optimization

**✅ Good: Process in batches**
```sql
-- Process 100 rows at a time
SELECT * FROM (
    SELECT *, stps_ask_ai(text, prompt) AS result
    FROM data
    LIMIT 100
) WHERE result IS NOT NULL;
```

**❌ Bad: Process entire table at once**
```sql
-- This could be slow and expensive
SELECT *, stps_ask_ai(text, prompt) AS result
FROM million_row_table;
```

### 2. Cost Management

**✅ Good: Use appropriate model**
```sql
-- Use gpt-3.5-turbo for simple tasks
SELECT stps_ask_ai(name, 'Categorize: food or drink?', 'gpt-3.5-turbo', 10);
```

**❌ Bad: Use GPT-4 unnecessarily**
```sql
-- Overkill and expensive for simple classification
SELECT stps_ask_ai(name, 'Categorize: food or drink?', 'gpt-4', 1000);
```

### 3. Prompt Engineering

**✅ Good: Clear, specific prompts**
```sql
SELECT stps_ask_ai(
    email,
    'Is this email format valid? Answer only YES or NO.'
);
```

**❌ Bad: Vague prompts**
```sql
SELECT stps_ask_ai(
    email,
    'What do you think about this?'
);
```

### 4. Error Handling

**✅ Good: Handle errors gracefully**
```sql
SELECT
    id,
    CASE
        WHEN result LIKE 'ERROR:%' THEN NULL
        ELSE result
    END AS cleaned_result
FROM (
    SELECT id, stps_ask_ai(data, prompt) AS result
    FROM source
) t;
```

### 5. Context Length Management

**✅ Good: Limit context size**
```sql
-- Truncate long text to avoid token limits
SELECT stps_ask_ai(
    LEFT(long_text, 2000),  -- Limit to ~500 tokens
    'Summarize the main points.'
);
```

## Cost Considerations

### Token Pricing (as of 2024)

| Model | Input (per 1K tokens) | Output (per 1K tokens) |
|-------|----------------------|------------------------|
| gpt-3.5-turbo | $0.0005 | $0.0015 |
| gpt-4 | $0.03 | $0.06 |
| gpt-4-turbo | $0.01 | $0.03 |

### Cost Example

Processing 1,000 rows with gpt-3.5-turbo:
- Average context: 100 tokens
- Average prompt: 20 tokens
- Average response: 50 tokens
- Total: ~170 tokens per row
- Cost: 1,000 rows × 170 tokens / 1,000 × ($0.0005 + $0.0015) ≈ **$0.34**

### Cost Optimization Tips

1. **Use gpt-3.5-turbo** for most tasks (20-60x cheaper than GPT-4)
2. **Limit `max_tokens`** to only what you need
3. **Process in batches** and cache results
4. **Keep context concise** - extract only relevant data
5. **Use WHERE clauses** to filter before processing
6. **Monitor usage** on [OpenAI dashboard](https://platform.openai.com/usage)

## Troubleshooting

### Error: "API key not configured"

**Solution:**
```sql
-- Check if key is set
SELECT stps_ask_ai('test', 'hello');  -- Will show error

-- Set the key
SELECT stps_set_api_key('sk-your-key-here');

-- Or use environment variable
-- In terminal: export OPENAI_API_KEY='sk-your-key-here'
```

### Error: "Failed to execute curl command"

**Solution:**
```bash
# Install curl (if not present)
# Ubuntu/Debian:
sudo apt-get install curl

# macOS (usually pre-installed):
brew install curl

# Windows:
# Curl is included in Windows 10+ by default
```

### Error: "OpenAI API returned error: Rate limit exceeded"

**Solution:**
```sql
-- Reduce request rate, process in smaller batches
-- Wait a few seconds between batches

-- Example: Process 10 at a time with delay
SELECT * FROM (
    SELECT *, stps_ask_ai(text, prompt) AS result
    FROM data
    LIMIT 10 OFFSET 0  -- Change offset manually
);
```

### Error: "Could not parse response from OpenAI API"

**Causes:**
- API endpoint changed
- Network issues
- Malformed request

**Solution:**
```sql
-- Check OpenAI status: https://status.openai.com/
-- Verify API key is valid
-- Try again with simpler prompt
```

### Response is "ERROR: ..."

Common errors and solutions:

| Error Message | Solution |
|--------------|----------|
| `insufficient_quota` | Add credits to OpenAI account |
| `invalid_api_key` | Check API key is correct |
| `model_not_found` | Use valid model name (gpt-3.5-turbo, gpt-4) |
| `context_length_exceeded` | Reduce context size or max_tokens |

## Requirements

- **curl** or **wget** must be installed
- **OpenAI API key** (get from [platform.openai.com](https://platform.openai.com))
- **Internet connection**
- **OpenAI account with credits**

## Security Notes

🔒 **API Key Security:**
- Never hardcode API keys in SQL queries that are saved
- Use environment variables or config files for persistent keys
- Restrict file permissions on `~/.stps/openai_api_key` (chmod 600)
- Rotate keys periodically
- Monitor usage on OpenAI dashboard

🔒 **Data Privacy:**
- Data sent to OpenAI is subject to their [privacy policy](https://openai.com/policies/privacy-policy)
- Do not send sensitive personal data (PII) without proper consent
- Consider data residency requirements for your jurisdiction
- Review OpenAI's [data usage policies](https://openai.com/policies/usage-policies)

## Additional Resources

- [OpenAI API Documentation](https://platform.openai.com/docs/api-reference)
- [OpenAI Pricing](https://openai.com/pricing)
- [Best Practices for Prompt Engineering](https://platform.openai.com/docs/guides/prompt-engineering)
- [OpenAI Usage Policies](https://openai.com/policies/usage-policies)

## Support

For issues specific to the STPS extension:
- GitHub: [https://github.com/Arengard/stps-extension/issues](https://github.com/Arengard/stps-extension/issues)

For OpenAI API issues:
- OpenAI Help: [https://help.openai.com/](https://help.openai.com/)
- Status Page: [https://status.openai.com/](https://status.openai.com/)
