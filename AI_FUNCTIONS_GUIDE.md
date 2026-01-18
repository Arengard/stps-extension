# AI Functions Guide - Anthropic Claude Integration

The STPS extension now includes powerful AI functions powered by Anthropic's Claude API. These functions allow you to query Claude directly from SQL, making it easy to enhance your data with AI-generated insights, summaries, classifications, and more.

## Table of Contents

- [Quick Start](#quick-start)
- [Available Models](#available-models)
- [Functions Overview](#functions-overview)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Best Practices](#best-practices)
- [Cost Considerations](#cost-considerations)
- [Troubleshooting](#troubleshooting)

## Quick Start

### 1. Get an Anthropic API Key

1. Sign up at [https://console.anthropic.com/](https://console.anthropic.com/)
2. Navigate to API Keys section
3. Create a new API key
4. Copy the key (starts with `sk-ant-...`)

### 2. Configure the API Key

**Option A: Using SQL** (recommended for session-based use)
```sql
SELECT stps_set_api_key('sk-ant-your-api-key-here');
```

**Option B: Using Environment Variable** (recommended for production)
```bash
export ANTHROPIC_API_KEY='sk-ant-your-api-key-here'
```

**Option C: Using Config File** (recommended for persistent use)
```bash
mkdir -p ~/.stps
echo 'sk-ant-your-api-key-here' > ~/.stps/anthropic_api_key
chmod 600 ~/.stps/anthropic_api_key
```

### 3. Start Querying

```sql
SELECT stps_ask_ai('Tax Network GmbH', 'What industry is this company in?');
-- Returns: "Tax Network GmbH is in the financial services and tax consulting industry..."
```

## Available Models

The extension supports the following Anthropic Claude models:

| Model | Use Case | Speed | Cost | Context Window |
|-------|----------|-------|------|----------------|
| `claude-sonnet-4-5-20250929` | **Default** - Best balance of speed, cost, and capability | Fast | Medium | 200K tokens |
| `claude-3-7-sonnet-20250219` | Simple tasks - Fast and cost-effective | Fastest | Lowest | 200K tokens |
| `claude-opus-4-5-20251101` | Complex reasoning - Most capable | Slower | Highest | 200K tokens |

**Model Selection Guidelines:**
- Use **Haiku** for: Simple classification, sentiment analysis, data validation
- Use **Sonnet** (default) for: Most tasks, address lookups, summarization, translation
- Use **Opus** for: Complex financial analysis, detailed reasoning, critical accuracy needs

## Web Search Integration

### Overview

When configured with a Brave Search API key, both `stps_ask_ai` and `stps_ask_ai_address` can automatically search the web for current information.

### Setup

1. Get Brave API key from https://brave.com/search/api/
2. Configure using one of three methods (same as Anthropic key)
3. Queries automatically use search when Claude determines it's needed

### Examples

**Real-time Financial Data:**
```sql
SELECT stps_set_brave_api_key('BSA-...');

SELECT
    ticker,
    stps_ask_ai(ticker, 'Current stock price?') as price
FROM stocks;
```

**Current Events:**
```sql
SELECT stps_ask_ai('Ukraine', 'What is the latest news today?');
```

**Company Information:**
```sql
SELECT
    company_name,
    stps_ask_ai(company_name, 'Latest quarterly revenue?') as revenue
FROM companies;
```

**Address Lookups (Structured Output):**
```sql
-- stps_ask_ai_address also benefits from web search
SELECT
    company,
    (stps_ask_ai_address(company)).city,
    (stps_ask_ai_address(company)).postal_code,
    (stps_ask_ai_address(company)).street_name
FROM new_companies
-- For recently founded companies, Claude will search for current address
```

### Cost Implications

- **Without search:** 1 Claude API call per query
- **With search:** 2 Claude API calls + 1 Brave search
- **Cost:** Approximately 2x Claude cost + $0.003/search
- **Free tier:** 2,000 searches/month from Brave

### When Search is Used

Claude automatically decides when to search based on the query:
- ✅ "Current price of Bitcoin" → Searches
- ✅ "Latest news about X" → Searches
- ✅ "Who is the current CEO of Y" → Searches
- ❌ "What is a database?" → Uses knowledge, no search
- ❌ "Explain Python lists" → Uses knowledge, no search

## Functions Overview

### stps_ask_ai_address - Structured Address Lookup with Web Search

**Signature:**
```sql
stps_ask_ai_address(company_name VARCHAR) → STRUCT(city VARCHAR, postal_code VARCHAR, street_name VARCHAR, street_nr VARCHAR)
stps_ask_ai_address(company_name VARCHAR, model VARCHAR) → STRUCT(...)
```

**Description:**
Looks up the registered business address for a company using web search (when Brave API key is configured) and returns structured address data. This function automatically triggers web search to find current, accurate address information from business registries and official sources.

**Behavior:**
- **With Brave API key:** Automatically searches the web for current business address
- **Without Brave API key:** Uses Claude's training data (may be outdated for recent companies)
- **Output:** Structured data with city, postal_code, street_name, street_nr fields
- **NULL fields:** If a specific field cannot be determined, it will be NULL

**Examples:**

```sql
-- Basic usage (requires API keys configured)
SELECT stps_ask_ai_address('STP Solution GmbH');
-- Result: {city: Karlsruhe, postal_code: 76135, street_name: Brauerstraße, street_nr: 12}

-- Extract individual fields
SELECT
    (stps_ask_ai_address('Deutsche Bank AG')).city AS city,
    (stps_ask_ai_address('Deutsche Bank AG')).postal_code AS plz,
    (stps_ask_ai_address('Deutsche Bank AG')).street_name AS street;

-- Batch processing
SELECT
    company_name,
    (stps_ask_ai_address(company_name)).city,
    (stps_ask_ai_address(company_name)).postal_code,
    (stps_ask_ai_address(company_name)).street_name
FROM companies
WHERE address_missing = true;
```

#### How It Works

This function uses a two-step approach for reliability:

1. **Search Step:** Retrieves address information using web search (if Brave API key configured) or training data. Claude responds naturally with explanatory text and context.

2. **Parsing Step:** Extracts structured components (city, postal_code, street_name, street_nr) from the natural language result into JSON format.

**Why Two Steps?**

The two-step approach works with Claude's natural behavior instead of constraining it with strict "JSON-only" requirements. When Claude uses the web_search tool, it naturally includes explanatory text like "Based on my web search, I found...". The first step allows this natural behavior, then the second step cleanly extracts the structured data.

**API Calls:** 2 per lookup (search + parse)
**Latency:** Typically 3-5 seconds per address
**Cost:** ~$0.002 per lookup with Claude Sonnet 4.5 (includes 2 Claude API calls + 1 Brave search)

**Web Search Behavior:**
This function is equivalent to:
```sql
SELECT stps_ask_ai(company_name, 'make a websearch and look for business address')
```
...but with structured JSON output instead of natural language text.

**Performance Tip:** For batch processing, consider caching results to avoid duplicate lookups.

### `stps_ask_ai(context, prompt[, model][, max_tokens])`

Query Anthropic's Claude with context and a prompt - returns free-form text response.

**Parameters:**
- `context` (VARCHAR, required): Background information or data to provide context
- `prompt` (VARCHAR, required): The question or instruction for Claude
- `model` (VARCHAR, optional): Claude model to use (default: `claude-sonnet-4-5-20250929`)
- `max_tokens` (INTEGER, optional): Maximum response length (default: 1000)

**Returns:** VARCHAR - The AI-generated response

**Available Models:** See [Available Models](#available-models) section above for details.

### `stps_set_api_key(api_key)`

Configure the Anthropic API key for the current session.

**Parameters:**
- `api_key` (VARCHAR, required): Your Anthropic API key

**Returns:** VARCHAR - Confirmation message

## Configuration

### Priority Order

The extension looks for API keys in this order:

1. **Session key** set via `stps_set_api_key()` (highest priority)
2. **Environment variable** `ANTHROPIC_API_KEY`
3. **Config file** at `~/.stps/anthropic_api_key`

### Security Best Practices

⚠️ **Never commit API keys to version control!**

**For development:**
```sql
-- Set once per session
SELECT stps_set_api_key('sk-ant-...');
```

**For production:**
```bash
# In your deployment script or .bashrc
export ANTHROPIC_API_KEY='sk-ant-...'
```

**For persistent local use:**
```bash
# One-time setup
mkdir -p ~/.stps
echo 'sk-ant-...' > ~/.stps/anthropic_api_key
chmod 600 ~/.stps/anthropic_api_key  # Restrict permissions
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
        'claude-sonnet-4-5-20250929',
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
        'claude-sonnet-4-5-20250929',
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
        'claude-sonnet-4-5-20250929',
        500
    ) AS description_en
FROM products
WHERE description_en IS NULL;
```

### Example 7: Using Claude Opus for Complex Reasoning

```sql
-- Use Claude Opus for complex financial analysis
SELECT
    company_name,
    revenue_2023,
    revenue_2022,
    profit_margin,
    stps_ask_ai(
        'Company: ' || company_name || ', Revenue 2023: ' || revenue_2023::VARCHAR ||
        ', Revenue 2022: ' || revenue_2022::VARCHAR || ', Profit Margin: ' || profit_margin::VARCHAR,
        'Analyze this company''s financial health and growth trajectory. Provide a brief assessment (2-3 sentences).',
        'claude-opus-4-5-20251101',  -- Use more capable model
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
-- Use claude-sonnet-4-5-20250929 for simple tasks
SELECT stps_ask_ai(name, 'Categorize: food or drink?', 'claude-sonnet-4-5-20250929', 10);
```

**❌ Bad: Use Claude Opus unnecessarily**
```sql
-- Overkill and expensive for simple classification
SELECT stps_ask_ai(name, 'Categorize: food or drink?', 'claude-opus-4-5-20251101', 1000);
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
| claude-sonnet-4-5-20250929 | $0.0005 | $0.0015 |
| claude-opus-4-5-20251101 | $0.03 | $0.06 |
| claude-opus-4-5-20251101-turbo | $0.01 | $0.03 |

### Cost Example

Processing 1,000 rows with claude-sonnet-4-5-20250929:
- Average context: 100 tokens
- Average prompt: 20 tokens
- Average response: 50 tokens
- Total: ~170 tokens per row
- Cost: 1,000 rows × 170 tokens / 1,000 × ($0.0005 + $0.0015) ≈ **$0.34**

### Cost Optimization Tips

1. **Use claude-sonnet-4-5-20250929** for most tasks (20-60x cheaper than Claude Opus)
2. **Limit `max_tokens`** to only what you need
3. **Process in batches** and cache results
4. **Keep context concise** - extract only relevant data
5. **Use WHERE clauses** to filter before processing
6. **Monitor usage** on [Anthropic Console](https://console.anthropic.com/)

## Troubleshooting

### Error: "API key not configured"

**Solution:**
```sql
-- Check if key is set
SELECT stps_ask_ai('test', 'hello');  -- Will show error

-- Set the key
SELECT stps_set_api_key('sk-your-key-here');

-- Or use environment variable
-- In terminal: export ANTHROPIC_API_KEY='sk-ant-your-key-here'
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

### Error: "Anthropic API returned error: Rate limit exceeded"

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

### Error: "Could not parse response from Anthropic API"

**Causes:**
- API endpoint changed
- Network issues
- Malformed request

**Solution:**
```sql
-- Check Anthropic status: https://status.anthropic.com/
-- Verify API key is valid
-- Try again with simpler prompt
```

### Response is "ERROR: ..."

Common errors and solutions:

| Error Message | Solution |
|--------------|----------|
| `insufficient_quota` | Add credits to Anthropic account |
| `invalid_api_key` | Check API key is correct |
| `model_not_found` | Use valid model name (claude-sonnet-4-5-20250929, claude-opus-4-5-20251101) |
| `context_length_exceeded` | Reduce context size or max_tokens |

## Requirements

- **curl** or **wget** must be installed
- **Anthropic API key** (get from [console.anthropic.com](https://console.anthropic.com))
- **Internet connection**
- **Anthropic account with credits**

## Security Notes

🔒 **API Key Security:**
- Never hardcode API keys in SQL queries that are saved
- Use environment variables or config files for persistent keys
- Restrict file permissions on `~/.stps/anthropic_api_key` (chmod 600)
- Rotate keys periodically
- Monitor usage on Anthropic Console

🔒 **Data Privacy:**
- Data sent to Anthropic is subject to their [privacy policy](https://www.anthropic.com/legal/privacy)
- Do not send sensitive personal data (PII) without proper consent
- Consider data residency requirements for your jurisdiction
- Review Anthropic's [data usage policies](https://www.anthropic.com/legal/commercial-terms)

## Additional Resources

- [Anthropic API Documentation](https://docs.anthropic.com/en/api)
- [Anthropic Pricing](https://www.anthropic.com/pricing)
- [Best Practices for Prompt Engineering](https://docs.anthropic.com/en/docs/prompt-engineering)
- [Anthropic Usage Policies](https://www.anthropic.com/legal/aup)

## Support

For issues specific to the STPS extension:
- GitHub: [https://github.com/Arengard/stps-extension/issues](https://github.com/Arengard/stps-extension/issues)

For Anthropic API issues:
- Anthropic Support: [https://support.anthropic.com/](https://support.anthropic.com/)
- Status Page: [https://status.anthropic.com/](https://status.anthropic.com/)
