# CiteConnect Pipeline Configuration Guide

This guide explains how to configure API keys, search terms, and paper limits for the CiteConnect data pipeline.

## Table of Contents

1. [Multiple Semantic Scholar API Keys](#multiple-semantic-scholar-api-keys)
2. [Search Terms Configuration](#search-terms-configuration)
3. [Paper Limits Configuration](#paper-limits-configuration)
4. [Complete Configuration Example](#complete-configuration-example)
5. [How the DAG Uses These Settings](#how-the-dag-uses-these-settings)
6. [Restarting After Changes](#restarting-after-changes)

---

## Multiple Semantic Scholar API Keys

### Why Multiple Keys?

- **3-4x Faster Collection**: Parallel API calls across multiple keys
- **Better Rate Limit Handling**: Distribute load across keys
- **Higher Throughput**: More papers collected per minute
- **Resilience**: If one key fails, others continue

### How to Add API Keys

1. **Get Your API Keys**
   - Go to [Semantic Scholar API](https://www.semanticscholar.org/product/api)
   - Sign up or log in
   - Request API keys (you can request multiple keys)
   - Copy your API keys

2. **Add to `.env` File**

   Open your `.env` file and add your API keys:

   ```bash
   # Multiple API keys - comma-separated (NO SPACES around commas)
   SEMANTIC_SCHOLAR_API_KEYS=your_key_1,your_key_2,your_key_3,your_key_4
   ```

   **Example:**
   ```bash
   SEMANTIC_SCHOLAR_API_KEYS=abc123def456,ghi789jkl012,mno345pqr678,stu901vwx234
   ```

3. **Format Rules**

   ✅ **Correct:**
   ```bash
   SEMANTIC_SCHOLAR_API_KEYS=key1,key2,key3
   ```

   ❌ **Wrong (spaces will break it):**
   ```bash
   SEMANTIC_SCHOLAR_API_KEYS=key1, key2, key3  # ❌ Don't add spaces
   SEMANTIC_SCHOLAR_API_KEYS=key1 , key2 , key3  # ❌ Don't add spaces
   ```

4. **Fallback to Single Key**

   If `SEMANTIC_SCHOLAR_API_KEYS` is not set, the system will fall back to:
   ```bash
   SEMANTIC_SCHOLAR_API_KEY=your_single_key_here
   ```

### How It Works

- **API Key Manager** reads `SEMANTIC_SCHOLAR_API_KEYS` from environment
- **Automatic Rotation**: Keys are rotated in round-robin fashion (thread-safe)
- **Parallel Collection**: Each API key can be used simultaneously
- **Rate Limit Handling**: If one key hits rate limits, others continue

### Verification

After restarting Airflow, check the logs:

```bash
docker compose logs airflow-scheduler | grep "API key manager"
```

You should see:
```
✅ Initialized API key manager with 3 key(s)
```

---

## Search Terms Configuration

### How Search Terms Are Configured

**The DAG does NOT hardcode search terms.** Instead, it reads from the `SEARCH_TERMS` environment variable.

### Setting Search Terms

1. **Add to `.env` File**

   ```bash
   # Search terms - comma-separated
   SEARCH_TERMS=finance,quantum computing,healthcare,computer vision
   ```

2. **Format Rules**

   - Separate multiple terms with commas
   - Spaces around commas are automatically trimmed
   - Each term can contain spaces (e.g., "quantum computing")
   - Terms are case-sensitive

   **Example:**
   ```bash
   SEARCH_TERMS=machine learning,deep learning,neural networks,artificial intelligence
   ```

3. **Default Value**

   If `SEARCH_TERMS` is not set, the DAG uses:
   ```python
   'finance, quantum computing, healthcare'
   ```

### Where It's Used

The search terms are read in the `test_paper_collection` task:

```python
# In dags/test_dag.py, test_paper_collection() function
search_terms_env = os.getenv('SEARCH_TERMS', 'finance, quantum computing, healthcare')
search_terms = [term.strip() for term in search_terms_env.split(',')]
```

### Example Configurations

**Small test run:**
```bash
SEARCH_TERMS=machine learning
```

**Medium collection:**
```bash
SEARCH_TERMS=finance,quantum computing,healthcare
```

**Large collection:**
```bash
SEARCH_TERMS=machine learning,deep learning,neural networks,computer vision,natural language processing,reinforcement learning
```

---

## Paper Limits Configuration

### How Paper Limits Are Configured

**The DAG does NOT hardcode paper limits.** Instead, it reads from the `PAPERS_PER_TERM` environment variable.

### Setting Paper Limits

1. **Add to `.env` File**

   ```bash
   # Number of papers to collect per search term
   PAPERS_PER_TERM=100
   ```

2. **Format Rules**

   - Must be a positive integer
   - No commas or spaces
   - Applied to ALL search terms equally

3. **Default Value**

   If `PAPERS_PER_TERM` is not set, the DAG uses:
   ```python
   5  # papers per term
   ```

### Where It's Used

The paper limit is read in the `test_paper_collection` task:

```python
# In dags/test_dag.py, test_paper_collection() function
limit = int(os.getenv('PAPERS_PER_TERM', '5'))
```

### Example Configurations

**Quick test:**
```bash
PAPERS_PER_TERM=5
```

**Small collection:**
```bash
PAPERS_PER_TERM=50
```

**Large collection:**
```bash
PAPERS_PER_TERM=500
```

**Very large collection:**
```bash
PAPERS_PER_TERM=1000
```

### Total Papers Collected

The total number of papers collected is:
```
Total Papers = (Number of Search Terms) × (PAPERS_PER_TERM)
```

**Example:**
- `SEARCH_TERMS=machine learning,deep learning,neural networks` (3 terms)
- `PAPERS_PER_TERM=100`
- **Total: 300 papers**

---

## Complete Configuration Example

Here's a complete example `.env` configuration:

```bash
# ============================================
# SEMANTIC SCHOLAR API KEYS
# ============================================
# Multiple keys for parallelization (recommended)
SEMANTIC_SCHOLAR_API_KEYS=abc123def456ghi789,xyz987uvw654rst321,qwe456asd789zxc123

# Single key fallback (only used if SEMANTIC_SCHOLAR_API_KEYS is empty)
SEMANTIC_SCHOLAR_API_KEY=

# ============================================
# SEARCH TERMS
# ============================================
# Comma-separated list of search terms
SEARCH_TERMS=machine learning,deep learning,neural networks,computer vision

# ============================================
# PAPER LIMITS
# ============================================
# Number of papers to collect per search term
PAPERS_PER_TERM=100

# ============================================
# OTHER CONFIGURATION
# ============================================
GCS_BUCKET_NAME=citeconnect-test-bucket
GCS_PROJECT_ID=strange-calling-476017-r5
AIRFLOW_PROJ_DIR=.
```

---

## How the DAG Uses These Settings

### Configuration Flow

1. **Environment Variables** → Set in `.env` file
2. **Docker Compose** → Passes variables to Airflow containers (see `docker-compose.yaml`)
3. **DAG Task** → `test_paper_collection()` reads from `os.getenv()`
4. **Pipeline** → Uses settings for collection and processing

### Code Location

The configuration is read in `dags/test_dag.py`:

```python
def test_paper_collection():
    from src.DataPipeline.Ingestion.main import collect_and_process_pipeline
    import os
    
    # Get search terms from environment variable or use default
    search_terms_env = os.getenv('SEARCH_TERMS', 'finance, quantum computing, healthcare')
    search_terms = [term.strip() for term in search_terms_env.split(',')]
    limit = int(os.getenv('PAPERS_PER_TERM', '5'))
    
    # Use async pipeline with these settings
    results = collect_and_process_pipeline(
        search_terms=search_terms,
        limit=limit,
        raw_output_dir="/tmp/test_data/raw",
        processed_output_dir="/tmp/test_data/processed",
        use_async=True
    )
```

### API Key Usage

API keys are managed by `APIKeyManager` in `src/DataPipeline/Ingestion/api_key_manager.py`:

```python
# Automatically reads from SEMANTIC_SCHOLAR_API_KEYS or SEMANTIC_SCHOLAR_API_KEY
api_key_manager = APIKeyManager()

# Keys are rotated automatically during parallel collection
api_key = api_key_manager.get_key()
```

---

## Restarting After Changes

### When to Restart

You need to restart Airflow when you change:
- ✅ `SEMANTIC_SCHOLAR_API_KEYS` or `SEMANTIC_SCHOLAR_API_KEY`
- ✅ `SEARCH_TERMS`
- ✅ `PAPERS_PER_TERM`

### How to Restart

**Option 1: Full Restart (Recommended)**
```bash
docker compose down
docker compose up -d
```

**Option 2: Restart Scheduler Only (Faster)**
```bash
docker compose restart airflow-scheduler
```

**Option 3: Restart All Airflow Services**
```bash
docker compose restart airflow-scheduler airflow-webserver
```

### Verification After Restart

1. **Check API Keys:**
   ```bash
   docker compose logs airflow-scheduler | grep "API key manager"
   ```
   Should show: `✅ Initialized API key manager with N key(s)`

2. **Check Environment Variables:**
   ```bash
   docker compose exec airflow-scheduler env | grep -E "SEARCH_TERMS|PAPERS_PER_TERM|SEMANTIC_SCHOLAR"
   ```

3. **Run the DAG:**
   - Go to Airflow UI: http://localhost:8081
   - Trigger the `test_citeconnect` DAG
   - Check logs to see your configured values

---

## Troubleshooting

### Issue: Search terms not updating

**Solution:**
1. Check `.env` file has `SEARCH_TERMS` set correctly
2. Restart Airflow: `docker compose restart airflow-scheduler`
3. Check logs: `docker compose logs airflow-scheduler | grep "Search terms"`

### Issue: Paper limit not changing

**Solution:**
1. Check `.env` file has `PAPERS_PER_TERM` set (must be integer)
2. Restart Airflow: `docker compose restart airflow-scheduler`
3. Check logs: `docker compose logs airflow-scheduler | grep "Papers per term"`

### Issue: API keys not being used

**Solution:**
1. Check `.env` file has `SEMANTIC_SCHOLAR_API_KEYS` set (no spaces around commas)
2. Restart Airflow: `docker compose down && docker compose up -d`
3. Check logs: `docker compose logs airflow-scheduler | grep "API key manager"`

### Issue: Default values being used

**Solution:**
- Make sure environment variables are set in `.env` file
- Make sure `docker-compose.yaml` passes them to containers (already configured)
- Restart Airflow after changes

---

## Security Notes

⚠️ **Important:**
- Never commit `.env` file to git
- Keep API keys secret
- Rotate keys periodically
- Use different keys for different environments (dev/staging/prod)
- Review `.gitignore` to ensure `.env` is excluded

---

## Summary

| Configuration | Environment Variable | Default Value | Format |
|--------------|---------------------|---------------|--------|
| **API Keys** | `SEMANTIC_SCHOLAR_API_KEYS` | None (falls back to `SEMANTIC_SCHOLAR_API_KEY`) | Comma-separated, no spaces |
| **Search Terms** | `SEARCH_TERMS` | `'finance, quantum computing, healthcare'` | Comma-separated |
| **Paper Limit** | `PAPERS_PER_TERM` | `5` | Integer |

**All settings are read from environment variables - nothing is hardcoded in the DAG!**

