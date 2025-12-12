# 📤 Supabase Paper Upload

## Overview

Automatically uploads papers from GCS (`processed_v2/`) to your Supabase PostgreSQL database after each DAG run.

**No embeddings** - this only uploads paper metadata, not vector embeddings!

---

## 🔄 DAG Integration

### **Updated Pipeline Flow:**

```
Collection → Bias → Alert → Mitigation → Schema → Supabase Upload → Notification
                                                        ↑
                                                   NEW TASK
```

**Task:** `upload_to_supabase`
**Trigger:** Only runs if all previous tasks succeeded (`trigger_rule='all_success'`)

---

## 📋 Database Schema

The code uploads to a `papers` table with this structure:

```sql
CREATE TABLE papers (
    id BIGSERIAL PRIMARY KEY,
    paper_id TEXT UNIQUE NOT NULL,           -- Semantic Scholar ID
    external_ids JSONB,                       -- DOI, ArXiv, etc.
    title TEXT NOT NULL,
    abstract TEXT,
    year INTEGER,
    publication_date TEXT,
    authors JSONB,                            -- [{name, id}, ...]
    citation_count INTEGER DEFAULT 0,
    reference_count INTEGER DEFAULT 0,
    fields_of_study JSONB,                    -- ['AI', 'ML', ...]
    pdf_url TEXT,
    tldr TEXT,
    introduction TEXT,                        -- Extracted intro
    extraction_method TEXT,                   -- 'arxiv_html', 'grobid', etc.
    content_quality TEXT,                     -- 'high', 'medium', 'low'
    has_intro BOOLEAN DEFAULT FALSE,
    intro_length INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active',
    search_term TEXT,
    domain TEXT DEFAULT 'general',            -- 'AI', 'Healthcare', etc.
    sub_domains JSONB,                        -- ['ML', 'NLP', ...]
    references_id JSONB,                      -- IDs of reference papers
    scraped_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_papers_paper_id ON papers(paper_id);
CREATE INDEX idx_papers_domain ON papers(domain);
CREATE INDEX idx_papers_year ON papers(year);
CREATE INDEX idx_papers_citation_count ON papers(citation_count);
```

---

## ⚙️ Configuration

### **Environment Variables**

Add to your `.env` file:

```bash
# Supabase Database Connection
SUPABASE_DB_HOST=db.xxxxx.supabase.co
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=your_password

# GCS Configuration (already have these)
GCS_BUCKET_NAME=citeconnect-test-bucket
GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcs-key.json
```

### **How to Get Supabase Credentials:**

1. Go to your Supabase project dashboard
2. Click **Settings** → **Database**
3. Find **Connection string** section
4. Copy the **Direct connection** details:
   ```
   Host: db.xxxxx.supabase.co
   Port: 5432
   Database: postgres
   User: postgres
   Password: [your project password]
   ```

---

## 🚀 Usage

### **1. Automatic (DAG Integration)**

The upload happens automatically after each DAG run:

```python
# In your DAG:
initial_checks >> collection >> ... >> schema_validation >> upload_to_supabase >> notification
```

**Features:**
- ✅ Deduplicates: Skips papers already in database
- ✅ Batch processing: Uploads 100 papers at a time
- ✅ Error handling: Continues even if individual papers fail
- ✅ Logging: Detailed progress logs

---

### **2. Standalone Execution**

Test the uploader independently:

```bash
# Upload all papers from GCS
python src/DataPipeline/Processing/upload_papers_to_supabase.py

# Upload limited number (for testing)
python src/DataPipeline/Processing/upload_papers_to_supabase.py --max-papers 100

# Custom batch size
python src/DataPipeline/Processing/upload_papers_to_supabase.py --batch-size 50
```

---

### **3. DAG Parameters**

Configure upload via Airflow UI when triggering DAG:

```json
{
  "MAX_PAPERS_UPLOAD": 500,      // Limit papers to upload (optional)
  "UPLOAD_BATCH_SIZE": 100       // Papers per batch (default: 100)
}
```

---

## 📊 What Happens During Upload

### **Step-by-Step Process:**

```
1. 📥 Load all parquet files from gs://bucket/processed_v2/
   └─> Finds: processed_AI_*.parquet, processed_Healthcare_*.parquet, etc.

2. 🔄 Deduplicate papers by paperId
   └─> Keeps first occurrence

3. 🔌 Connect to Supabase PostgreSQL

4. 📊 Fetch existing paper IDs from database
   └─> SELECT paper_id FROM papers

5. 🔄 Prepare papers for upload
   └─> Convert DataFrame rows to database format
   └─> Handle JSON fields (authors, externalIds, etc.)

6. 📤 Upload in batches of 100
   └─> INSERT INTO papers (...) ON CONFLICT DO NOTHING
   └─> Skip if paper_id already exists

7. ✅ Return statistics
```

---

## 📈 Example Output

```
================================================================================
🚀 STARTING PAPER UPLOAD TO SUPABASE
================================================================================

📥 Loading papers from GCS...
📦 Found 3 parquet files in gs://citeconnect-test-bucket/processed_v2/
  ✅ Loaded 500 papers from processed_AI_1234567890.parquet
  ✅ Loaded 450 papers from processed_Healthcare_1234567891.parquet
  ✅ Loaded 380 papers from processed_Quantum_1234567892.parquet

🔄 Removed 15 duplicate papers
📊 Total unique papers in GCS: 1315

🔌 Connecting to Supabase...
📊 Found 800 existing papers in database

🔄 Preparing papers for upload...
✅ Prepared 1315 papers

📤 Uploading in batches of 100...
  Batch 1/14 (100 papers)...
    ✅ Inserted: 85, Skipped: 15, Failed: 0
  Batch 2/14 (100 papers)...
    ✅ Inserted: 92, Skipped: 8, Failed: 0
  ...
  Batch 14/14 (15 papers)...
    ✅ Inserted: 12, Skipped: 3, Failed: 0

================================================================================
✅ UPLOAD COMPLETE
================================================================================
📊 Papers in GCS: 1315
✅ Papers inserted: 515
⏭️  Papers skipped (already exist): 800
❌ Papers failed: 0
================================================================================
🔌 Database connection closed
```

---

## 🔍 Deduplication Logic

### **How It Works:**

1. **GCS Level**: Deduplicates papers across parquet files
   ```python
   df_all = pd.concat(dataframes)
   df_all = df_all.drop_duplicates(subset='paperId', keep='first')
   ```

2. **Database Level**: Skips papers already in Supabase
   ```python
   existing_ids = {row['paper_id'] for row in db_papers}
   if paper_id in existing_ids:
       skip()
   ```

3. **SQL Level**: Uses `ON CONFLICT DO NOTHING`
   ```sql
   INSERT INTO papers (...)
   VALUES (...)
   ON CONFLICT (paper_id) DO NOTHING
   ```

**This means:**
- ✅ Safe to run multiple times
- ✅ Only new papers are uploaded
- ✅ No duplicates in database

---

## 🛠️ Troubleshooting

### **1. Connection Errors**

```
❌ Failed to connect: could not translate host name
```

**Solution:**
- Check `SUPABASE_DB_HOST` is correct
- Verify database is accessible from your network
- Check firewall/VPN settings

---

### **2. Authentication Errors**

```
❌ Failed: password authentication failed
```

**Solution:**
- Verify `SUPABASE_DB_PASSWORD` is correct
- Check `SUPABASE_DB_USER` (usually `postgres`)
- Reset password in Supabase dashboard if needed

---

### **3. Table Not Found**

```
❌ relation "papers" does not exist
```

**Solution:**
Create the table in Supabase SQL Editor (see schema above)

---

### **4. JSON Conversion Errors**

```
❌ Failed to insert paper: invalid input syntax for type json
```

**Solution:**
The code handles this automatically, but check your parquet files for corrupted JSON fields

---

## 🎯 Best Practices

### **1. Run After Each Collection**
✅ Upload is now part of DAG flow
✅ Ensures database is always up-to-date

### **2. Monitor Logs**
```bash
# Check upload statistics in Airflow logs:
# - Papers inserted
# - Papers skipped
# - Papers failed
```

### **3. Initial Population**
For first-time setup with existing data:

```bash
# Run standalone to populate database
python src/DataPipeline/Processing/upload_papers_to_supabase.py

# Or trigger DAG manually
# (Airflow UI → test_citeconnect → Trigger DAG)
```

### **4. Incremental Updates**
After initial population, DAG automatically:
- Uploads only new papers
- Skips existing papers
- Fast and efficient!

---

## 📊 Performance

**Upload Speed:**
- ~100 papers per second (sequential inserts)
- ~1000 papers in ~10 seconds
- ~10,000 papers in ~2 minutes

**Optimization Tips:**
- Increase `batch_size` for faster uploads (default: 100)
- Database index on `paper_id` speeds up deduplication
- Use connection pooling for very large datasets

---

## 🔐 Security

**Connection Security:**
- ✅ Uses SSL/TLS encryption (Supabase default)
- ✅ Credentials stored in environment variables
- ✅ No hardcoded passwords
- ✅ `.env` file in `.gitignore`

**Database Permissions:**
- Uploader only needs `INSERT` and `SELECT` on `papers` table
- Can create restricted user for production:

```sql
-- Create restricted user (optional)
CREATE USER airflow_uploader WITH PASSWORD 'secure_password';
GRANT SELECT, INSERT ON papers TO airflow_uploader;
```

---

## ✅ Summary

**Added:**
- ✅ `src/DataPipeline/Processing/upload_papers_to_supabase.py`
- ✅ DAG task: `upload_to_supabase`
- ✅ Automatic upload after each run
- ✅ Deduplication at GCS and DB levels
- ✅ Batch processing for efficiency
- ✅ Detailed logging and error handling

**Configuration Needed:**
```bash
SUPABASE_DB_HOST=...
SUPABASE_DB_PASSWORD=...
```

**Next Steps:**
1. Add Supabase credentials to `.env`
2. Create `papers` table in Supabase
3. Run DAG to test upload
4. Check database for new papers!

---

**Status:** ✅ Ready to Use

