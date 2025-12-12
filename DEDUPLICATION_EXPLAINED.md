# 🔄 Deduplication Logic - Complete Explanation

## Overview

The upload system handles duplicates at **3 levels** to ensure data integrity.

---

## 📊 The 3 Levels of Deduplication

```
┌─────────────────────────────────────────────────────────────────┐
│                        LEVEL 1: GCS FILES                       │
│  Multiple parquet files may contain same paper                  │
│                                                                  │
│  processed_v2/                                                   │
│  ├── processed_AI_1234567890.parquet     → Paper X, Y, Z        │
│  ├── processed_Healthcare_1234567891.parquet → Paper Y, Z, W    │
│  └── processed_Quantum_1234567892.parquet    → Paper Z, Q       │
│                                                                  │
│  ACTION: df.drop_duplicates(subset='paperId', keep='first')     │
│  RESULT: Paper X, Y, Z, W, Q (unique)                           │
└─────────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                   LEVEL 2: SUPABASE CHECK                       │
│  Before uploading, check what's already in database             │
│                                                                  │
│  Query: SELECT paper_id FROM papers                             │
│  Result: {Paper X, Paper Y} already exist                       │
│                                                                  │
│  ACTION: if paper_id in existing_ids: skip()                    │
│  RESULT: Only upload Paper Z, W, Q                              │
└─────────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                    LEVEL 3: SQL CONSTRAINT                      │
│  Safety net: even if duplicate slips through                    │
│                                                                  │
│  INSERT INTO papers (paper_id, ...)                             │
│  VALUES ('paper_z', ...)                                        │
│  ON CONFLICT (paper_id) DO NOTHING                              │
│                                                                  │
│  ACTION: Database rejects duplicates automatically              │
│  RESULT: Only unique papers inserted                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Example Scenario

### **Setup:**
```
GCS Files:
├── processed_AI_12345.parquet
│   └── Papers: A, B, C, D
│
├── processed_AI_67890.parquet (from mitigation)
│   └── Papers: C, D, E, F
│
└── processed_Healthcare_11111.parquet
    └── Papers: D, E, G, H

Supabase Database (already has):
└── Papers: A, B, D, G
```

### **Upload Process:**

#### **Step 1: Load from GCS**
```python
Load all 3 files:
Total papers loaded: 12
  - A, B, C, D (from file 1)
  - C, D, E, F (from file 2)
  - D, E, G, H (from file 3)
```

#### **Step 2: Level 1 Deduplication (GCS)**
```python
df_all = pd.concat([file1, file2, file3])
df_all = df_all.drop_duplicates(subset='paperId', keep='first')

Duplicates within GCS:
  - Paper C: appears 2 times → Keep first
  - Paper D: appears 3 times → Keep first
  - Paper E: appears 2 times → Keep first

Result: A, B, C, D, E, F, G, H (8 unique papers)
Removed: 4 duplicates
```

#### **Step 3: Level 2 Deduplication (Supabase Check)**
```python
existing_ids = {A, B, D, G}  # From database

For each paper:
  - A: in existing_ids → SKIP
  - B: in existing_ids → SKIP
  - C: NOT in existing_ids → UPLOAD
  - D: in existing_ids → SKIP
  - E: NOT in existing_ids → UPLOAD
  - F: NOT in existing_ids → UPLOAD
  - G: in existing_ids → SKIP
  - H: NOT in existing_ids → UPLOAD

Papers to upload: C, E, F, H (4 papers)
Skipped: A, B, D, G (4 papers already in DB)
```

#### **Step 4: Level 3 Deduplication (SQL)**
```sql
INSERT INTO papers (paper_id, ...) VALUES ('C', ...)
ON CONFLICT (paper_id) DO NOTHING;  -- Paper C inserted

INSERT INTO papers (paper_id, ...) VALUES ('E', ...)
ON CONFLICT (paper_id) DO NOTHING;  -- Paper E inserted

INSERT INTO papers (paper_id, ...) VALUES ('F', ...)
ON CONFLICT (paper_id) DO NOTHING;  -- Paper F inserted

INSERT INTO papers (paper_id, ...) VALUES ('H', ...)
ON CONFLICT (paper_id) DO NOTHING;  -- Paper H inserted
```

### **Final Result:**
```
Supabase Database now has:
└── Papers: A, B, C, D, E, F, G, H (8 total)

Upload Statistics:
├── Papers in GCS (after dedupe): 8
├── Inserted: 4 (C, E, F, H)
├── Skipped: 4 (A, B, D, G)
└── Failed: 0
```

---

## 🔬 Testing Deduplication

### **Test Script:**
```bash
# Run test with 10 papers
python test_supabase_upload.py
```

### **What the test does:**

```
1. First Upload (10 papers):
   ├── Load 10 papers from GCS
   ├── Check Supabase for existing papers
   ├── Upload new papers
   └── Result: Some inserted, some skipped

2. Second Upload (same 10 papers):
   ├── Load same 10 papers from GCS
   ├── Check Supabase (now includes first upload)
   ├── All 10 papers already exist
   └── Result: 0 inserted, 10 skipped ✅

Expected Output:
  First run: Inserted: X, Skipped: Y
  Second run: Inserted: 0, Skipped: 10
  → Deduplication working correctly!
```

---

## 📋 Code Walkthrough

### **Level 1: GCS Deduplication**
```python
# File: upload_papers_to_supabase.py, Line ~236

# Load all parquet files
dataframes = self.load_papers_from_gcs()

# Combine into single DataFrame
df_all = pd.concat(dataframes, ignore_index=True)

# Deduplicate by paperId
original_count = len(df_all)
df_all = df_all.drop_duplicates(subset='paperId', keep='first')
dedupe_count = original_count - len(df_all)

if dedupe_count > 0:
    logger.info(f"🔄 Removed {dedupe_count} duplicate papers")
```

### **Level 2: Supabase Check**
```python
# File: upload_papers_to_supabase.py, Line ~41

async def get_existing_paper_ids(self, conn) -> set:
    """Get set of paper IDs already in database"""
    rows = await conn.fetch("SELECT paper_id FROM papers")
    existing_ids = {row['paper_id'] for row in rows}
    return existing_ids

# Then in upload_papers_batch(), Line ~177:
for paper in papers:
    paper_id = paper.get('paper_id')
    
    if paper_id in existing_ids:
        stats['skipped'] += 1
        continue  # Skip this paper
    
    # Only upload if not in existing_ids
    await conn.execute(insert_query, *values)
```

### **Level 3: SQL Constraint**
```python
# File: upload_papers_to_supabase.py, Line ~190

query = f"""
    INSERT INTO papers ({', '.join(columns)})
    VALUES ({', '.join(placeholders)})
    ON CONFLICT (paper_id) DO NOTHING
"""

await conn.execute(query, *values)
```

**SQL Constraint in Supabase:**
```sql
-- The papers table has:
paper_id TEXT UNIQUE NOT NULL

-- This creates a UNIQUE constraint
-- ON CONFLICT (paper_id) DO NOTHING means:
-- "If paper_id already exists, silently ignore this insert"
```

---

## 🎯 Why 3 Levels?

### **Level 1 (GCS) - Performance**
- **Why:** Reduces data to upload
- **Example:** 10,000 papers across 5 files → 8,000 unique
- **Benefit:** 20% less data to process, faster upload

### **Level 2 (Supabase Check) - Efficiency**
- **Why:** Avoid unnecessary INSERT queries
- **Example:** 8,000 unique papers, 6,000 already in DB → only upload 2,000
- **Benefit:** 75% fewer database operations

### **Level 3 (SQL Constraint) - Safety**
- **Why:** Guarantee data integrity even if logic fails
- **Example:** Race condition, concurrent uploads
- **Benefit:** Database ensures no duplicates ever exist

---

## 🧪 Test Outputs

### **Expected Output (First Run):**
```
================================================================================
🚀 STARTING PAPER UPLOAD TO SUPABASE
================================================================================

📥 Loading papers from GCS...
📦 Found 3 parquet files
  ✅ Loaded 500 papers from processed_AI_*.parquet
  ✅ Loaded 450 papers from processed_Healthcare_*.parquet
  ✅ Loaded 380 papers from processed_Quantum_*.parquet

🔄 Removed 45 duplicate papers (Level 1)
📊 Total unique papers in GCS: 1285

⚠️ Limited to first 10 papers (test mode)

🔌 Connecting to Supabase...
📊 Found 0 existing papers in database

📤 Uploading in batches of 10...
  Batch 1/1 (10 papers)...
    ✅ Inserted: 10, Skipped: 0, Failed: 0 (Level 2)

================================================================================
✅ UPLOAD COMPLETE
================================================================================
📊 Papers in GCS: 10
✅ Papers inserted: 10
⏭️  Papers skipped: 0
❌ Papers failed: 0
================================================================================

🔄 Running second upload to test deduplication...

📊 Found 10 existing papers in database

📤 Uploading in batches of 10...
  Batch 1/1 (10 papers)...
    ✅ Inserted: 0, Skipped: 10, Failed: 0 (Level 2)

📊 Second Upload Results:
   Inserted: 0
   Skipped: 10

✅ DEDUPLICATION VERIFIED!
   → All 10 papers were correctly identified as duplicates
   → No papers were inserted on second run
```

---

## 🔍 Debugging Deduplication

### **Check GCS Duplicates:**
```python
# In Python
import pandas as pd

df1 = pd.read_parquet('gs://bucket/processed_v2/file1.parquet')
df2 = pd.read_parquet('gs://bucket/processed_v2/file2.parquet')

# Find duplicates
df_all = pd.concat([df1, df2])
duplicates = df_all[df_all.duplicated(subset='paperId', keep=False)]
print(f"Duplicate papers: {len(duplicates)}")
print(duplicates[['paperId', 'title']])
```

### **Check Supabase:**
```sql
-- Count total papers
SELECT COUNT(*) FROM papers;

-- Find duplicates (should be 0!)
SELECT paper_id, COUNT(*) as count
FROM papers
GROUP BY paper_id
HAVING COUNT(*) > 1;

-- Check specific paper
SELECT * FROM papers WHERE paper_id = 'xyz123';
```

---

## ✅ Guarantees

With all 3 levels, the system guarantees:

1. ✅ **No duplicates within GCS upload batch**
2. ✅ **No duplicates between GCS and existing Supabase data**
3. ✅ **No duplicates in database even if logic fails**
4. ✅ **Concurrent uploads won't create duplicates**
5. ✅ **Safe to run upload multiple times**

---

## 🚀 Run the Test

```bash
# Make sure .env has Supabase credentials
python test_supabase_upload.py

# Expected:
# ✅ First run: Inserts 10 papers
# ✅ Second run (automatic): Skips all 10 papers
# ✅ Deduplication verified!
```

---

**Status:** ✅ Triple-redundant deduplication implemented and tested

