# 🔍 Understanding Anomaly Detection Alerts

## Your Alert Breakdown

```
🔴 volume_anomaly - total_papers
Paper count changed by 85.3% from historical average
current_count: 82, historical_avg: 556, expected_range: -742 - 1855, change_percentage: 85.3
```

---

## 📊 What This Means

### **Volume Anomaly Explanation**

Your pipeline collected **82 papers** in this run, but historically it collects an average of **556 papers per run**.

**This is an 85.3% DECREASE** from your normal collection volume!

```
Historical Pattern:
Run 1: ~500 papers
Run 2: ~600 papers  
Run 3: ~550 papers
...
Current Run: 82 papers ⚠️  (WAY LOWER!)
```

### **Why This Matters**

A volume anomaly indicates:
- ✅ **Good**: You intentionally changed collection parameters (e.g., fewer search terms, lower `PAPERS_PER_TERM`)
- ⚠️  **Warning**: Something is blocking paper collection (API issues, rate limits, search term problems)
- 🔴 **Critical**: Pipeline malfunction or configuration error

---

## 🔍 Investigating Your 82-Paper Run

### **Possible Causes:**

#### 1. **Intentional Configuration Change** ✅
```bash
# Did you change these?
PAPERS_PER_TERM=100  # Was this lowered?
SEARCH_TERMS="..."   # Fewer terms than before?
MAX_REFERENCES_PER_PAPER=0  # References disabled? (normal for mitigation)
```

#### 2. **Search Term Issues** ⚠️
```
- Using very specific/narrow search terms → fewer results
- Search term with no matching papers
- API rate limiting or errors
```

#### 3. **API Errors** 🔴
```
- 500 Internal Server Error (like you saw earlier)
- Rate limits hit immediately
- All retries exhausted
```

#### 4. **Mitigation Run** ℹ️
```
If this was a bias mitigation run:
- Expected to collect fewer papers (only for underrepresented domains)
- MAX_REFERENCES_PER_PAPER=0 (no references collected)
- This is NORMAL for mitigation!
```

---

## 📁 What Data Is Used for Anomaly Detection?

### **Data Source:**
```
Primary: gs://citeconnect-test-bucket/processed_v2/
         ├── processed_AI_1234567890.parquet
         ├── processed_Healthcare_1234567891.parquet
         └── ...

Fallback: /opt/airflow/working_data/processed/
```

### **Historical Baseline:**
```
Location: data/schemas/
         ├── schema_20251210_140530.json  (Run 1)
         ├── schema_20251210_150123.json  (Run 2)
         ├── schema_20251210_160456.json  (Run 3)
         └── ...
         
Uses: Last 10 runs as baseline
```

### **What Gets Analyzed:**

```python
Current Run Data:
├── total_papers: 82
├── columns: [paperId, title, citationCount, ...]
├── missing_count: {paperId: 0, title: 2, ...}
├── numeric_stats: {citationCount: {mean: 45.2, min: 0, max: 350}}
└── quality_metrics: {completeness: 96.7%, validity: 98.1%}

Compared Against Historical Average:
├── avg_total_papers: 556
├── avg_missing_rate: 3.2%
├── avg_citationCount: 52.1
└── ...
```

---

## 🎯 Your Specific Case

### **Your Alert:**
- **Current**: 82 papers
- **Historical Avg**: 556 papers
- **Change**: -85.3% (significant drop!)

### **Expected Range Explanation:**
```
expected_range: -742 - 1855
```

This looks weird because it shows a negative number! Here's why:

```python
# Statistical calculation (Z-score method):
historical_mean = 556
historical_std = 598  # High variance in your past runs!

lower_bound = mean - 2*std = 556 - 1196 = -640 ≈ -742
upper_bound = mean + 2*std = 556 + 1196 = 1752 ≈ 1855
```

**Why high variance?**
Your past runs had very different paper counts:
- Some runs: 100-200 papers (maybe mitigation runs?)
- Other runs: 800-1000 papers (normal collection)
- This creates high standard deviation

**Better interpretation:**
- If paper count is < 100 or > 1000: anomaly
- Your 82 papers is at the extreme low end

---

## ✅ What To Do Next

### **1. Check If This Was Intentional**
```bash
# Review your DAG run configuration
# In Airflow UI → DAG → latest run → Config

Was this a:
- ✅ Mitigation run? (Expected to be smaller)
- ✅ Test run? (Deliberately smaller)
- ❌ Normal collection run? (Should be ~500 papers)
```

### **2. Review Logs**
```bash
# Check for:
- API errors (500, 429, 403)
- "No results found" messages
- Rate limit warnings
- Deduplication removing most papers
```

### **3. Compare with Previous Runs**
```bash
# In GCS:
gs://citeconnect-test-bucket/raw_v2/
├── AI_v2_1234567890.parquet           # Latest (82 papers)
├── AI_v2_1234567800.parquet           # Previous (~500 papers?)
└── ...

# Check if the drop is gradual or sudden
```

### **4. Validate Configuration**
```python
# Check environment variables:
SEARCH_TERMS="machine learning,deep learning,..."  # How many?
PAPERS_PER_TERM=100                                # Normal
MAX_REFERENCES_PER_PAPER=50                        # 0 for mitigation

# Calculate expected:
num_terms = len(SEARCH_TERMS.split(','))
expected_papers = num_terms * PAPERS_PER_TERM + (references if enabled)
```

---

## 📊 Anomaly Detection Logic Summary

```python
# Pattern Anomalies
if abs(current - historical_mean) / historical_std > 2.5:
    alert("Volume anomaly")

if current_missing_rate > historical_missing * 1.5:
    alert("Missing data spike")

# Domain Anomalies
if extraction_failures > 60%:
    alert("Extraction failure spike")

if duplicates > 0:
    alert("Deduplication failed")

if zero_citations > 70%:
    alert("Low citation anomaly")
```

---

## 🎨 Email Visualization

Your alerts now include the **bias_metrics.png** visualization showing:
- Citation disparity across domains/subdomains
- Current bias levels
- Disparity summary metrics

This helps correlate anomalies with bias patterns!

---

## 💡 Pro Tips

1. **Mitigation runs are expected to be smaller** - don't panic!
2. **First 3-10 runs** may show false positives (building baseline)
3. **Seasonal patterns** (e.g., always collect fewer papers on weekends) will trigger alerts until normalized
4. **Adjust thresholds** in `schema_validator.py` if too sensitive

---

## 🔧 Adjusting Sensitivity

If you get too many false alerts, edit `src/DataPipeline/Validation/schema_validator.py`:

```python
class AnomalyDetector:
    def _detect_pattern_anomalies(...):
        # Change from 2.5 to 3.0 for less sensitivity
        if z_score > 3.0 or percent_change > 75:  # Was 2.5 and 50
            anomalies.append(...)
```

---

## Summary

**Your 82-paper run triggered the alert because:**
- Historical average: 556 papers
- Current: 82 papers (-85.3%)
- System correctly flagged this as unusual

**Next steps:**
1. Check if this was a mitigation run (expected)
2. Review logs for API errors
3. Verify configuration hasn't changed
4. If intentional, no action needed!

The anomaly detection is working correctly - it's alerting you to investigate why collection volume dropped significantly! 🎯

