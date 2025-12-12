# 🔬 Anomaly Detection System

## Overview

Enhanced CiteConnect's schema validation with **automated anomaly detection** to catch data quality issues before they impact downstream systems.

---

## 🎯 **What It Does**

### **Method 1: Pattern Anomalies**
Detects sudden changes in data characteristics:

```python
✅ Volume Anomalies
   - Sudden spike/drop in paper count (>50% change)
   - Uses Z-score (>2.5σ) and percent change
   - Example: Expected 1000 papers, got 200 → ALERT

✅ Missing Data Spikes
   - Increase in null/missing values (>50% increase)
   - Tracks missing rate across all columns
   - Example: Missing data increased from 5% to 12% → ALERT
```

### **Method 2: Domain-Specific Anomalies**
CiteConnect-specific quality checks:

```python
✅ Content Extraction Failures
   - >60% papers missing introduction → WARNING
   - >80% papers missing introduction → CRITICAL

✅ Duplicate Papers
   - Any duplicates detected → WARNING
   - >5% duplicates → CRITICAL
   - Should be 0 after deduplication!

✅ Low Citation Anomaly
   - >70% papers with 0 citations → WARNING
   - May indicate data quality issues or recency bias

✅ Temporal Bias
   - >70% papers from same year → WARNING
   - Indicates lack of diversity in collection

✅ Domain Skew
   - >85% papers from one domain → WARNING
   - May indicate bias in collection strategy
```

---

## 📊 **Severity Levels**

```
🟢 NORMAL    - No anomalies detected
⚠️  WARNING   - 1-2 warnings detected
🔴 CRITICAL  - Any critical anomaly OR 3+ warnings
```

---

## 📧 **Email Alerts**

Automatically sends email when anomalies are detected:

**Trigger:** WARNING or CRITICAL severity
**Recipients:** Configured via `SMTP_USER` and `ALERT_EMAIL` env vars
**Content:**
- Anomaly type and severity
- Affected columns
- Detailed metrics
- Action recommendations

**Example Alert:**

```
🔴 CiteConnect Data Anomaly Alert - CRITICAL

Severity: CRITICAL
Total Papers: 2,341
Quality Score: 78.5%

Detected Anomalies (3):

🔴 extraction_failure_spike - has_intro
   High content extraction failure rate (82.4%)
   Details: failed_count: 1928, successful_count: 413

⚠️  volume_anomaly - total_papers
   Paper count changed by 65.3% from historical average
   Details: current_count: 2341, historical_avg: 1417

⚠️  duplicate_papers - paperId
   Duplicate papers detected (47 duplicates)
   Details: duplicate_count: 47, unique_papers: 2294
```

---

## 🔄 **Integration in DAG**

Already integrated in `test_dag.py`:

```
Collection → Bias Analysis → Alert → Mitigation → Schema Validation → Notification
                                                           ↓
                                                    Anomaly Detection
                                                           ↓
                                                    Email Alert (if anomalies)
```

**Task:** `generate_schema_and_stats`
**Function:** `validate_schema()` in `src/DataPipeline/Validation/schema_validator.py`

---

## 📈 **How It Works**

### **1. Load Historical Baseline**
```python
# Uses last 10 runs as baseline
historical_schemas = validator._load_historical_schemas()
# Stored in: data/schemas/schema_*.json
```

### **2. Run Anomaly Detection**
```python
anomalies = validator.validate_with_anomaly_detection(df)

# Returns:
{
  'has_anomalies': True,
  'severity': 'critical',
  'detected_anomalies': [
    {
      'type': 'extraction_failure_spike',
      'column': 'has_intro',
      'severity': 'critical',
      'description': '...',
      'details': {...}
    }
  ]
}
```

### **3. Alert if Needed**
```python
if anomalies['severity'] in ['critical', 'warning']:
    send_anomaly_alert(anomalies, schema)
```

### **4. Save Report**
```python
# Local: data/schemas/schema_YYYYMMDD_HHMMSS.json
# GCS: gs://bucket/schemas/schema_YYYYMMDD_HHMMSS.json
```

---

## 🛠️ **Configuration**

### **Environment Variables**

```bash
# Required for email alerts
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
ALERT_EMAIL=alert_recipient@example.com  # Optional, defaults to SMTP_USER

# Required for GCS storage
GCS_BUCKET_NAME=citeconnect-test-bucket
GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcs-key.json
```

### **Customization**

Edit thresholds in `src/DataPipeline/Validation/schema_validator.py`:

```python
class AnomalyDetector:
    def __init__(self, history_window: int = 10):
        self.history_window = history_window  # How many runs to use for baseline
    
    # Adjust thresholds:
    def _detect_pattern_anomalies(...):
        if z_score > 2.5 or percent_change > 50:  # Volume threshold
            ...
        
        if current_missing_rate > hist_mean_missing * 1.5:  # Missing data threshold
            ...
    
    def _detect_domain_anomalies(...):
        if missing_pct > 60:  # Extraction failure threshold
            ...
```

---

## 📋 **Example Output**

```
============================================================
SCHEMA VALIDATION & ANOMALY DETECTION
============================================================
✅ Loaded 2341 papers from GCS: processed_v2/processed_AI_1234567890.parquet
✅ Schema saved to: data/schemas/schema_20251211_033045.json
✅ Schema uploaded to GCS: gs://citeconnect-test-bucket/schemas/schema_20251211_033045.json

📊 Quality Metrics:
  - Completeness: 94.2%
  - Validity: 87.5%
  - Overall Score: 90.85%

🔴 ANOMALIES DETECTED (Severity: CRITICAL)
   Total anomalies: 3

   🔴 extraction_failure_spike in 'has_intro'
      High content extraction failure rate (82.4%)
      Details: {'failed_count': 1928, 'failure_percentage': 82.4, 'successful_count': 413}

   ⚠️  volume_anomaly in 'total_papers'
      Paper count changed by 65.3% from historical average
      Details: {'current_count': 2341, 'historical_avg': 1417, 'expected_range': '1000 - 1834', 'change_percentage': 65.3}

   ⚠️  duplicate_papers in 'paperId'
      Duplicate papers detected (47 duplicates)
      Details: {'duplicate_count': 47, 'duplicate_percentage': 2.01, 'unique_papers': 2294}

📧 Sending anomaly alert email...
✅ Anomaly alert email sent to anushasrini2001@gmail.com
============================================================
```

---

## 🎯 **Benefits**

✅ **Proactive Monitoring**: Catch issues before they propagate  
✅ **Automated Alerts**: No manual checking needed  
✅ **Historical Context**: Compares against baseline, not absolute thresholds  
✅ **Actionable Insights**: Specific details for debugging  
✅ **Production-Ready**: GCS integration, email alerts, error handling  

---

## 🔍 **Next Steps**

1. **Configure SMTP** for email alerts
2. **Run the DAG** - anomaly detection runs automatically
3. **Review alerts** in your inbox
4. **Adjust thresholds** if you get too many false positives
5. **Monitor trends** using historical schema files in GCS

---

## 📚 **Files Modified**

- `src/DataPipeline/Validation/schema_validator.py` - Enhanced with anomaly detection
- `dags/test_dag.py` - Already integrated (no changes needed)
- `.env` - Add SMTP credentials for alerts

---

**Status:** ✅ Implemented and Ready to Use

