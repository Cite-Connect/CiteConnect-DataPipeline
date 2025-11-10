# 🎯 CiteConnect – Data Bias Detection Module

This module detects and analyzes bias within the CiteConnect research paper dataset stored in Google Cloud Storage (GCS).  
It integrates seamlessly with the main Airflow pipeline and supports visualization, slicing, and mitigation of multiple bias types.

---

## 🧩 Overview

The scripts in this folder perform **bias detection, slicing, and fairness analysis** on research data collected from Semantic Scholar.  
They connect directly to GCS (bucket: `citeconnect-test-bucket/raw`) and analyze metadata fields such as `year`, `fieldsOfStudy`, and `citationCount`.

---

## ⚙️ Bias Types Analyzed

| Bias Type | Description | Metric Used |
|------------|--------------|--------------|
| **Temporal Bias** | Detects concentration of papers in certain years | Distribution by `year` |
| **Field Bias** | Identifies overrepresented academic domains | Count of `fieldsOfStudy` |
| **Citation Bias** | Measures imbalance in high- vs low-cited papers | `citationCount` statistics |
| **Abstract / Quality Bias** | Evaluates quality of extracted content | `intro_length` + `content_quality` |

---

## 🧮 Data Slicing & Mitigation

- Uses **Fairlearn** to create slices (e.g., by field or year)  
- Computes group-wise citation averages and fairness disparities  
- Applies simple balancing or threshold-based resampling to mitigate bias  
- Generates JSON summaries and plots for review

---

## 📂 File Descriptions

| File | Purpose |
|------|----------|
| `explore_gcs.py` | Connects to GCS and merges all `.parquet` research files |
| `bias_analysis_connect_gcs.py` | Loads a single file from GCS for initial testing |
| `inspect_single_file.py` | View schema and sample rows for one file |
| `analyze_bias.py` | Performs Temporal, Field, Citation & Quality bias analysis with plots |
| `slicing_bias_analysis.py` | Performs slicing, mitigation, and fairness disparity computation |
| `send_test_email.py` | Verifies SMTP setup for automated DAG alerts |
| `bias_summary.json` / `slice_summary.json` / `fairness_disparity.json` | Output summaries from analysis |
| `field_slicing_bias.png` | Visualization example from slicing analysis |

---

## 🧠 Integration with Airflow

These scripts are integrated into the **CiteConnect Airflow DAG** under:
- **Phase 2:** Bias detection & visualization  
- **Phase 3:** Data slicing & fairness monitoring  
- **Phase 4:** Automated alerts for bias thresholds via email  

To automate email alerts, ensure SMTP credentials are added in the `.env` file and the DAG uses `send_test_email.py` logic for notifications.

---

## 📦 Dependencies

Install from the project’s main `requirements.txt`:
```bash
pip install -r requirements.txt
```

Key packages used:
```
pandas, numpy, seaborn, fairlearn, google-cloud-storage, python-dotenv, matplotlib
```

---

## 📊 Outputs

All generated outputs are saved to:
```
databias/plots/
databias/slices/
```

Example results:
- `bias_summary.json` → Summary of 4 bias types
- `slice_summary.json` → Group-wise slicing results
- `fairness_disparity.json` → Fairness ratio and difference metrics
- `field_slicing_bias.png` → Bar plot of bias across research domains

---

## 🧾 Author
**Dhiksha Mathanagopal**  
CiteConnect Data Pipeline – Bias Detection Component  
Northeastern University | 2025
