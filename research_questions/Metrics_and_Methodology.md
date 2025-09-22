## Metrics and Methodology (for comparing ours vs Google vs DataCite)

This document defines the recommended metrics, the corresponding algorithms, parameter choices, robustness and limitations, and a reproducibility recipe for experiments.

### 1. Evaluation perspective and overall pipeline

- Unit of analysis: an Excel workbook. For each workbook, the `survey` sheet serves as the gold standard for that workbook.
- Three matcher strengths (parallel views): Exact, Norm (normalized), and Fuzzy.
- Coverage is always computed on the entity layer (after deduplication). Evidence/URL slices are computed only on entities that hit gold. Redundancy is diagnosed on the mention→entity (Norm) layer.

Per-workbook steps:
1) Extract the gold list G from `survey`.
2) From the evaluated tables (e.g., ours/google/datacite), extract candidates U and build three deduplicated views: U_Exact, U_Norm, U_Fuzzy.
3) Compute coverage (recall) under the three views; compute evidence slices on hits; compute redundancy rate and evidence distribution.

### 2. Name normalization and fuzzy matching

2.1 Normalization canonical_norm(name)
- Lowercasing
- Collapse whitespace
- Remove light punctuation (quotes, dots, commas, semicolons, colons), while preserving parentheses and hyphen contents

2.2 Fuzzy key fuzzy_key(name)
- On top of normalization, drop all non-alphanumeric and non-space characters; keep only [a-z0-9 ]

2.3 Fuzzy clustering cluster_fuzzy(names, τ)
- Single-link greedy clustering using difflib.SequenceMatcher(k1, k2).ratio() ≥ τ
- τ = --fuzzy-threshold (default 0.9)
- Output is a list of clusters; each cluster is treated as one fuzzy entity

### 3. Entity-layer construction (deduplication)

- Exact: deduplicate by canonical_exact(name) (whitespace merging only)
- Norm: deduplicate by canonical_norm(name)
- Fuzzy: treat clusters from cluster_fuzzy(names, τ) as entities

Let U_Exact, U_Norm, U_Fuzzy denote the three entity sets; |U_*| their sizes.

### 4. Coverage/recall and denominators (per workbook)

Gold cardinality (denominator):
- Exact: |G_Exact| = |{canonical_exact(g): g∈G}|
- Norm: |G_Norm| = |{canonical_norm(g): g∈G}|
- Fuzzy: |G_Fuzzy| = |cluster_fuzzy(G, τ)|

Hit definition (numerator):
- Exact: entity e∈U_Exact hits if canonical_exact(repr_name(e)) ∈ {canonical_exact(g)}
- Norm: entity e∈U_Norm hits if canonical_norm(repr_name(e)) ∈ {canonical_norm(g)}
- Fuzzy: entity e∈U_Fuzzy hits if fuzzy_key(repr_name(e)) has similarity ≥ τ to the fuzzy_key of some gold cluster representative

Coverage/Recall:
- Coverage_* = Recall_* = (# of hit entities) / (gold cardinality) × 100%

Recommended primary metric: Recall_Norm_percent.

### 5. FuzzyGain (robustness gain)

Definition: FuzzyGain = Recall_Fuzzy_percent − Recall_Norm_percent.
Interpretation: improvement in robustness to name variants/noise; higher means fuzzy matching provides more “remedy”.

### 6. Evidence and provenance slices (only on hit entities)

6.1 EvidenceBacked_Recall_*_percent
- Among hit entities, the proportion that contain any evidence link (source/cited/homepage/dataset URL, etc.)

6.2 TrustedBacked_Recall_*_percent (stronger evidence)
- Among hit entities, the proportion whose evidence contains a DOI/HANDLE/ARK or a trusted hostname (configurable)
- Trusted hostnames are set via --trust-hosts (default includes doi.org, zenodo.org, kaggle.com, github.com, huggingface.co, etc.)

6.3 Recall_withDatasetURL_*_percent
- Among hit entities, the proportion with a non-empty “Dataset URL” field
- Optional: Recall_withValidDatasetURL_*_percent requires enabling --check-urls (live availability); off by default to avoid time-varying noise

### 7. Evidence distribution (on U_Norm, mutually exclusive categories)

For each entity (U_Norm):
1) If it contains a PID (DOI/HANDLE/ARK) → Evidence_PID
2) Else if it contains a trusted hostname → Evidence_TrustedHost
3) Else if it contains other links → Evidence_OtherLink
4) Else → Evidence_None

PID_Rate_percent = Evidence_PID / |U_Norm| × 100%.

### 8. Redundancy rate (diagnostic)

Redundancy_rate = (|mentions| − |U_Norm|) / max(1, |U_Norm|)
- Reflects the efficiency of mention→entity deduplication; lower is better

### 9. Parameters and recommendations

- τ (fuzzy threshold): default 0.9. If names vary widely, consider 0.85; too low risks over-aggregation (false merges)
- Trusted hosts: include DOI/PID platforms and authoritative repositories; extend for your domain as needed
- Live availability checks: off by default; avoid network noise. Use small-sample checks when necessary

### 10. Robustness and limitations

Strengths:
- The three views (Exact/Norm/Fuzzy) are complementary; Norm is a stable primary metric, Fuzzy provides an upper-bound estimate and tolerance to variants
- Evidence slices decouple “verifiable/usable” from “hit/non-hit,” avoiding mixing different dimensions

Limitations:
- Greedy single-link fuzzy clustering can be order-sensitive; a speed/simplicity trade-off. For large scale, one may switch to more stable clustering (e.g., hierarchical)
- Evidence extraction relies on column-name heuristics (configurable). Abnormal headers may reduce coverage

### 11. Reproducibility and usage

Command-line (example: all .xlsx workbooks under a directory; each workbook uses its `survey` sheet as gold):
```
/mlde/venv/bin/python /mlde/s2/anonymous_repo/research_questions/evaluate_datasets.py \
  /mlde/s2/anonymous_repo/xlsx \
  --output-dir /mlde/s2/anonymous_repo/evaluation_v2 \
  --fuzzy-threshold 0.9
```

Outputs:
- Per-file: `/mlde/s2/anonymous_repo/evaluation_v2/xlsx_per_file_v2.tsv`
- Aggregate (micro): `/mlde/s2/anonymous_repo/evaluation_v2/xlsx_aggregate_v2.tsv`

Recommended comparison columns (for paper figures/tables):
- Method, Recall_Norm_percent, FuzzyGain (=Recall_Fuzzy−Recall_Norm), TrustedBacked_Recall_Norm_percent, Recall_withDatasetURL_Norm_percent, PID_Rate_percent, Redundancy_rate (optional)

### 12. References
- Wilkinson et al., The FAIR Guiding Principles for scientific data management and stewardship, Scientific Data (2016)
- Christen, Data Matching, Springer (2012)
- Python standard library: difflib.SequenceMatcher


