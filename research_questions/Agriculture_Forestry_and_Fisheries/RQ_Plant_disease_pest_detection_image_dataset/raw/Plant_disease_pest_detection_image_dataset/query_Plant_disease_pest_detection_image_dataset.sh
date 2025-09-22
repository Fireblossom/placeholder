#HF_HOME=/mlde/hf vllm serve Qwen/Qwen2.5-72B-Instruct --port 8000 --tensor-parallel-size 4 --gpu-memory-utilization 0.96

#!/bin/bash
#
# Research Dataset Analysis Pipeline
#
# This script runs the complete analysis pipeline for research dataset analysis:
# 1. Extract citation contexts from Semantic Scholar
# 2. Extract datasets using OpenAI LLM
# 3. Analyze datasets by citation count
# 4. Merge and deduplicate analysis results
#
# File Organization:
# - Intermediate files (JSON) are saved to a timestamped directory: analysis_output_YYYYMMDD_HHMMSS/
# - Individual TSV results are saved to: task_a/ directory
# - When CONTEXT_TYPE="both", you get TWO separate TSV files: one for citing contexts, one for cited contexts
# - Final merged and deduplicated result: task_a/[query]_final_merged_dataset_analysis_table.tsv
# - This keeps your workspace clean while providing both individual and merged results

set -e  # Exit on any error

# Ensure UTF-8 locale for reliable Unicode handling in echo output
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# Configuration
WORK_DIR="/mlde/s2"
TASK_A_DIR="$WORK_DIR/task_a"

# Paper limit configuration
# Set the maximum number of papers to retrieve and process
# Default: 1000 (can be changed to smaller values for testing)
# Recommended: 100 for quick testing, 1000 for full analysis
PAPER_LIMIT=400

# Expert Domain Configuration for LLM prompts
# Set a domain to specialize the LLM system role (improves judgment)
# Examples: "oncology", "single-cell transcriptomics", "computational linguistics", "archaeology"
# EXPERT_DOMAIN="Peptides"
EXPERT_DOMAIN="Agricultural and Food Sciences"

QUERY_KEYWORD="Plant Disease diagnosis or pest detection image Dataset"
FIELDOFSTUDY="Agricultural and Food Sciences"

# QUERY_KEYWORD="All-in-One Image Restoration"

# QUERY_KEYWORD="salty enhancing peptides"

# QUERY_KEYWORD="antioxidant peptides sequence and activity relationship"
# FIELDOFSTUDY="Agricultural and Food Sciences,Chemistry,Biology"

# QUERY_KEYWORD="Colorectal Cancer and Liver metastasis scRNA"
# QUERY_KEYWORD="Colorectal liver metastases CRLM single cell RNA sequencing"

# QUERY_KEYWORD="how does statistical information influence second language processing"
# QUERY_KEYWORD="topic modelling for research articles"
# QUERY_KEYWORD="second language Learner Corpus"
# QUERY_KEYWORD="Statistical Learning Non-native"
# FIELDOFSTUDY="Education,Linguistics,Psychology"
# QUERY_KEYWORD="agricultural water stress sensor dataset"
# FIELDOFSTUDY="Agricultural and Food Sciences,Chemistry,Biology,Computer Science"

# QUERY_KEYWORD="Laban Movement Analysis for robot emotion"
# FIELDOFSTUDY="Computer Science,Robotics"

# QUERY_KEYWORD="Laban Movement Analysis for dance emotion"
# FIELDOFSTUDY="Art"

# QUERY_KEYWORD="cues of probabilistic for second language"
# QUERY_KEYWORD="ritual of fire"
# QUERY_KEYWORD="symbol of fire"
# QUERY_KEYWORD="symbolism of fire"
# QUERY_KEYWORD="3D cultural heritage museum digitization"
# QUERY_KEYWORD="dance as artistic expression"
# QUERY_KEYWORD="dance style recognition Laban Movement Analysis"
# QUERY_KEYWORD="ballroom and street dance styles"

# QUERY_KEYWORD="professional bias newspaper"
# FIELDOFSTUDY="Sociology,Psychology"
#FIELDOFSTUDY="Computer Science,Chemistry,Biology,Materials Science,Physics,Geology,Psychology,Art,History,Geography,Sociology,Business,Political Science,Economics,Philosophy,Mathematics,Engineering,Environmental Science,Agricultural and Food Sciences,Education,Law,Linguistics"
# FIELDOFSTUDY="Art,History,Philosophy"
# FIELDOFSTUDY="" #"Education,Linguistics"

# QUERY_KEYWORD="Patent classification nlp"
# FIELDOFSTUDY="Computer Science,Linguistics,Medicine"

# Context Type Configuration
# Choose from: citing, cited, both
# citing: only extract citing papers contexts (default)
# cited: only extract cited papers contexts
# both: extract both citing and cited papers contexts
#
# To change context type, edit the line below:
CONTEXT_TYPE="both" # "citing"

# Keyword Matching Configuration
# Set to true to require exact keyword matching in paper abstracts
# This will pre-filter papers before LLM processing
# true: require all topic keywords to appear in abstracts (exact word match)
# false: use only LLM filtering (default)
#
# To enable keyword matching, edit the line below:
REQUIRE_KEYWORD_MATCH="false"

# Specific Keyword Filter Configuration
# Set a specific keyword that must appear in title or abstract
# This provides additional filtering beyond the general topic keywords
# Leave empty to disable specific keyword filtering
#
# To set a specific keyword filter, edit the line below:
SPECIFIC_KEYWORD_FILTER="disease"

# LLM Prefilter Configuration (Title/Abstract relevance)
# Enable LLM-based prefilter right after fetching papers
# true: use LLM to filter papers by title/abstract relevance to the topic
# false: skip LLM prefilter
#
# Minimum confidence for keeping a paper during prefilter
LLM_PREFILTER="true"
PREFILTER_MIN_CONFIDENCE="0.5"

# Relevance Audit Configuration (stop after Step 1)
# Audit top-N fetched papers (API order) and then stop the pipeline
AUDIT_ONLY="false"
AUDIT_TOP=0 #$PAPER_LIMIT
AUDIT_THRESHOLD="$PREFILTER_MIN_CONFIDENCE"

# Dataset Validation Configuration
# Set validation mode for dataset relevance filtering
# off: disable validation (keep all datasets)
# basic: basic validation (filter obvious generic datasets)
# smart: smart validation (consider topic relevance and context)
# strict: strict validation (require clear topic relevance)
# llm: LLM-based validation (most intelligent, uses AI to judge relevance) - default
#
# To change validation mode, edit the line below:
VALIDATION_MODE="llm"

# Strict Filtering Configuration
# Enable secondary LLM filtering for top papers with stricter criteria
# true: enable strict filtering (only keep papers with high confidence >= threshold)
# false: disable strict filtering (use standard filtering only)
#
# To enable strict filtering, edit the line below:
STRICT_FILTERING="true"

# Strict Filtering Confidence Threshold
# Minimum confidence level required for papers to pass strict filtering
# Higher values = stricter filtering (fewer papers pass)
# Recommended: 0.7-0.9 for strict filtering
#
# To change confidence threshold, edit the line below:
STRICT_CONFIDENCE_THRESHOLD="0.8"

# Output Directory Configuration
# Create a timestamped directory for intermediate files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="analysis_output_${TIMESTAMP}"
FINAL_OUTPUT_DIR="task_a"  # Directory for final TSV files

echo "📁 Analysis timestamp: $TIMESTAMP"
echo "📂 Intermediate files will be saved to: $OUTPUT_DIR/"
echo "📊 Final TSV files will be saved to: $FINAL_OUTPUT_DIR/"
echo

# API Configuration
# Semantic Scholar API Key for enhanced rate limits and access
SEMANTIC_SCHOLAR_API_KEY="yMXz0QIAce42pjgS8igZ01dS92YHVV0Y9PO6mTE7"

# Function to generate context filename based on context type
generate_context_filename() {
    local query="$1"
    local context_type="$2"
    local limit="$3"

    # Clean query for filename
    local safe_query=$(echo "$query" | tr '[:upper:]' '[:lower:]' | tr ' ' '_' | tr '-' '_' | tr '/' '_')

    case "$context_type" in
        "citing")
            echo "${safe_query}_citing_contexts_${PAPER_LIMIT}.json"
            ;;
        "cited")
            echo "${safe_query}_cited_contexts.json"
            ;;
        "both")
            echo "${safe_query}_combined_contexts_${PAPER_LIMIT}.json"
            ;;
        *)
            echo "${safe_query}_contexts_${PAPER_LIMIT}.json"
            ;;
    esac
}

# Function to generate annotated filename
generate_annotated_filename() {
    local query="$1"
    local context_type="$2"

    # Clean query for filename
    local safe_query=$(echo "$query" | tr '[:upper:]' '[:lower:]' | tr ' ' '_' | tr '-' '_' | tr '/' '_')

    case "$context_type" in
        "citing")
            echo "${safe_query}_citing_contexts_openai_annotated.json"
            ;;
        "cited")
            echo "${safe_query}_cited_contexts_openai_annotated.json"
            ;;
        "both")
            echo "${safe_query}_combined_contexts_openai_annotated.json"
            ;;
        *)
            echo "${safe_query}_contexts_openai_annotated.json"
            ;;
    esac
}

# Create output directory
mkdir -p "$OUTPUT_DIR"
mkdir -p "$FINAL_OUTPUT_DIR"
echo "✅ Created output directories"

# Validate context type
case "$CONTEXT_TYPE" in
    "citing"|"cited"|"both")
        echo "✅ Valid context type: $CONTEXT_TYPE"
        ;;
    *)
        echo "❌ Invalid context type: $CONTEXT_TYPE"
        echo "Valid options: citing, cited, both"
        exit 1
        ;;
esac

# Validate keyword matching configuration
case "$REQUIRE_KEYWORD_MATCH" in
    "true"|"false")
        echo "✅ Valid keyword matching setting: $REQUIRE_KEYWORD_MATCH"
        ;;
    *)
        echo "❌ Invalid keyword matching setting: $REQUIRE_KEYWORD_MATCH"
        echo "Valid options: true, false"
        exit 1
        ;;
esac

# Validate specific keyword filter configuration
if [ -n "$SPECIFIC_KEYWORD_FILTER" ]; then
    echo "✅ Specific keyword filter: '$SPECIFIC_KEYWORD_FILTER'"
else
    echo "✅ Specific keyword filter: DISABLED"
fi

# Validate dataset validation mode
case "$VALIDATION_MODE" in
    "off"|"basic"|"smart"|"strict"|"llm")
        echo "✅ Valid validation mode: $VALIDATION_MODE"
        ;;
    *)
        echo "❌ Invalid validation mode: $VALIDATION_MODE"
        echo "Valid options: off, basic, smart, strict, llm"
        exit 1
        ;;
esac

echo "🔍 Query keyword: '$QUERY_KEYWORD'"
echo "📁 Working directory: $WORK_DIR"
echo "🔑 API Key: Configured"
echo "🎯 Context type: $CONTEXT_TYPE"
echo "📊 Paper limit: $PAPER_LIMIT"
if [ "$REQUIRE_KEYWORD_MATCH" = "true" ]; then
    echo "🔍 Keyword matching: ENABLED (exact word match required in abstracts)"
else
    echo "🔍 Keyword matching: DISABLED (LLM filtering only)"
fi
if [ -n "$SPECIFIC_KEYWORD_FILTER" ]; then
    echo "🎯 Specific keyword filter: '$SPECIFIC_KEYWORD_FILTER' (must appear in title or abstract)"
else
    echo "🎯 Specific keyword filter: DISABLED"
fi
echo "🔍 Dataset validation: $VALIDATION_MODE"
if [ "$STRICT_FILTERING" = "true" ]; then
    echo "🎯 Strict filtering: ENABLED (confidence >= $STRICT_CONFIDENCE_THRESHOLD)"
else
    echo "🎯 Strict filtering: DISABLED"
fi
if [ -n "$FIELDOFSTUDY" ]; then
    echo "🎨 Research fields: $FIELDOFSTUDY"
else
    echo "🎨 Research fields: All fields"
fi
echo

# Stay in root directory for proper module imports
echo "📂 Staying in root directory: $WORK_DIR"
echo

# Step 1: Run citation context extraction with the new query
echo "=== Step 1: Extracting citation contexts ==="
echo "🚀 Running citation context extraction..."

# 执行命令 - Step 1: Citation context extraction
echo "🔄 Step 1: Extracting citation contexts to $OUTPUT_DIR/"
# Build conditional arguments
ARGS="--query \"$QUERY_KEYWORD\" --limit \"$PAPER_LIMIT\" --context-type \"$CONTEXT_TYPE\" --output-dir \"$OUTPUT_DIR\""

if [ -n "$FIELDOFSTUDY" ]; then
    ARGS="$ARGS --fieldofstudy \"$FIELDOFSTUDY\""
fi

if [ "$LLM_PREFILTER" = "true" ]; then
    ARGS="$ARGS --llm-prefilter"
fi

if [ "$REQUIRE_KEYWORD_MATCH" = "true" ]; then
    ARGS="$ARGS --require-keyword-match"
fi

if [ -n "$SPECIFIC_KEYWORD_FILTER" ]; then
    ARGS="$ARGS --specific-keyword-filter \"$SPECIFIC_KEYWORD_FILTER\""
fi

ARGS="$ARGS --prefilter-min-confidence \"$PREFILTER_MIN_CONFIDENCE\" --audit-top \"$AUDIT_TOP\" --audit-threshold \"$AUDIT_THRESHOLD\""

# Execute the command
eval "SEMANTIC_SCHOLAR_API_KEY=\"$SEMANTIC_SCHOLAR_API_KEY\" python task_a/2_query_research_topic_contexts.py $ARGS"

echo "✅ Citation contexts extracted"
echo

# Stop after audit if enabled
if [ "$AUDIT_ONLY" = "true" ]; then
    echo "🛑 Audit-only mode enabled. Stopping after Step 1 (relevance audit)."
    exit 0
fi

# Step 2: Run dataset extraction with the new topic
echo "=== Step 2: Extracting datasets using LLM ==="
echo "🚀 Running dataset extraction..."

if [ "$CONTEXT_TYPE" = "both" ]; then
    echo "🔄 Processing BOTH citing and cited contexts for dataset extraction..."
    echo

    # 2A: Process citing contexts
    echo "--- 2A: Extracting datasets from Citing Contexts ---"
    CITING_INPUT_FILE="$OUTPUT_DIR/$(generate_context_filename "$QUERY_KEYWORD" "citing" "$PAPER_LIMIT")"
    CITING_ANNOTATED_FILE="$OUTPUT_DIR/$(generate_annotated_filename "$QUERY_KEYWORD" "citing")"

    echo "📂 Citing input file: $CITING_INPUT_FILE"
    echo "📝 Citing output file: $CITING_ANNOTATED_FILE"

    if [ "$REQUIRE_KEYWORD_MATCH" = "true" ]; then
        python task_a/3_dataset_extractor_openai.py \
            --topic "$QUERY_KEYWORD" \
            --input-file "$CITING_INPUT_FILE" \
            --output-file "$CITING_ANNOTATED_FILE" \
            --expert-domain "$EXPERT_DOMAIN" \
            --require-keyword-match \
            --strict-filtering "$STRICT_FILTERING" \
            --strict-confidence "$STRICT_CONFIDENCE_THRESHOLD"
    else
        python task_a/3_dataset_extractor_openai.py \
            --topic "$QUERY_KEYWORD" \
            --input-file "$CITING_INPUT_FILE" \
            --output-file "$CITING_ANNOTATED_FILE" \
            --expert-domain "$EXPERT_DOMAIN" \
            --strict-filtering "$STRICT_FILTERING" \
            --strict-confidence "$STRICT_CONFIDENCE_THRESHOLD"
    fi

    echo "✅ Citing contexts dataset extraction completed"
    echo

    # 2B: Process cited contexts
    echo "--- 2B: Extracting datasets from Cited Contexts ---"
    CITED_INPUT_FILE="$OUTPUT_DIR/$(generate_context_filename "$QUERY_KEYWORD" "cited" "$PAPER_LIMIT")"
    CITED_ANNOTATED_FILE="$OUTPUT_DIR/$(generate_annotated_filename "$QUERY_KEYWORD" "cited")"

    echo "📂 Cited input file: $CITED_INPUT_FILE"
    echo "📝 Cited output file: $CITED_ANNOTATED_FILE"

    if [ "$REQUIRE_KEYWORD_MATCH" = "true" ]; then
        python task_a/3_dataset_extractor_openai.py \
            --topic "$QUERY_KEYWORD" \
            --input-file "$CITED_INPUT_FILE" \
            --output-file "$CITED_ANNOTATED_FILE" \
            --expert-domain "$EXPERT_DOMAIN" \
            --require-keyword-match \
            --strict-filtering "$STRICT_FILTERING" \
            --strict-confidence "$STRICT_CONFIDENCE_THRESHOLD"
    else
        python task_a/3_dataset_extractor_openai.py \
            --topic "$QUERY_KEYWORD" \
            --input-file "$CITED_INPUT_FILE" \
            --output-file "$CITED_ANNOTATED_FILE" \
            --expert-domain "$EXPERT_DOMAIN" \
            --strict-filtering "$STRICT_FILTERING" \
            --strict-confidence "$STRICT_CONFIDENCE_THRESHOLD"
    fi

    echo "✅ Cited contexts dataset extraction completed"
    echo

else
    # Single context type extraction
    INPUT_FILE="$OUTPUT_DIR/$(generate_context_filename "$QUERY_KEYWORD" "$CONTEXT_TYPE" "$PAPER_LIMIT")"
    ANNOTATED_FILE="$OUTPUT_DIR/$(generate_annotated_filename "$QUERY_KEYWORD" "$CONTEXT_TYPE")"

    echo "📂 Input file: $INPUT_FILE"
    echo "📝 Output file: $ANNOTATED_FILE"

    if [ "$REQUIRE_KEYWORD_MATCH" = "true" ]; then
        python task_a/3_dataset_extractor_openai.py \
            --topic "$QUERY_KEYWORD" \
            --input-file "$INPUT_FILE" \
            --output-file "$ANNOTATED_FILE" \
            --expert-domain "$EXPERT_DOMAIN" \
            --require-keyword-match \
            --strict-filtering "$STRICT_FILTERING" \
            --strict-confidence "$STRICT_CONFIDENCE_THRESHOLD"
    else
        python task_a/3_dataset_extractor_openai.py \
            --topic "$QUERY_KEYWORD" \
            --input-file "$INPUT_FILE" \
            --output-file "$ANNOTATED_FILE" \
            --expert-domain "$EXPERT_DOMAIN" \
            --strict-filtering "$STRICT_FILTERING" \
            --strict-confidence "$STRICT_CONFIDENCE_THRESHOLD"
    fi

    echo "Dataset extraction completed"
    echo
fi

# Step 3: Analyze datasets by citation count
echo "=== Step 3: Analyzing datasets by citation count ==="
echo "Running dataset analysis..."

# Generate output analysis file names
SAFE_QUERY_NAME=$(echo "$QUERY_KEYWORD" | tr '[:upper:]' '[:lower:]' | tr ' ' '_' | tr '-' '_' | tr '/' '_')

if [ "$CONTEXT_TYPE" = "both" ]; then
    echo "🔄 Processing BOTH citing and cited contexts..."
    echo

    # 3A: Process citing contexts
    echo "--- 3A: Analyzing Citing Contexts ---"
    CITING_INPUT_FILE="$OUTPUT_DIR/$(generate_context_filename "$QUERY_KEYWORD" "citing" "$PAPER_LIMIT")"
    CITING_ANNOTATED_FILE="$OUTPUT_DIR/$(generate_annotated_filename "$QUERY_KEYWORD" "citing")"
    CITING_ANALYSIS_JSON="$OUTPUT_DIR/${SAFE_QUERY_NAME}_citing_datasets_by_citation_analysis.json"
    CITING_FINAL_TSV="$FINAL_OUTPUT_DIR/${SAFE_QUERY_NAME}_citing_dataset_analysis_table.tsv"

    echo "📊 Citing analysis input: $CITING_ANNOTATED_FILE"
    echo "📄 Citing citation contexts: $CITING_INPUT_FILE"
    echo "📈 Citing JSON output: $CITING_ANALYSIS_JSON"
    echo "📋 Citing TSV output: $CITING_FINAL_TSV"

    # Check if files exist before running
    if [ ! -f "$CITING_ANNOTATED_FILE" ]; then
        echo "❌ Citing annotated file not found: $CITING_ANNOTATED_FILE"
        echo "Skipping citing contexts analysis"
    elif [ ! -f "$CITING_INPUT_FILE" ]; then
        echo "❌ Citing citation contexts file not found: $CITING_INPUT_FILE"
        echo "Skipping citing contexts analysis"
    else
        python task_a/4_analyze_datasets_by_citation_count.py \
            --extraction-results "$CITING_ANNOTATED_FILE" \
            --citation-contexts "$CITING_INPUT_FILE" \
            --output "$CITING_ANALYSIS_JSON" \
            --table-output "$CITING_FINAL_TSV" \
            --llm-summary \
            --topic "$QUERY_KEYWORD" \
            --validation-mode "$VALIDATION_MODE"
    fi

    echo "✅ Citing contexts analysis completed"
    echo

    # 3B: Process cited contexts
    echo "--- 3B: Analyzing Cited Contexts ---"
    CITED_ANNOTATED_FILE="$OUTPUT_DIR/$(generate_annotated_filename "$QUERY_KEYWORD" "cited")"
    CITED_ANALYSIS_JSON="$OUTPUT_DIR/${SAFE_QUERY_NAME}_cited_datasets_by_citation_analysis.json"
    CITED_FINAL_TSV="$FINAL_OUTPUT_DIR/${SAFE_QUERY_NAME}_cited_dataset_analysis_table.tsv"

    echo "📊 Cited analysis input: $CITED_ANNOTATED_FILE"
    echo "📄 Cited citation contexts: $CITING_INPUT_FILE"
    echo "📈 Cited JSON output: $CITED_ANALYSIS_JSON"
    echo "📋 Cited TSV output: $CITED_FINAL_TSV"

    # Check if files exist before running
    if [ ! -f "$CITED_ANNOTATED_FILE" ]; then
        echo "❌ Cited annotated file not found: $CITED_ANNOTATED_FILE"
        echo "Skipping cited contexts analysis"
    elif [ ! -f "$CITING_INPUT_FILE" ]; then
        echo "❌ Citing citation contexts file not found: $CITING_INPUT_FILE"
        echo "Skipping cited contexts analysis"
    else
        python task_a/4_analyze_datasets_by_citation_count.py \
            --extraction-results "$CITED_ANNOTATED_FILE" \
            --citation-contexts "$CITING_INPUT_FILE" \
            --output "$CITED_ANALYSIS_JSON" \
            --table-output "$CITED_FINAL_TSV" \
            --llm-summary \
            --topic "$QUERY_KEYWORD" \
            --validation-mode "$VALIDATION_MODE"
    fi

    echo "✅ Cited contexts analysis completed"
    echo

else
    # Single context type analysis
    ANALYSIS_JSON_FILE="$OUTPUT_DIR/${SAFE_QUERY_NAME}_datasets_by_citation_analysis.json"
    FINAL_TSV_FILE="$FINAL_OUTPUT_DIR/${SAFE_QUERY_NAME}_dataset_analysis_table.tsv"

    echo "📊 Analysis input: $ANNOTATED_FILE"
    echo "📄 Citation contexts: $INPUT_FILE"
    echo "📈 JSON output: $ANALYSIS_JSON_FILE"
    echo "📋 Final TSV output: $FINAL_TSV_FILE"

    # For single context, use the same input file for citation contexts
    if [ "$CONTEXT_TYPE" = "citing" ]; then
        CITATION_CONTEXTS_FILE="$INPUT_FILE"
    else
        CITATION_CONTEXTS_FILE="$CITING_INPUT_FILE"
    fi

    # Check if files exist before running
    if [ ! -f "$ANNOTATED_FILE" ]; then
        echo "❌ Annotated file not found: $ANNOTATED_FILE"
        echo "Skipping analysis"
    elif [ ! -f "$INPUT_FILE" ]; then
        echo "❌ Citation contexts file not found: $INPUT_FILE"
        echo "Skipping analysis"
    else
        python task_a/4_analyze_datasets_by_citation_count.py \
            --extraction-results "$ANNOTATED_FILE" \
            --citation-contexts "$CITATION_CONTEXTS_FILE" \
            --output "$ANALYSIS_JSON_FILE" \
            --table-output "$FINAL_TSV_FILE" \
            --llm-summary \
            --topic "$QUERY_KEYWORD" \
            --validation-mode "$VALIDATION_MODE"
    fi

    echo "✅ Dataset analysis completed"
    echo
fi

# Step 4: Merge and deduplicate analysis results
echo "=== Step 4: Merging and deduplicating analysis results ==="
echo "🚀 Running merge and deduplicate..."

# Check if we have any TSV files to merge
TSV_COUNT=$(find "$FINAL_OUTPUT_DIR" -name "*${SAFE_QUERY_NAME}*_dataset_analysis_table.tsv" 2>/dev/null | wc -l)

if [ "$TSV_COUNT" -gt 0 ]; then
    echo "📊 Found $TSV_COUNT TSV files to merge"

    MERGED_FINAL_TSV="$FINAL_OUTPUT_DIR/${SAFE_QUERY_NAME}_final_merged_dataset_analysis_table.tsv"

    echo "🔄 Merging TSV files and applying deduplication..."
    echo "📂 Output directory: $FINAL_OUTPUT_DIR"
    echo "📋 Final merged output: $MERGED_FINAL_TSV"

    python task_a/4_analyze_datasets_by_citation_count.py \
        --merge-tsv \
        --deduplicate \
        --query "$QUERY_KEYWORD" \
        --output-dir "$FINAL_OUTPUT_DIR"

    echo "✅ Merge and deduplication completed"
    echo "📁 Final merged result: $MERGED_FINAL_TSV"
else
    echo "⚠️  No TSV files found to merge"
fi

echo

# Step 5: Summary
echo "=== Analysis Complete ==="
echo "📊 Generated files:"

if [ "$CONTEXT_TYPE" = "both" ]; then
    echo "  📁 Intermediate files in: $OUTPUT_DIR/"
    echo "    - Citation contexts (citing): $(basename "$CITING_INPUT_FILE")"
    echo "    - Citation contexts (cited): $(basename "$CITED_INPUT_FILE")"
    echo "    - Dataset extraction (citing): $(basename "$CITING_ANNOTATED_FILE")"
    echo "    - Dataset extraction (cited): $(basename "$CITED_ANNOTATED_FILE")"
    echo "    - Analysis JSON (citing): $(basename "$CITING_ANALYSIS_JSON")"
    echo "    - Analysis JSON (cited): $(basename "$CITED_ANALYSIS_JSON")"
    echo "  📋 Individual results:"
    echo "    - Dataset analysis table (citing): $(basename "$CITING_FINAL_TSV")"
    echo "    - Dataset analysis table (cited): $(basename "$CITED_FINAL_TSV")"
    if [ "$TSV_COUNT" -gt 0 ]; then
        echo "  📋 Merged results:"
        echo "    - Final merged analysis table: ${FINAL_OUTPUT_DIR}/${SAFE_QUERY_NAME}_final_merged_dataset_analysis_table.tsv"
    fi
else
    echo "  📁 Intermediate files in: $OUTPUT_DIR/"
    echo "    - Citation contexts: $(basename "$INPUT_FILE")"
    echo "    - Dataset extraction: $(basename "$ANNOTATED_FILE")"
    echo "    - Analysis JSON: $(basename "$ANALYSIS_JSON_FILE")"
    echo "  📋 Results:"
    echo "    - Dataset analysis table: $(basename "$FINAL_TSV_FILE")"
    if [ "$TSV_COUNT" -gt 0 ]; then
        echo "    - Final merged analysis table: ${SAFE_QUERY_NAME}_final_merged_dataset_analysis_table.tsv"
    fi
fi

echo "  📂 Output directory: $OUTPUT_DIR/"
echo "  📊 Results directory: $FINAL_OUTPUT_DIR/"
echo

echo "🎯 Query keyword: '$QUERY_KEYWORD'"
if [ "$REQUIRE_KEYWORD_MATCH" = "true" ]; then
    echo "🔍 Keyword matching: ENABLED (exact word match required in abstracts)"
else
    echo "🔍 Keyword matching: DISABLED (LLM filtering only)"
fi
if [ -n "$SPECIFIC_KEYWORD_FILTER" ]; then
    echo "🎯 Specific keyword filter: '$SPECIFIC_KEYWORD_FILTER' (must appear in title or abstract)"
else
    echo "🎯 Specific keyword filter: DISABLED"
fi
echo "🔍 Dataset validation: $VALIDATION_MODE"
if [ -n "$FIELDOFSTUDY" ]; then
    echo "🎨 Research fields: $FIELDOFSTUDY"
else
    echo "🎨 Research fields: All fields"
fi

echo
echo "✅ All steps completed successfully!"
echo
echo "💡 Tips:"
if [ "$CONTEXT_TYPE" = "both" ]; then
    echo "  - Your final TSV results are in: $FINAL_OUTPUT_DIR/ (2 files: citing and cited analyses)"
else
    echo "  - Your final TSV results are in: $FINAL_OUTPUT_DIR/"
fi
echo "  - All intermediate files are organized in: $OUTPUT_DIR/"
echo "  - To clean up intermediate files later, run: rm -rf $OUTPUT_DIR"
echo "  - To keep only TSV files, run: find $OUTPUT_DIR -name '*.tsv' -exec mv {} $FINAL_OUTPUT_DIR/ \; && rm -rf $OUTPUT_DIR"
