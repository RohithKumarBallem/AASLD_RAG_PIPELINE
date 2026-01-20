# AASLD RAG Pipeline

A comprehensive Retrieval-Augmented Generation (RAG) pipeline designed to process, index, and query AASLD (American Association for the Study of Liver Diseases) clinical guidelines and hepatology content for accurate, context-aware responses.

## Overview

This project implements a complete RAG system that:
- **Scrapes & Extracts** AASLD practice guidelines from the web
- **Cleans & Normalizes** extracted data for high-quality indexing
- **Chunks & Embeds** clinical content with semantic embeddings
- **Evaluates** multiple LLM models (Llama, Phi, Qwen) for retrieval quality
- **Stores** indexed data in a vector database for efficient retrieval

The pipeline is structured in two main stages:
- **Pipeline-1**: Data extraction, cleaning, and embedding
- **Pipeline-2**: LLM evaluation and benchmarking

---

## Project Structure

```
AASLD_RAG_PIPELINE/
├── Data_Extraction.py          # Web scraper for AASLD guidelines (HTML + PDF processing)
├── clean_data.py               # Data cleaning and normalization module
├── LLM_Preprocessing.ipynb     # Data chunking and embedding generation
├── Llama.ipynb                 # Llama model evaluation and RAG testing
├── Phi.ipynb                   # Phi model evaluation and RAG testing
├── Qwen.ipynb                  # Qwen model evaluation and RAG testing
│
├── Pipeline-1 Outputs/
│   ├── chunks_metadata.json    # Metadata for all text chunks
│   ├── embeddings.npy          # Pre-computed semantic embeddings (768-dim)
│   ├── index.txt               # Index of chunk IDs for retrieval
│   ├── pipeline_summary.json   # Processing statistics and config
│   ├── cleaned_data/           # ~44 cleaned JSON files from AASLD sources
│   └── data/
│       ├── json/               # Raw extracted JSON content
│       ├── metadata/           # Source metadata
│       ├── pdfs/               # Downloaded PDF guidelines
│       └── text_content/       # Extracted text from PDFs
│
├── Pipeline-2 Outputs/
│   ├── final_evaluation_Llama.csv   # Llama model evaluation metrics
│   ├── final_evaluation_Phi.csv     # Phi model evaluation metrics
│   └── final_evaluation_Qwen.csv    # Qwen model evaluation metrics
│
├── Vector_DB/                  # Vector database storage
├── README.md                   # This file
└── LLM_Project_Report.pdf     # Detailed project report
```

---

## Pipeline Workflow

### Stage 1: Data Acquisition & Processing

#### 1.1 **Data Extraction** (`Data_Extraction.py`)
- Scrapes AASLD practice guidelines from `https://www.aasld.org/practice-guidelines`
- Extracts content from HTML pages and PDF documents
- Handles dynamic content via Selenium
- Implements rate limiting and Cloudflare bypass
- Outputs structured JSON with full content, metadata, and source URLs

**Key Features:**
- Web scraping with BeautifulSoup and Selenium
- PDF parsing with PyPDF2
- Cloudflare handling with configurable wait times
- Rate limiting (1.5s between requests)
- Structured JSON output with timestamps and hashes

#### 1.2 **Data Cleaning** (`clean_data.py`)
- Removes navigation boilerplate and non-clinical content
- Normalizes text formatting (whitespace, line breaks)
- Extracts and preserves clinical metadata
- Maintains document structure (sections, tables, lists)
- Prepares data for semantic chunking

**Processing Steps:**
- Remove AASLD site navigation patterns
- Extract clinical recommendations and dosages
- Normalize formatting for consistency
- Validate and structure output

#### 1.3 **Preprocessing & Embedding** (`LLM_Preprocessing.ipynb`)
- Chunks cleaned text into semantic units (512 tokens, 25 token overlap)
- Generates embeddings using `pritamdeka/S-Bluebert-snli-multinli-stsb`
- Creates metadata index for chunk retrieval
- Stores embeddings efficiently (37.46 MB, 768-dimensional vectors)

**Statistics:**
- 49 JSON files processed
- 44 files successfully loaded
- 1,507 sections extracted
- 12,788 text chunks created
- 768-dimensional embeddings (float32)
- CUDA acceleration on T4 GPU

### Stage 2: LLM Evaluation & RAG Testing

Three LLM models are evaluated for RAG performance:

#### 2.1 **Llama Model** (`Llama.ipynb`)
- Meta's Llama language model integration
- RAG pipeline with AASLD retrieval
- Evaluation metrics for clinical accuracy

#### 2.2 **Phi Model** (`Phi.ipynb`)
- Microsoft's Phi language model integration
- Context-aware clinical query answering
- Performance benchmarking

#### 2.3 **Qwen Model** (`Qwen.ipynb`)
- Alibaba's Qwen language model integration
- Multi-lingual support considerations
- Cross-model comparison

**Evaluation Outputs:**
- `final_evaluation_Llama.csv`: Performance metrics for Llama
- `final_evaluation_Phi.csv`: Performance metrics for Phi
- `final_evaluation_Qwen.csv`: Performance metrics for Qwen

---

## Installation & Setup

### Prerequisites
- Python 3.8+
- CUDA-capable GPU (recommended for embeddings)
- Chrome/Chromium browser for web scraping

### Required Dependencies

```bash
pip install -r requirements.txt
```

**Key Libraries:**
- `requests` - HTTP requests for web scraping
- `beautifulsoup4` - HTML parsing
- `selenium` - Dynamic content loading
- `webdriver-manager` - Chrome driver management
- `PyPDF2` - PDF text extraction
- `transformers` - HuggingFace models
- `torch` - Deep learning framework
- `numpy` - Numerical computations
- `pandas` - Data analysis
- `jupyter` - Notebook environment

---

## Usage

### Step 1: Extract AASLD Data
```bash
python Data_Extraction.py
```
**Output:** JSON files in `Pipeline-1 Outputs/data/json/`

### Step 2: Clean & Normalize Data
```bash
python clean_data.py
```
**Output:** Cleaned JSON files in `Pipeline-1 Outputs/cleaned_data/`

### Step 3: Generate Embeddings
Open and run `LLM_Preprocessing.ipynb`:
```bash
jupyter notebook LLM_Preprocessing.ipynb
```
**Outputs:**
- `embeddings.npy` - Semantic embeddings
- `chunks_metadata.json` - Chunk metadata
- `pipeline_summary.json` - Processing stats

### Step 4: Evaluate LLM Models
Run model evaluation notebooks:
```bash
jupyter notebook Llama.ipynb
jupyter notebook Phi.ipynb
jupyter notebook Qwen.ipynb
```
**Outputs:** CSV files with evaluation metrics

---

## Pipeline Statistics

**Extraction Results:**
- Total Guidelines Files: 49
- Successfully Processed: 44
- Total Sections: 1,507
- Total Chunks: 12,788

**Embedding Configuration:**
- Model: `pritamdeka/S-Bluebert-snli-multinli-stsb`
- Dimension: 768
- Chunk Size: 512 tokens
- Chunk Overlap: 25 tokens
- Embedding Size: 37.46 MB
- Hardware: T4 GPU (Google Colab)

---

## Key Features

**Comprehensive Web Scraping**
- Handles both HTML and PDF content
- Overcomes Cloudflare protection
- Rate-limited and respectful crawling

**Intelligent Data Cleaning**
- Removes boilerplate and navigation
- Preserves clinical information
- Maintains document structure

**High-Quality Embeddings**
- BioBERT-based semantic embeddings
- Efficient storage with NumPy
- GPU-accelerated processing

**Multi-Model Evaluation**
- Benchmarks three state-of-the-art LLMs
- Standardized evaluation metrics
- Clinical relevance assessment

**Production-Ready Pipeline**
- Modular architecture
- Error handling and validation
- Detailed logging and statistics

---

## Model Evaluation

The pipeline evaluates three LLM models on their ability to answer AASLD clinical queries using the indexed guidelines:

| Model | Evaluation File | Purpose |
|-------|-----------------|---------|
| **Llama** | `final_evaluation_Llama.csv` | Meta's flagship open-source LLM |
| **Phi** | `final_evaluation_Phi.csv` | Microsoft's efficient small language model |
| **Qwen** | `final_evaluation_Qwen.csv` | Alibaba's advanced LLM |

Each evaluation includes metrics such as:
- Retrieval accuracy
- Answer relevance
- Clinical accuracy
- Response quality
- Processing time

---

## Vector Database

The `Vector_DB/` directory stores:
- Indexed embeddings for fast similarity search
- Metadata mappings for chunk-to-document tracing
- Query optimization indexes

This enables efficient semantic retrieval of relevant clinical guidelines for RAG queries.

---

## Documentation

- **Full Report:** See `LLM_Project_Report.pdf` for comprehensive project details
- **Presentation:** See `PresSlides.pdf` for project overview

---

## Configuration

### Data Extraction Settings (`Data_Extraction.py`)
```python
MAIN_URL = "https://www.aasld.org/practice-guidelines"
RATE_LIMIT_REQUESTS = 1.5  # seconds between requests
RATE_LIMIT_SELENIUM = 5.0  # seconds for dynamic content
CLOUDFLARE_WAIT = 15       # seconds to bypass Cloudflare
SELENIUM_TIMEOUT = 30      # seconds for page load
```

### Chunking Configuration (`LLM_Preprocessing.ipynb`)
```python
CHUNK_SIZE = 512           # tokens per chunk
CHUNK_OVERLAP = 25         # overlap between chunks
```

### Embedding Model
```
Model: pritamdeka/S-Bluebert-snli-multinli-stsb
Device: CUDA (GPU)
Batch Size: 256
Output Dimension: 768
```

---

## Use Cases

1. **Clinical Decision Support** - Retrieve relevant AASLD guidelines for patient cases
2. **Literature Review** - Search hepatology guidelines semantically
3. **Medical Education** - Study comprehensive clinical recommendations
4. **Research** - Benchmark LLM performance on medical content
