# Technical Assesment

https://app.snowflake.com/streamlit/ngqfvdy/jj33982/#/apps/33mb2s7t27hxk62px6ka



This application ingests PDF documents from a Snowflake stage, chunks them, stores metadata, and provides a natural language interface for querying document contents with citation support.
Overview

Vectera RAG enables users to upload PDF documents to a Snowflake stage, automatically process them into searchable chunks, and then ask natural language questions about the document contents. The application leverages Snowflake's computational capabilities for text processing and retrieval, providing answers with source citations and relevance scoring.
Features

    PDF Ingestion: Automatically processes PDF files from a Snowflake stage (@REPORTS)

    Intelligent Chunking: Splits documents into overlapping chunks with paragraph and sentence boundary detection

    Metadata Management: Add company/group and version labels to documents for filtered retrieval

    Version-Aware Querying: Filter search scope by document version or company

    Retrieval with Scoring: Returns relevant text chunks with relative relevance scores

    Citation Support: Answers include source file references and chunk locations

    Persistent Storage: All processed content stored in Snowflake tables

Architecture
text

PDF Files → Snowflake Stage (@REPORTS)
         ↓
    REPORTS Table (raw PDF text + metadata)
         ↓
    reports_chunked Table (chunked text with metadata)
         ↓
    REPORTS_LLM() UDF (retrieval + answer generation)
         ↓
