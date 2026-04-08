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

# Architecture


PDF Files → Snowflake Stage (@REPORTS)
         → 
    REPORTS Table (raw PDF text + metadata)
         → 
    reports_chunked Table (chunked text with metadata)
         → 
    REPORTS_LLM() UDF (retrieval + answer generation)

Setup:

In snowflake workspace in a ipybn file create a vector database. 



Then create a chunk database like this for example:

    CREATE OR REPLACE TABLE reports_chunked AS 
    WITH RECURSIVE split_contents AS (
        SELECT 
            file_name,
            SUBSTRING(contents, 1, 3000) AS chunk_text,
            SUBSTRING(contents, 2001) AS remaining_contents,
            1 AS chunk_number
        FROM 
            reports
    
        UNION ALL
    
        SELECT 
            file_name,
            SUBSTRING(remaining_contents, 1, 3000),
            SUBSTRING(remaining_contents, 2001),
            chunk_number + 1
        FROM 
            split_contents
        WHERE 
            LENGTH(remaining_contents) > 0
    )
    SELECT 
        file_name,
        chunk_number,
        chunk_text,
        CONCAT(
            'Sampled contents from reports [', 
            file_name,
            ']: ', 
            chunk_text
        ) AS combined_chunk_text
    FROM 
        split_contents
    ORDER BY 
        file_name,
        chunk_number;
        
You'll have to hire me to get the full setup! :)

Finally test this streamlit app!
