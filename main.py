"""
Vectera RAG — Streamlit UI with Snowflake Integration
"""

from __future__ import annotations

import streamlit as st
from snowflake.snowpark.context import get_active_session
from typing import List, Dict, Any


def safe_get_attribute(row, attr_name, default="N/A"):
    """Safely get attribute from Snowpark Row object."""
    try:
        if hasattr(row, attr_name):
            value = getattr(row, attr_name)
            return value if value is not None else default
        return default
    except:
        return default


def chunk_text(text: str, chunk_size: int = 3000, overlap: int = 200) -> List[str]:
    """
    Split text into overlapping chunks of specified size.
    """
    chunks = []
    start = 0
    text_length = len(text)
    
    while start < text_length:
        end = start + chunk_size
        
        # Try to break at paragraph or sentence boundary
        if end < text_length:
            # Look for paragraph break
            paragraph_break = text.rfind('\n\n', start, end)
            if paragraph_break != -1 and paragraph_break > start + chunk_size // 2:
                end = paragraph_break
            else:
                # Look for sentence break
                sentence_break = max(
                    text.rfind('. ', start, end),
                    text.rfind('! ', start, end),
                    text.rfind('? ', start, end),
                    text.rfind('\n', start, end)
                )
                if sentence_break != -1 and sentence_break > start + chunk_size // 2:
                    end = sentence_break + 1
        
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        
        start = end - overlap if end < text_length else text_length
    
    return chunks


def setup_snowflake_environment(session):
    """Set up the Snowflake environment with proper role, database, and schema."""
    try:
        session.sql("USE ROLE ACCOUNTADMIN").collect()
        session.sql("USE DATABASE LLM").collect()
        session.sql("USE SCHEMA RAG").collect()
        return True
    except Exception as e:
        st.error(f"Error setting up Snowflake environment: {str(e)}")
        return False


def create_reports_table(session):
    """Create or replace the reports table using the PDF stage."""
    try:
        create_reports_sql = """
        CREATE OR REPLACE TABLE REPORTS AS
        WITH filenames AS (SELECT DISTINCT METADATA$FILENAME AS file_name FROM @REPORTS)
        SELECT 
            file_name, 
            py_read_pdf(build_scoped_file_url(@REPORTS, file_name)) AS contents,
            NULL AS company,
            NULL AS version,
            CURRENT_TIMESTAMP() AS upload_date
        FROM filenames
        WHERE file_name IS NOT NULL
        """
        session.sql(create_reports_sql).collect()
        return True
    except Exception as e:
        st.error(f"Error creating reports table: {str(e)}")
        return False


def refresh_reports_table(session):
    """Refresh the reports table with any new PDFs in the stage."""
    try:
        # Get existing files
        existing_files = session.sql("SELECT DISTINCT file_name FROM REPORTS").collect()
        existing_file_names = set([f.FILE_NAME for f in existing_files])
        
        # Get all files from stage
        stage_files_sql = """
        SELECT DISTINCT METADATA$FILENAME AS file_name 
        FROM @REPORTS 
        WHERE METADATA$FILENAME IS NOT NULL
        """
        stage_files = session.sql(stage_files_sql).collect()
        
        # Find new files
        new_files = []
        for file in stage_files:
            if file.FILE_NAME not in existing_file_names:
                new_files.append(file.FILE_NAME)
        
        if new_files:
            # Insert new files
            for new_file in new_files:
                insert_sql = f"""
                INSERT INTO REPORTS (file_name, contents, upload_date)
                SELECT 
                    '{new_file}',
                    py_read_pdf(build_scoped_file_url(@REPORTS, '{new_file}')),
                    CURRENT_TIMESTAMP()
                """
                session.sql(insert_sql).collect()
            
            return len(new_files)
        return 0
    except Exception as e:
        st.error(f"Error refreshing reports table: {str(e)}")
        return 0


def create_reports_chunked_table(session):
    """Create the chunked reports table if it doesn't exist."""
    create_chunked_table_sql = """
    CREATE TABLE IF NOT EXISTS reports_chunked (
        file_name VARCHAR,
        chunk_number INT,
        chunk_text VARCHAR,
        combined_chunk_text VARCHAR,
        company VARCHAR,
        version VARCHAR,
        created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    session.sql(create_chunked_table_sql).collect()


def add_metadata_columns(session):
    """Add company and version columns to REPORTS table if they don't exist."""
    try:
        # Check if columns exist
        columns = session.sql("DESCRIBE TABLE REPORTS").collect()
        column_names = [c["name"].upper() for c in columns]
        
        if 'COMPANY' not in column_names:
            session.sql("ALTER TABLE REPORTS ADD COLUMN company VARCHAR").collect()
        
        if 'VERSION' not in column_names:
            session.sql("ALTER TABLE REPORTS ADD COLUMN version VARCHAR").collect()
            
        if 'UPLOAD_DATE' not in column_names:
            session.sql("ALTER TABLE REPORTS ADD COLUMN upload_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()").collect()
    except Exception as e:
        st.warning(f"Note: {str(e)}")


def update_metadata(session, file_name: str, company: str, version: str):
    """Update metadata for a specific file."""
    update_sql = f"""
    UPDATE REPORTS 
    SET company = '{company.replace("'", "''") if company else ''}',
        version = '{version.replace("'", "''") if version else ''}'
    WHERE file_name = '{file_name.replace("'", "''")}'
    """
    session.sql(update_sql).collect()


def chunk_and_store_reports(session):
    """Chunk all reports and store in reports_chunked table."""
    try:
        # Get all reports
        reports = session.sql("SELECT file_name, contents, company, version FROM REPORTS").collect()
        
        chunks_inserted = 0
        
        for report in reports:
            # Check if already chunked
            existing_chunks = session.sql(
                f"SELECT COUNT(*) as cnt FROM reports_chunked WHERE file_name = '{report.FILE_NAME}'"
            ).collect()
            
            if existing_chunks[0].CNT == 0 and report.CONTENTS:
                # Chunk the content
                chunks = chunk_text(report.CONTENTS)
                
                # Insert chunks
                for chunk_num, chunk_text in enumerate(chunks, 1):
                    combined_text = f"Sampled contents from reports [{report.FILE_NAME}]: {chunk_text}"
                    
                    insert_chunk_sql = f"""
                    INSERT INTO reports_chunked (file_name, chunk_number, chunk_text, combined_chunk_text, company, version)
                    SELECT 
                        '{report.FILE_NAME.replace("'", "''")}',
                        {chunk_num},
                        $${chunk_text.replace("'", "''")}$$,
                        $${combined_text.replace("'", "''")}$$,
                        '{report.COMPANY.replace("'", "''") if report.COMPANY else ''}',
                        '{report.VERSION.replace("'", "''") if report.VERSION else ''}'
                    """
                    session.sql(insert_chunk_sql).collect()
                    chunks_inserted += 1
                
                # Update chunked flag in reports table
                session.sql(
                    f"UPDATE REPORTS SET upload_date = CURRENT_TIMESTAMP() WHERE file_name = '{report.FILE_NAME}'"
                ).collect()
        
        return chunks_inserted
    except Exception as e:
        st.error(f"Error chunking reports: {str(e)}")
        return 0


def main() -> None:
    st.set_page_config(page_title="Vectera RAG", layout="wide")

    st.title("Vectera RAG")
    st.caption(
        "PDF ingestion → chunking → embeddings → retrieval → answers with citations. "
        "Version and conflict-aware prompting; tables via pdfplumber when possible."
    )

    # Initialize Snowflake session
    session = get_active_session()
    
    # Setup Snowflake environment
    if not setup_snowflake_environment(session):
        st.error("Failed to setup Snowflake environment. Please check your permissions.")
        return
    
    # Ensure tables exist
    try:
        add_metadata_columns(session)
        create_reports_chunked_table(session)
    except Exception as e:
        st.sidebar.error(f"Error setting up tables: {str(e)}")

    with st.sidebar:
        st.subheader("PDF Management")
        
        # Show current PDFs in stage
        try:
            stage_files = session.sql(
                "SELECT DISTINCT METADATA$FILENAME AS file_name FROM @REPORTS WHERE METADATA$FILENAME IS NOT NULL"
            ).collect()
            
            if stage_files:
                st.write(f"**{len(stage_files)} PDFs in stage**")
                with st.expander("View PDFs in stage"):
                    for file in stage_files:
                        st.write(f"📄 {file.FILE_NAME}")
            else:
                st.info("No PDFs found in @REPORTS stage")
        except Exception as e:
            st.warning("Unable to access @REPORTS stage")
        
        st.divider()
        
        # PDF Processing
        if st.button("🔄 Process PDFs from Stage", type="primary"):
            with st.spinner("Processing PDFs from stage..."):
                try:
                    # Create/refresh reports table
                    reports_exist = session.sql("SHOW TABLES LIKE 'REPORTS'").collect()
                    
                    if not reports_exist:
                        success = create_reports_table(session)
                        if success:
                            st.success("✅ Created REPORTS table from stage PDFs")
                    else:
                        new_files = refresh_reports_table(session)
                        if new_files > 0:
                            st.success(f"✅ Added {new_files} new PDF(s) to REPORTS table")
                        else:
                            st.info("No new PDFs to process")
                    
                    # Chunk the reports
                    chunks_created = chunk_and_store_reports(session)
                    if chunks_created > 0:
                        st.success(f"✅ Created {chunks_created} chunks")
                    
                    st.cache_data.clear()
                    
                except Exception as e:
                    st.error(f"Error processing PDFs: {str(e)}")
        
        st.divider()
        st.subheader("Metadata Management")
        
        # Get list of files in REPORTS table
        try:
            reports_files = session.sql(
                "SELECT file_name, company, version FROM REPORTS ORDER BY file_name"
            ).collect()
            
            if reports_files:
                selected_file = st.selectbox(
                    "Select PDF to update metadata",
                    options=[f.FILE_NAME for f in reports_files],
                    key="metadata_file"
                )
                
                # Get current metadata
                current_metadata = next(
                    (f for f in reports_files if f.FILE_NAME == selected_file), 
                    None
                )
                
                company = st.text_input(
                    "Company / document group",
                    value=current_metadata.COMPANY if current_metadata and current_metadata.COMPANY else "",
                    placeholder="e.g. Acme Corp",
                    help="Used to group related filings",
                    key="company_input"
                )
                
                version = st.text_input(
                    "Document version label",
                    value=current_metadata.VERSION if current_metadata and current_metadata.VERSION else "",
                    placeholder="e.g. FY2024, Q3-2023 draft",
                    help="Critical: same company may have multiple versions",
                    key="version_input"
                )
                
                if st.button("💾 Update Metadata", type="primary"):
                    update_metadata(session, selected_file, company, version)
                    
                    # Update chunks table with new metadata
                    update_chunks_sql = f"""
                    UPDATE reports_chunked 
                    SET company = '{company.replace("'", "''") if company else ''}',
                        version = '{version.replace("'", "''") if version else ''}'
                    WHERE file_name = '{selected_file.replace("'", "''")}'
                    """
                    session.sql(update_chunks_sql).collect()
                    
                    st.success(f"✅ Updated metadata for {selected_file}")
                    st.cache_data.clear()
            else:
                st.info("No PDFs processed yet. Click 'Process PDFs from Stage' first.")
        except Exception as e:
            st.error(f"Error loading files: {str(e)}")

        st.divider()
        st.subheader("Retrieval scope")
        
        # Get unique companies and versions for filters
        try:
            companies_query = """
            SELECT DISTINCT company 
            FROM reports_chunked 
            WHERE company IS NOT NULL AND company != '' 
            ORDER BY company
            """
            companies = session.sql(companies_query).collect()
            company_options = ["—"] + [c.COMPANY for c in companies]
            
            versions_query = """
            SELECT DISTINCT version 
            FROM reports_chunked 
            WHERE version IS NOT NULL AND version != '' 
            ORDER BY version
            """
            versions = session.sql(versions_query).collect()
            version_options = ["—"] + [v.VERSION for v in versions]
        except:
            company_options = ["—"]
            version_options = ["—"]
        
        filter_scope = st.radio(
            "Limit context",
            ("All ingested documents", "Filter by version label", "Filter by company/group"),
            index=0,
            key="filter_scope"
        )
        
        selected_version = st.selectbox("Version", options=version_options, key="version_filter")
        selected_company = st.selectbox("Company / group", options=company_options, key="company_filter")

        chunks_to_retrieve = st.slider("Chunks to retrieve", min_value=4, max_value=16, value=8)

        st.divider()
        st.caption(
            "**Charts:** raster figures are not read—only text and extracted tables. "
            "**Tables:** pdfplumber grid extraction may misalign complex layouts."
        )

        if st.button("🗑️ Clear All Data"):
            with st.spinner("Clearing all data..."):
                try:
                    session.sql("DROP TABLE IF EXISTS reports_chunked").collect()
                    session.sql("DROP TABLE IF EXISTS reports").collect()
                    create_reports_chunked_table(session)
                    st.success("All data cleared successfully")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"Error clearing data: {str(e)}")

    col_q, col_meta = st.columns([2, 1])
    with col_q:
        question = st.text_area(
            "Question",
            placeholder="Ask something answerable from your PDFs…",
            value="What drives demand according to these materials",
            height=100,
        )
        
        if st.button(":snowflake: Submit", type="primary"):
            if question.strip():
                with st.spinner("Querying Snowflake..."):
                    try:
                        # Build WHERE clause based on filters
                        where_clauses = []
                        if filter_scope == "Filter by version label" and selected_version != "—":
                            where_clauses.append(f"version = '{selected_version}'")
                        elif filter_scope == "Filter by company/group" and selected_company != "—":
                            where_clauses.append(f"company = '{selected_company}'")
                        
                        # Query Snowflake using REPORTS_LLM function
                        manuals_query = f"""
                        SELECT * FROM TABLE(REPORTS_LLM('{question}'))
                        """
                        manuals_response = session.sql(manuals_query).collect()
                        
                        # Store in session state for persistence
                        st.session_state['manuals_response'] = manuals_response
                        st.session_state['question_asked'] = question
                        
                        # Create Tabs for response display
                        tab1, tab2, tab3 = st.tabs(["Reports", "Internal Logs", "Combined Insights"])
                        
                        with tab1:
                            if manuals_response:
                                available_fields = list(manuals_response[0].as_dict().keys())
                                
                                st.subheader("Response")
                                
                                file_field = 'FILE_NAME' if 'FILE_NAME' in available_fields else 'file_name'
                                response_field = 'RESPONSE' if 'RESPONSE' in available_fields else 'response'
                                
                                if file_field in available_fields:
                                    st.write(f"**Source File:** {safe_get_attribute(manuals_response[0], file_field)}")
                                
                                st.markdown("**Answer:**")
                                if response_field in available_fields:
                                    st.write(safe_get_attribute(manuals_response[0], response_field))
                                
                                st.subheader("Chunks and their relative scores:")
                                
                                for i, chunk in enumerate(manuals_response):
                                    score = "N/A"
                                    for score_field in ['RELATIVE_SCORE', 'relative_score', 'SCORE', 'score', 'SIMILARITY', 'similarity']:
                                        if score_field in available_fields:
                                            score_val = safe_get_attribute(chunk, score_field, None)
                                            if score_val and score_val != "N/A":
                                                score = score_val
                                                break
                                    
                                    chunk_content = "No chunk content available"
                                    for chunk_field in ['CHUNK', 'chunk_text', 'combined_chunk_text', 'CHUNK_TEXT']:
                                        if chunk_field in available_fields:
                                            content = safe_get_attribute(chunk, chunk_field, None)
                                            if content and content != "N/A":
                                                chunk_content = content
                                                break
                                    
                                    chunk_num = safe_get_attribute(chunk, 'chunk_number', str(i+1))
                                    
                                    with st.expander(f"Chunk {chunk_num} - Score: {score}"):
                                        chunk_dict = chunk.as_dict()
                                        for key, value in chunk_dict.items():
                                            if key not in ['RESPONSE', 'response']:
                                                if value is not None:
                                                    if 'chunk' in key.lower() or 'text' in key.lower():
                                                        st.write(f"**{key}:**")
                                                        st.text(str(value)[:500] + "..." if len(str(value)) > 500 else str(value))
                                                    else:
                                                        st.write(f"**{key}:** {value}")
                            else:
                                st.warning("No results returned from the query.")
                        
                        
                        
                        with tab3:
                            st.subheader("Combined Insights")
                            if manuals_response:
                                st.markdown("### Key Findings")
                                response_field = 'RESPONSE' if 'RESPONSE' in available_fields else 'response'
                                if response_field in available_fields:
                                    st.write(safe_get_attribute(manuals_response[0], response_field))
                                
                                st.divider()
                                st.markdown("### Supporting Evidence")
                                
                                for i, chunk in enumerate(manuals_response[:3]):
                                    chunk_dict = chunk.as_dict()
                                    
                                    file_name = "Unknown file"
                                    for field in ['FILE_NAME', 'file_name']:
                                        if field in chunk_dict:
                                            file_name = chunk_dict[field]
                                            break
                                    
                                    chunk_text = "No content"
                                    for field in ['CHUNK', 'chunk_text', 'combined_chunk_text']:
                                        if field in chunk_dict:
                                            chunk_text = chunk_dict[field]
                                            break
                                    
                                    st.markdown(f"**From {file_name}**")
                                    st.text(chunk_text[:500] + "..." if len(chunk_text) > 500 else chunk_text)
                                    if i < 2:
                                        st.divider()
                    
                    except Exception as e:
                        st.error(f"Error querying Snowflake: {str(e)}")
                        st.info("Please check your Snowflake connection and the REPORTS_LLM function.")
            else:
                st.warning("Please enter a question.")

    with col_meta:
        # Display statistics
        try:
            # Total chunks
            chunk_count = session.sql("SELECT COUNT(*) as total FROM reports_chunked").collect()[0].TOTAL
            st.metric("Chunks in index", chunk_count)
            
            # Total PDFs
            pdf_count = session.sql("SELECT COUNT(DISTINCT file_name) as total FROM reports").collect()[0].TOTAL
            st.metric("PDFs processed", pdf_count)
        except:
            st.metric("Chunks in index", "—")
            st.metric("PDFs processed", "—")
        
        with st.expander("Recent ingest notes"):
            try:
                recent_query = """
                SELECT 
                    file_name,
                    COUNT(*) as chunk_count,
                    MAX(company) as company,
                    MAX(version) as version,
                    MAX(created_date) as upload_date
                FROM reports_chunked 
                GROUP BY file_name
                ORDER BY upload_date DESC NULLS LAST
                LIMIT 5
                """
                recent_ingests = session.sql(recent_query).collect()
                if recent_ingests:
                    for ingest in recent_ingests:
                        company_str = f", '{ingest.COMPANY}'" if ingest.COMPANY else ""
                        version_str = f", v'{ingest.VERSION}'" if ingest.VERSION else ""
                        st.write(f"**{ingest.FILE_NAME}** — {ingest.CHUNK_COUNT} chunks{company_str}{version_str}")
                else:
                    st.write("No PDFs processed yet")
            except Exception as e:
                st.write("No PDFs processed yet")

    # Display answer section
    st.subheader("Answer")
    if 'manuals_response' in st.session_state and st.session_state['manuals_response']:
        manuals_response = st.session_state['manuals_response']
        response_text = None
        for field in ['RESPONSE', 'response']:
            if hasattr(manuals_response[0], field):
                response_text = getattr(manuals_response[0], field)
                break
        
        if response_text:
            st.markdown(response_text)
        else:
            st.markdown("*Response field not found*")
    else:
        st.markdown("*Answer will appear here after submission*")

    st.subheader("Retrieved sources (for verification)")
    if 'manuals_response' in st.session_state and st.session_state['manuals_response']:
        for i, chunk in enumerate(st.session_state['manuals_response'][:3]):
            chunk_dict = chunk.as_dict()
            
            file_name = chunk_dict.get('FILE_NAME', chunk_dict.get('file_name', 'Unknown'))
            chunk_num = chunk_dict.get('chunk_number', i+1)
            chunk_text = None
            
            for field in ['chunk_text', 'combined_chunk_text', 'CHUNK']:
                if field in chunk_dict:
                    chunk_text = chunk_dict[field]
                    break
            
            with st.expander(f"{file_name} · Chunk {chunk_num}"):
                for key, value in chunk_dict.items():
                    if 'chunk' not in key.lower() and 'text' not in key.lower():
                        st.write(f"**{key}:** {value}")
                
                if chunk_text:
                    st.text(chunk_text[:1000] + "..." if len(chunk_text) > 1000 else chunk_text)
                else:
                    st.text("No chunk text available")
    else:
        with st.expander("No sources retrieved yet"):
            st.text("Submit a question to see relevant sources")


if __name__ == "__main__":
    main()