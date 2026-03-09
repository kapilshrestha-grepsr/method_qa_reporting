import streamlit as st
import pandas as pd
import dtale
from file_compare import map_files, generate_report, generate_issue_files
from downloader import download_section
from concurrent.futures import ThreadPoolExecutor
import threading

# Define paths
BASE_PATH = "/home/kapil/project/method_qa_reporting/methodusa_parent"
PREV_FOLDER = f"{BASE_PATH}/prev_month_files"
CURR_FOLDER = f"{BASE_PATH}/current_month_files"
OUTPUT_FOLDER = f"{BASE_PATH}/qa_reports"
ISSUES_FOLDER = f"{BASE_PATH}/methodusa_issue_files"

# Streamlit UI
st.title("Method QA Reporting Tool")
st.write("This tool helps you generate QA reports and identify issues in your data files.")

# Button to download files
if st.button("Download Latest Files"):
    st.write("Downloading latest files...")
    try:
        download_section()
        st.success("Files downloaded successfully!")
    except Exception as e:
        st.error(f"Error downloading files: {e}")

# Button to map files and generate report
if st.button("Generate Report"):
    st.write("Mapping files and generating report...")

    try:
        # Map files
        file_pairs = map_files(PREV_FOLDER, CURR_FOLDER)
        # st.write(f"Mapped file pairs: {file_pairs}")

        # Generate the main report
        st.write("Generating the main report...")
        report_path = generate_report(file_pairs, PREV_FOLDER, CURR_FOLDER, OUTPUT_FOLDER, ISSUES_FOLDER)

        # Generate issue files in parallel
        st.write("Generating issue files...")
        with ThreadPoolExecutor() as executor:
            executor.submit(generate_issue_files, file_pairs, PREV_FOLDER, CURR_FOLDER, ISSUES_FOLDER)

        st.success(f"Report generated successfully! Report path: {report_path}")

        # Add a button to open the report in D-Tale
        if st.button("Open Report in D-Tale"):
            st.write("Opening report in D-Tale...")
            try:
                report_df = pd.read_csv(report_path)
                instance = dtale.show(report_df, ignore_duplicate=True)
                st.write(f"D-Tale is running at: {instance._main_url}")
            except Exception as e:
                st.error(f"Error opening report in D-Tale: {e}")
    except Exception as e:
        st.error(f"Error generating report: {e}")