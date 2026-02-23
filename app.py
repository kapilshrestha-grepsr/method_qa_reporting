from file_compare import map_files, generate_report
from downloader import download_section

BASE_PATH = "/home/kapil/project/method_qa_reporting/methodusa_parent"
PREV_FOLDER = f"{BASE_PATH}/prev_month_files"
CURR_FOLDER = f"{BASE_PATH}/current_month_files"
OUTPUT_FOLDER = f"{BASE_PATH}/qa_reports"
ISSUES_FOLDER = f"{BASE_PATH}/methodusa_issue_files"

def main():
    print("Welcome to Method QA Reporting Tool")
    print("\n--- Downloading latest files ---")
    download_section()

    print("\n--- Mapping Files ---")
    file_pairs = map_files(PREV_FOLDER, CURR_FOLDER)
    print("Mapped file pairs:", file_pairs)

    print("\n--- Generating Report ---")
    report_path = generate_report(file_pairs, PREV_FOLDER, CURR_FOLDER, OUTPUT_FOLDER, ISSUES_FOLDER)
    print(f"Report generated: {report_path}")

if __name__ == "__main__":
    main()