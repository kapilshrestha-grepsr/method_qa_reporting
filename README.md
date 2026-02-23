# Method QA Reporting Tool

This tool automates the process of downloading, mapping, comparing, and reporting on CSV data files for QA purposes. It also generates per-file issue reports for rows with data problems.

## Features

- **Download latest CSV files** from provided links.
- **Map previous and current month files** using smart filename normalization.
- **Generate a summary QA report** comparing file pairs.
- **Create per-file issue CSVs** listing only problematic rows, with an "Issue" column describing the problem(s).

## Folder Structure

```
method_qa_reporting/
├── app.py
├── downloader.py
├── file_compare.py
├── methodusa_parent/
│   ├── current_month_files/
│   ├── prev_month_files/
│   ├── qa_reports/
│   └── methodusa_issue_files/
```

## Usage

1. **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

2. **Run the main script**
    ```bash
    python app.py
    ```

3. **Outputs**
    - Summary report: `methodusa_parent/qa_reports/method_summary_report.csv`
    - Issue files: `methodusa_parent/methodusa_issue_files/` (one CSV per problematic file)

## How It Works

- **Downloading:**  
  Downloads all current month files into `current_month_files/`.

- **Mapping:**  
  Matches previous and current files using a normalization function that extracts the main brand/identifier from filenames.

- **Reporting:**  
  Compares each mapped file pair and generates a summary report with metrics such as missing SKUs, nulls, duplicates, and special characters.

- **Issue Files:**  
  For each file with issues, creates a CSV in `methodusa_issue_files/` containing only the problematic rows, with an `"Issue"` column explaining the problem.

## Customization

- **Filename Normalization:**  
  Edit `normalize_filename` in `file_compare.py` to adjust how files are matched.

- **Issue Detection:**  
  Update logic in `analyze_file_pair` in `file_compare.py` to add or change issue types.

## License

MIT License

---

**Maintainer:**  
Kapil Shrestha  
kapil.shrestha@grepsr.com