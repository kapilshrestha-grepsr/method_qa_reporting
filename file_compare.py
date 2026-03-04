import os
import re
import pandas as pd
from pandas.errors import EmptyDataError
from datetime import datetime

def normalize_filename(filename: str) -> str:
    """
    Extract a robust identifier from the filename:
    - Remove extensions like .csv
    - Remove numeric prefixes, timestamps, and dates
    - Ignore common words like methodusa, shop, com, merged, procurement, llc
    - Extract the most meaningful part of the filename (e.g., domain or brand name)
    """
    ignore_words = {"methodusa", "shop", "com", "merged", "procurement", "llc"}
    name = filename.lower()
    name = re.sub(r'\.csv$', '', name)  # Remove extension
    name = re.sub(r'^[\d_\-]+', '', name)  # Remove leading numbers, underscores, or dashes
    parts = re.split(r'[_\-\s]+', name)  # Split by underscores, dashes, or spaces

    # Filter out ignored words and non-meaningful parts
    meaningful_parts = [part for part in parts if part not in ignore_words and re.search(r'[a-z]', part)]

    # If the filename contains a domain-like structure, extract the domain
    for part in meaningful_parts:
        if "_com" in part or "_net" in part or "_org" in part:
            return part.split("_")[0]  # Extract the domain name (e.g., scottsdental from scottsdental_com)

    # Join remaining meaningful parts for better matching
    return "_".join(meaningful_parts)

def map_files(prev_folder, curr_folder):
    prev_files = [f for f in os.listdir(prev_folder) if f.endswith(".csv")]
    curr_files = [f for f in os.listdir(curr_folder) if f.endswith(".csv")]
    prev_norm_map = {f: normalize_filename(f) for f in prev_files}
    curr_norm_map = {f: normalize_filename(f) for f in curr_files}
    file_pairs = []
    used_curr = set()
    for prev_file, prev_norm in prev_norm_map.items():
        for curr_file, curr_norm in curr_norm_map.items():
            if curr_file in used_curr:
                continue
            if prev_norm and (prev_norm in curr_norm or curr_norm in prev_norm):
                file_pairs.append((prev_file, curr_file))
                used_curr.add(curr_file)
                break
    return file_pairs

def analyze_file_pair(prev_file, curr_file, prev_folder, curr_folder):
    special_pattern = r'[@™©®ﾮ]'
    prev_file_path = os.path.join(prev_folder, prev_file)
    curr_file_path = os.path.join(curr_folder, curr_file)
    try:
        df_prev = pd.read_csv(prev_file_path)
        df_curr = pd.read_csv(curr_file_path)
    except (EmptyDataError, Exception):
        return {
            "row": {
                "Previous_File": prev_file,
                "Current_File": curr_file,
                "Previous_SKU_Count": "",
                "Current_SKU_Count": "",
                "Missing_Count_From_Previous": "",
                "Special_Character_Found": "",
                "Null_SKU_Present": "",
                "Null_MFRPart_Present": "",
                "Null_ProductPageURL_Present": "",
                "Null_ImageURL_Present": "",
                "Duplicate_SKU_Present": "",
                "Duplicate_MFRPart_Present": "",
                "Should_Open_Ticket": "Yes",
                "Reason_To_Open_Ticket": "Issue in file"
            },
            "issues": [],
            "df_curr": None,
            "df_prev": None
        }

    df_prev.columns = df_prev.columns.str.strip()
    df_curr.columns = df_curr.columns.str.strip()

    prev_count = len(df_prev)
    curr_count = len(df_curr)

    missing_count = (
        (~df_prev["SKU"].isin(df_curr["SKU"])).sum()
        if "SKU" in df_prev.columns and "SKU" in df_curr.columns
        else 0
    )

    exclude_cols = [
        col for col in df_curr.columns
        if re.search(r'product[\s_]*page[\s_]*url|image[\s_]*url', col, re.I)
    ]
    check_cols = [col for col in df_curr.columns if col not in exclude_cols]

    # Special character mask
    if check_cols:
        mask = df_curr[check_cols].apply(
            lambda col: col.astype(str).str.contains(special_pattern, regex=True)
        )
        special_rows_count = mask.any(axis=1).sum()
        special_char = f"Yes ({special_rows_count})" if special_rows_count else "No"
        special_mask = mask.any(axis=1)
    else:
        special_rows_count = 0
        special_char = "No"
        special_mask = pd.Series([False]*len(df_curr))

    null_sku_mask = df_curr["SKU"].isnull() if "SKU" in df_curr.columns else pd.Series([False]*len(df_curr))
    null_mfr_mask = df_curr.get("MFRPart #", pd.Series()).isnull()
    product_url_cols = df_curr.filter(regex=r'product[\s_]*page[\s_]*url', axis=1).columns
    image_url_cols = df_curr.filter(regex=r'image[\s_]*url', axis=1).columns

    null_product_url_masks = [df_curr[col].isnull() for col in product_url_cols]
    null_image_url_masks = [df_curr[col].isnull() for col in image_url_cols]

    dup_sku_mask = df_curr["SKU"].duplicated(keep=False) if "SKU" in df_curr.columns else pd.Series([False]*len(df_curr))
    dup_mfr_mask = df_curr.get("MFRPart #", pd.Series()).duplicated(keep=False)

    null_sku_count = null_sku_mask.sum()
    null_mfr_count = null_mfr_mask.sum()
    null_product_url_count = sum(mask.sum() for mask in null_product_url_masks)
    null_image_url_count = sum(mask.sum() for mask in null_image_url_masks)
    dup_sku_count = dup_sku_mask.sum()
    dup_mfr_count = dup_mfr_mask.sum()

    reasons = []
    if curr_count < prev_count:
        reasons.append(f"Less data count ({curr_count} < {prev_count})")
    if missing_count:
        reasons.append(f"{missing_count} missing SKUs")
    if special_rows_count:
        reasons.append(f"Special character found ({special_rows_count} rows)")
    if null_sku_count:
        reasons.append(f"Null SKU present ({null_sku_count})")
    if null_mfr_count:
        reasons.append(f"Null MFRPart # present ({null_mfr_count})")
    if null_product_url_count:
        reasons.append(f"Null ProductPageURL present ({null_product_url_count})")
    if null_image_url_count:
        reasons.append(f"Null ImageURL present ({null_image_url_count})")
    if dup_sku_count:
        reasons.append(f"Duplicate SKU present ({dup_sku_count})")
    if dup_mfr_count:
        reasons.append(f"Duplicate MFRPart # present ({dup_mfr_count})")

    should_open_ticket = "Yes" if reasons else "No"
    row = {
        "Previous_File": prev_file,
        "Current_File": curr_file,
        "Previous_SKU_Count": prev_count,
        "Current_SKU_Count": curr_count,
        "Missing_Count_From_Previous": int(missing_count),
        "Special_Character_Found": special_char,
        "Null_SKU_Present": f"Yes ({null_sku_count})" if null_sku_count else "No",
        "Null_MFRPart_Present": f"Yes ({null_mfr_count})" if null_mfr_count else "No",
        "Null_ProductPageURL_Present": f"Yes ({null_product_url_count})" if null_product_url_count else "No",
        "Null_ImageURL_Present": f"Yes ({null_image_url_count})" if null_image_url_count else "No",
        "Duplicate_SKU_Present": f"Yes ({dup_sku_count})" if dup_sku_count else "No",
        "Duplicate_MFRPart_Present": f"Yes ({dup_mfr_count})" if dup_mfr_count else "No",
        "Should_Open_Ticket": should_open_ticket,
        "Reason_To_Open_Ticket": "; ".join(reasons)
    }

    # Issue masks and labels for issue file generation
    issue_masks = []
    issue_labels = []
    if "SKU" in df_curr.columns:
        issue_masks.append(null_sku_mask)
        issue_labels.append("Null SKU")
        issue_masks.append(dup_sku_mask)
        issue_labels.append("Duplicate SKU")
    if "MFRPart #" in df_curr.columns:
        issue_masks.append(null_mfr_mask)
        issue_labels.append("Null MFRPart #")
        issue_masks.append(dup_mfr_mask)
        issue_labels.append("Duplicate MFRPart #")
    for col, mask in zip(product_url_cols, null_product_url_masks):
        issue_masks.append(mask)
        issue_labels.append(f"Null {col}")
    for col, mask in zip(image_url_cols, null_image_url_masks):
        issue_masks.append(mask)
        issue_labels.append(f"Null {col}")
    issue_masks.append(special_mask)
    issue_labels.append("Special Character")

    return {
        "row": row,
        "issues": list(zip(issue_masks, issue_labels)),
        "df_curr": df_curr,
        "df_prev": df_prev
    }

def generate_report(file_pairs, prev_folder, curr_folder, output_folder, ISSUES_FOLDER):
    os.makedirs(output_folder, exist_ok=True)
    rows = []
    for prev_file, curr_file in file_pairs:
        result = analyze_file_pair(prev_file, curr_file, prev_folder, curr_folder)
        rows.append(result["row"])
    report_df = pd.DataFrame(rows)
    report_path = os.path.join(output_folder, "method_summary_report.csv")
    report_df.to_csv(report_path, index=False)
    print(f"Report generated: {report_path}")
    return report_path

def generate_issue_files(file_pairs, prev_folder, curr_folder, issues_folder):
    os.makedirs(issues_folder, exist_ok=True)
    today_str = datetime.today().strftime("%Y-%m-%d")
    for prev_file, curr_file in file_pairs:
        result = analyze_file_pair(prev_file, curr_file, prev_folder, curr_folder)
        row = result["row"]
        df_curr = result["df_curr"]
        df_prev = result["df_prev"]
        if row["Should_Open_Ticket"] == "Yes" and df_curr is not None and df_prev is not None:
            issue_rows = []
            # Standard issues
            for mask, label in result["issues"]:
                rows = df_curr[mask].copy()
                if not rows.empty:
                    rows["Issue"] = label
                    issue_rows.append(rows)
            # Detailed QA: Compare key columns for each SKU
            if "SKU" in df_prev.columns and "SKU" in df_curr.columns:
                df_curr_sku_map = df_curr.set_index("SKU")
                key_columns = [col for col in df_prev.columns if re.search(r'product[\s_]*page[\s_]*url|image[\s_]*url', col, re.I)]
                for idx, prev_row in df_prev.iterrows():
                    sku = prev_row["SKU"]
                    if sku in df_curr_sku_map.index:
                        curr_row = df_curr_sku_map.loc[sku]
                        for col in key_columns:
                            prev_val = prev_row.get(col, "")
                            curr_val = curr_row.get(col, "")
                            if pd.notnull(prev_val) and (pd.isnull(curr_val) or curr_val == ""):
                                flagged = curr_row.copy()
                                flagged["Issue"] = f"{col} missing for SKU {sku}"
                                issue_rows.append(pd.DataFrame(flagged))
            if issue_rows:
                all_issues_df = pd.concat(issue_rows).drop_duplicates()
                issue_file_name = f"{os.path.splitext(curr_file)[0]}_issue_{today_str}.csv"
                issue_file_path = os.path.join(issues_folder, issue_file_name)
                all_issues_df.to_csv(issue_file_path, index=False)
                print(f"Issue file generated: {issue_file_path}")