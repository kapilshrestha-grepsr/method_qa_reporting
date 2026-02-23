import os
import re
import pandas as pd
from pandas.errors import EmptyDataError
from datetime import datetime

def normalize_filename(filename: str) -> str:
    ignore_words = {"methodusa", "shop", "com", "merged", "procurement", "llc"}
    name = filename.lower()
    name = re.sub(r'\.csv$', '', name)
    name = re.sub(r'^[\d_\-]+', '', name)
    parts = re.split(r'[_\-\s]+', name)
    for part in parts:
        if re.search(r'[a-z]', part) and part not in ignore_words:
            return part
    return ""

def map_files(PREV_FOLDER, CURR_FOLDER):
    prev_files = [f for f in os.listdir(PREV_FOLDER) if f.endswith(".csv")]
    curr_files = [f for f in os.listdir(CURR_FOLDER) if f.endswith(".csv")]
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

def generate_report(file_pairs, PREV_FOLDER, CURR_FOLDER, OUTPUT_FOLDER, ISSUES_FOLDER):
    special_pattern = r'[@™©®ﾮ]'
    today_str = datetime.today().strftime("%Y-%m-%d")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(ISSUES_FOLDER, exist_ok=True)
    rows = []
    for prev_file, curr_file in file_pairs:
        prev_file_path = os.path.join(PREV_FOLDER, prev_file)
        curr_file_path = os.path.join(CURR_FOLDER, curr_file)
        try:
            df_prev = pd.read_csv(prev_file_path)
            df_curr = pd.read_csv(curr_file_path)
        except (EmptyDataError, Exception):
            rows.append({
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
            })
            continue

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

        if check_cols:
            mask = df_curr[check_cols].apply(
                lambda col: col.astype(str).str.contains(special_pattern, regex=True)
            )
            special_rows_count = mask.any(axis=1).sum()
            special_char = f"Yes ({special_rows_count})" if special_rows_count else "No"
        else:
            special_rows_count = 0
            special_char = "No"

        null_sku_count = df_curr["SKU"].isnull().sum() if "SKU" in df_curr.columns else 0
        null_mfr_count = df_curr.get("MFRPart #", pd.Series()).isnull().sum()

        null_product_url_count = df_curr.filter(
            regex=r'product[\s_]*page[\s_]*url', axis=1
        ).isnull().sum().sum()

        null_image_url_count = df_curr.filter(
            regex=r'image[\s_]*url', axis=1
        ).isnull().sum().sum()

        dup_sku_mask = df_curr["SKU"].duplicated(keep=False) if "SKU" in df_curr.columns else pd.Series([False]*len(df_curr))
        dup_sku_count = dup_sku_mask.sum()
        dup_mfr_mask = df_curr.get("MFRPart #", pd.Series()).duplicated(keep=False)
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
        rows.append({
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
        })

        # --- Issue file generation ---
        if should_open_ticket == "Yes":
            issue_rows = []
            # Null SKU
            if "SKU" in df_curr.columns:
                null_sku_rows = df_curr[df_curr["SKU"].isnull()].copy()
                if not null_sku_rows.empty:
                    null_sku_rows["Issue"] = "Null SKU"
                    issue_rows.append(null_sku_rows)
                # Duplicate SKU
                dup_sku_rows = df_curr[dup_sku_mask].copy()
                if not dup_sku_rows.empty:
                    dup_sku_rows["Issue"] = "Duplicate SKU"
                    issue_rows.append(dup_sku_rows)
            # Null MFRPart #
            if "MFRPart #" in df_curr.columns:
                null_mfr_rows = df_curr[df_curr["MFRPart #"].isnull()].copy()
                if not null_mfr_rows.empty:
                    null_mfr_rows["Issue"] = "Null MFRPart #"
                    issue_rows.append(null_mfr_rows)
                # Duplicate MFRPart #
                dup_mfr_rows = df_curr[dup_mfr_mask].copy()
                if not dup_mfr_rows.empty:
                    dup_mfr_rows["Issue"] = "Duplicate MFRPart #"
                    issue_rows.append(dup_mfr_rows)
            # Null ProductPageURL
            product_url_cols = df_curr.filter(regex=r'product[\s_]*page[\s_]*url', axis=1).columns
            for col in product_url_cols:
                null_product_url_rows = df_curr[df_curr[col].isnull()].copy()
                if not null_product_url_rows.empty:
                    null_product_url_rows["Issue"] = f"Null {col}"
                    issue_rows.append(null_product_url_rows)
            # Null ImageURL
            image_url_cols = df_curr.filter(regex=r'image[\s_]*url', axis=1).columns
            for col in image_url_cols:
                null_image_url_rows = df_curr[df_curr[col].isnull()].copy()
                if not null_image_url_rows.empty:
                    null_image_url_rows["Issue"] = f"Null {col}"
                    issue_rows.append(null_image_url_rows)
            # Special Characters
            if check_cols:
                special_mask = mask.any(axis=1)
                special_char_rows = df_curr[special_mask].copy()
                if not special_char_rows.empty:
                    special_char_rows["Issue"] = "Special Character"
                    issue_rows.append(special_char_rows)
            # Combine all issue rows
            if issue_rows:
                all_issues_df = pd.concat(issue_rows).drop_duplicates()
                issue_file_name = f"{os.path.splitext(curr_file)[0]}_issue_{today_str}.csv"
                issue_file_path = os.path.join(ISSUES_FOLDER, issue_file_name)
                all_issues_df.to_csv(issue_file_path, index=False)
                print(f"Issue file generated: {issue_file_path}")

    report_df = pd.DataFrame(rows)
    report_path = os.path.join(OUTPUT_FOLDER, "method_summary_report.csv")
    report_df.to_csv(report_path, index=False)
    print(f"Report generated: {report_path}")
    return report_path