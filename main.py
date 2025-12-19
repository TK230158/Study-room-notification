import pdfplumber as pl
import pandas as pd
import requests
import base64
import os
from datetime import datetime

# constants
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
GAS_URL = os.getenv("GAS_URL")

def download_pdf():
    try:
        response = requests.get(GAS_URL, allow_redirects=True)
        if response.status_code == 200:
            json_data = response.json()
            pdf_content = base64.b64decode(json_data["data"])
            with open("temp.pdf", "wb") as f:
                f.write(pdf_content)
            print("PDF downloaded successfully.")
        else:
            print(f"Failed to download PDF. Status: {response.status_code}")
    except Exception as e:
        print(f"Download error: {e}")

def get_room_data(pdf_path):
    with pl.open(pdf_path) as pdf:
        table_settings = {"vertical_strategy": "lines", "horizontal_strategy": "lines"}
        page = pdf.pages[0]
        table = page.extract_table(table_settings)
        df = pd.DataFrame(table)

    df.columns = range(len(df.columns))
    df = df[df[2].str.contains('談話室|自習室', na=False)].copy()
    df[[0, 1, 9]] = df[[0, 1, 9]].ffill()

    def safe_fill_periods(row):
        periods = row[3:9].copy()
        periods = periods.replace("", None)
        periods = periods.ffill().bfill()
        row[3:9] = periods
        return row

    df = df.apply(safe_fill_periods, axis=1)
    df_melted = df.melt(id_vars=[0, 1, 2, 9], value_vars=[3, 4, 5, 6, 7, 8], 
                        var_name="時限_idx", value_name="部屋番号")
    df_melted = df_melted.dropna(subset=["部屋番号"])
    df_melted = df_melted[~df_melted["部屋番号"].isin(["ー", "－", "―", "None", ""])]
    df_melted["部屋番号"] = df_melted["部屋番号"].str.replace(' ', '').str.split("・")
    df_final = df_melted.explode("部屋番号").reset_index(drop=True)
    
    df_final.columns = ["日付", "曜日", "タイプ", "退館時間", "時限_idx", "部屋番号"]
    return df_final

def get_current_period_info():
    now = datetime.now().time()
    periods = [
        ("09:15", "10:45", 3), ("10:55", "12:25", 4), ("13:20", "14:50", 5),
        ("15:00", "16:30", 6), ("16:40", "18:10", 7), ("18:20", "19:50", 8)
    ]
    for start, end, idx in periods:
        start_time = datetime.strptime(start, "%H:%M").time()
        end_time = datetime.strptime(end, "%H:%M").time()
        if start_time <= now <= end_time:
            return idx, start, end
    return None, None, None

def process_and_notify(df):
    today_str = datetime.now().strftime("%m/%d")
    current_idx, s_time, e_time = get_current_period_info()

    if not current_idx:
        print("Outside of school hours.")
        return

    target_data = df[
        (df["日付"].str.contains(today_str, na=False)) & 
        (df["時限_idx"] == current_idx)
    ]

    if target_data.empty:
        print("No rooms available for the current period.")
        return

    info = target_data.iloc[0]

    def format_rooms(room_type):
        rooms = target_data[target_data["タイプ"].str.contains(room_type, na=False)]["部屋番号"].unique()
        return " / ".join(rooms) if len(rooms) > 0 else "None"

    payload = {
        "date": f"{info['日付']}({info['曜日']})",
        "idx": int(current_idx),
        "start_time": s_time,
        "end_time": e_time,
        "lounge": format_rooms("談話室"),
        "study_room": format_rooms("自習室")
    }

    requests.post(SLACK_WEBHOOK_URL, json=payload)
    print(f"Success! Lounge: {payload['lounge']}, Study Room: {payload['study_room']}")

if __name__ == "__main__":
    try:
        download_pdf()
        if os.path.exists("temp.pdf"):
            final_df = get_room_data("temp.pdf")
            process_and_notify(final_df)
    except Exception as e:
        print(f"Main error: {e}")