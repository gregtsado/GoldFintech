import pandas as pd


def data_quality(**kwargs):
    data = pd.read_csv("./msftstock.csv")
    assert data['volume'].isnull().sum() == 0, "Missing values detected!"
    assert (data['volume'] > 0).all(), "Invalid stock prices detected!"
    
    
    

    # Completeness Check
    missing_data = data.isnull().sum()
    if missing_data.any():
        print(f"Missing Data:\n{missing_data}")

    # Accuracy Check
    invalid_volume = data[data['volume'] <= 0]
    if not invalid_volume.empty:
        print(f"Invalid Prices Detected:\n{invalid_volume}")

    # Duplicates Check
    duplicates = data.duplicated().sum()
    if duplicates > 0:
        print(f"Duplicate Records Found: {duplicates}")
