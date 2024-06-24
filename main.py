from datetime import datetime, timedelta

import pandas as pd
import requests
import yfinance as yf

# Dividenden Screener


def add_business_days(start_date: datetime, business_days: int) -> datetime:
    """
    Add a specified number of business days to a start date.

    Parameters:
    start_date (datetime): The starting date.
    business_days (int): The number of business days to add.

    Returns:
    datetime: The resulting date after adding the business days.
    """
    current_date = start_date
    days_added = 0

    while days_added < business_days:
        current_date += timedelta(days=1)
        if current_date.weekday() < 5:  # Monday to Friday are business days
            days_added += 1

    return current_date


def get_dividend_day(div_date: datetime = None) -> pd.DataFrame:
    """
    Fetch dividend data for a specific date from the NASDAQ API.

    Parameters:
    div_date (datetime): The date for which to fetch dividend data. Defaults to today.

    Returns:
    pd.DataFrame: A DataFrame containing the dividend data.
    """
    if div_date is None:
        div_date = datetime.now()

    url = "https://api.nasdaq.com/api/calendar/dividends"
    headers = {
        "Accept": "application/json",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        ),
    }
    payload = {"date": div_date.strftime("%Y-%m-%d")}

    response = requests.get(url, headers=headers, params=payload)

    if response.status_code == 200:
        json_data = response.json()
        df = pd.DataFrame(json_data.get("data", {}).get("calendar", {}).get("rows", []))

        if not df.empty:
            df["dividend_Ex_Date"] = pd.to_datetime(
                df["dividend_Ex_Date"], errors="coerce"
            )
            df["adr"] = df.companyName.str.contains("ADR")
            df["etf"] = df.companyName.str.contains("ETF")
            df["bond"] = df.companyName.str.contains("Bond")

            return df

    return pd.DataFrame()


def get_dividend_days(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetch dividend data for a range of dates.

    Parameters:
    start_date (datetime): The starting date.
    end_date (datetime): The ending date.

    Returns:
    pd.DataFrame: A DataFrame containing the dividend data for the date range.
    """
    dfs = []

    while start_date <= end_date:
        if start_date.weekday() < 5:  # Only consider weekdays
            dfs.append(get_dividend_day(start_date))
        start_date += timedelta(days=1)

    return pd.concat(dfs, ignore_index=True)


def export_screener(df: pd.DataFrame) -> None:
    """
    Export the screener data to a formatted output.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the screener data.
    """
    df.rename(
        columns={
            "symbol": "Ticker",
            "dividend_Ex_Date": "Date",
            "dividend_percentage": "Divid %",
            "Last Close": "Close",
            "dividend_Rate": "Divid Rate",
            "roc_5_pos": "5_Days_pos",
            "above_SMA_50": "MA50",
        },
        inplace=True,
    )

    df.set_index("Date", inplace=True)

    print(
        df[
            [
                "Ticker",
                # "Date",
                "Divid %",
                "Volume",
                "Close",
                "Divid Rate",
                "5_Days_pos",
                "MA50",
                "etf",
                "adr",
                "bond",
            ]
        ].to_string()
    )

    df[
        [
            "Ticker",
            "companyName",
            "Divid %",
            "Volume",
            "Close",
            "Divid Rate",
            "5_Days_pos",
            "MA50",
            "etf",
            "adr",
            "bond",
        ]
    ].to_csv("./screener.csv")


def update_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update the stock data with additional financial metrics.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the initial stock data.

    Returns:
    pd.DataFrame: The updated DataFrame with additional financial metrics.
    """
    df["Close"] = 0.0
    df["Volume"] = 0.0
    df["dividend_percentage"] = 0.0
    df["last_close_volume"] = 0.0
    df["close_5_days_ago"] = 0.0
    df["SMA_50"] = 0.0

    for index, row in df.iterrows():
        ticker = yf.Ticker(row.symbol.replace(".", "-"))
        hist = ticker.history()

        if hist.empty or len(hist) < 6:
            continue  # Skip symbols with not enough data

        df.at[index, "Close"] = hist["Close"].iloc[-1]
        df.at[index, "Volume"] = hist["Volume"].iloc[-1]
        df.at[index, "SMA_50"] = hist["Close"].rolling(50).mean().iloc[-1]
        df.at[index, "dividend_percentage"] = round(
            (row.dividend_Rate / hist["Close"].iloc[-1]) * 100, 2
        )
        df.at[index, "last_close_volume"] = round(
            hist["Close"].iloc[-1] * hist["Volume"].iloc[-1]
        )
        df.at[index, "close_5_days_ago"] = hist["Close"].iloc[-5]

    df["roc_5_pos"] = df["Close"] > df["close_5_days_ago"]
    df["above_SMA_50"] = df["Close"] > df["SMA_50"]

    df = df[df["last_close_volume"] > 1_000_000]
    df = df[df["dividend_percentage"] > 3]

    return df.sort_values(by="dividend_Ex_Date")


def main() -> None:
    """
    Main function to execute the dividend screener.
    """
    today = datetime.now()

    df = get_dividend_days(
        start_date=(today - timedelta(days=1)).date(),
        end_date=add_business_days(today, 10).date(),
    )
    df = update_stock_data(df)
    export_screener(df)


if __name__ == "__main__":
    main()
