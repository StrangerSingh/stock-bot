import pandas as pd 
import os
import threading
import queue
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import requests
import yagmail
from datetime import datetime
from keep_alive import keep_alive  # for Replit webserver

# --- ALERT SPAM PROTECTION ---
alert_counters = {}
MAX_ALERTS_PER_TRIGGER = 5

# === CONFIGURATION ===
GOOGLE_SHEET_NAME = "Monthly ATH Stocks"
ACTIVE_HOLDINGS_SHEET = "Active Holdings"
USER_DIRECTORY_SHEET = "UserDirectory"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")

CHECK_INTERVAL = 5 * 60  # 5 minutes

# --- Setup yagmail SMTP ---
yag = yagmail.SMTP(GMAIL_USER, GMAIL_APP_PASSWORD)

# Start keep-alive web server
keep_alive()

def send_telegram_alert(chat_id, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": chat_id, "text": message})
    except Exception as e:
        print(f"[DEBUG] Telegram send error: {e}")

def send_email_alert(email, subject, message):
    try:
        yag.send(to=email, subject=subject, contents=message)
    except Exception as e:
        print(f"[DEBUG] Email send error to {email}: {e}")

# --- Multithreaded price fetcher using yfinance ---
def fetch_price(symbol, out_q):
    yf_symbol = symbol.upper() + ".NS"
    try:
        ticker = yf.Ticker(yf_symbol)
        hist = ticker.history(period="1d")
        if not hist.empty:
            last_price = hist["Close"].iloc[-1]
            out_q.put((symbol, last_price))
        else:
            out_q.put((symbol, None))
    except Exception as e:
        print(f"[DEBUG] Error for {symbol}: {e}")
        out_q.put((symbol, None))

def get_prices(symbols):
    threads = []
    out_q = queue.Queue()
    for s in symbols:
        t = threading.Thread(target=fetch_price, args=(s, out_q))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    result = {}
    while not out_q.empty():
        sym, price = out_q.get()
        result[sym] = price
    return result

def load_sheets():
    # Load credentials and connect to Google Sheets
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    ath_sheet = client.open(GOOGLE_SHEET_NAME).sheet1
    active_sheet = client.open(GOOGLE_SHEET_NAME).worksheet(
        ACTIVE_HOLDINGS_SHEET)
    user_sheet = client.open(GOOGLE_SHEET_NAME).worksheet(USER_DIRECTORY_SHEET)
    alert_log_sheet = client.open(GOOGLE_SHEET_NAME).worksheet("Alert_Log")

    ath_data = ath_sheet.get_all_records()
    user_data = user_sheet.get_all_records()
    active_data = active_sheet.get_all_records()
    alert_log_data = alert_log_sheet.get_all_records()

    # Create user lookup map
    user_map = {}
    for u in user_data:
        if u.get("Name"):
            user_map[u["Name"]] = {
                "telegram_id": str(u.get("Telegram ID") or ""),
                "email": u.get("Email", "").strip()
            }
    return ath_data, user_map, active_data, alert_log_sheet, alert_log_data

# --- Alert Log helpers ---
def buy_alert_sent_this_month(alert_log_data, user, stock, year_month):
    for record in alert_log_data:
        if (record.get("User") == user and
            record.get("Stock") == stock and
            record.get("YearMonth") == year_month and
            record.get("AlertType") == "buy"):
            return True
    return False

def log_buy_alert_month(alert_log_sheet, user, stock, year_month):
    alert_log_sheet.append_row([
        user, stock, year_month, "buy", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ])

def main_loop():
    global alert_counters
    while True:
        print(
            f"\n[DEBUG] Scan started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        try:
            ath_data, user_map, active_data, alert_log_sheet, alert_log_data = load_sheets()

            # --- BUY SIGNAL LOGIC ---
            now = datetime.now()
            year_month = now.strftime("%Y-%m")
            stock_symbols = [row["Stock"] for row in ath_data if row["Stock"]]
            prices = get_prices(stock_symbols)
            print("[DEBUG] All prices fetched.")

            for row in ath_data:
                stock = row["Stock"]
                ath = row.get("ATH_Current_Month") or row.get("ATH")
                live_price = prices.get(stock)
                if not stock or not ath or live_price is None:
                    continue
                try:
                    ath_float = float(ath)
                    live_float = float(live_price)
                except:
                    continue
                for user_name, user_info in user_map.items():
                    chat_id = user_info["telegram_id"]
                    email = user_info["email"]
                    # Block alerts if already sent max allowed for this user-stock-month
                    if buy_alert_sent_this_month(alert_log_data, user_name, stock, year_month):
                        continue
                    if live_float > ath_float:
                        alert_key = f"buy:{user_name}:{stock}:{ath_float}:{now.strftime('%Y-%m-%d')}"
                        prev_count = alert_counters.get(alert_key, 0)
                        if prev_count < MAX_ALERTS_PER_TRIGGER:
                            msg = f"📈 *BUY ALERT* for {stock}\nLive: ₹{live_float}\nATH: ₹{ath_float} (x{prev_count+1})"
                            if chat_id:
                                send_telegram_alert(chat_id, msg)
                            if email:
                                send_email_alert(email,
                                                 f"BUY ALERT for {stock}", msg)
                            alert_counters[alert_key] = prev_count + 1
                        # If this was the 5th alert for the day, log to Alert_Log so rest of month is blocked
                        if alert_counters[alert_key] == MAX_ALERTS_PER_TRIGGER:
                            log_buy_alert_month(alert_log_sheet, user_name, stock, year_month)

            print("[DEBUG] Buy Signal Monitor Cycle Done.")

            # --- SELL SIGNAL LOGIC ---
            print("[DEBUG] Running Sell Alert Logic...")
            ah_symbols = [
                row["Stock"] for row in active_data if row.get("Stock")
            ]
            ah_prices = get_prices(ah_symbols)
            for row in active_data:
                stock = row.get("Stock")
                if not stock:
                    continue
                buy_price = row.get("Buy Price")
                try:
                    buy_price = float(buy_price)
                except:
                    continue
                live_price = ah_prices.get(stock)
                if live_price is None:
                    continue
                try:
                    sma_10m = float(row.get("SMA_10M") or 0)
                    sma_20m = float(row.get("SMA_20M") or 0)
                except:
                    print(
                        f"[DEBUG] Error getting SMAs for {stock}: {row.get('SMA_10M')}, {row.get('SMA_20M')}"
                    )
                    continue
                user_name = row.get("Name")
                chat_id = user_map[user_name][
                    "telegram_id"] if user_name in user_map else ""
                email = user_map[user_name][
                    "email"] if user_name in user_map else ""
                sell_signal = False
                sell_reason = ""
                if live_price < buy_price * 2:
                    if live_price < sma_20m and sma_20m > 0:
                        sell_signal = True
                        sell_reason = f"Price below 20M SMA ({sma_20m})"
                else:
                    if live_price < sma_10m and sma_10m > 0:
                        sell_signal = True
                        sell_reason = f"Price below 10M SMA ({sma_10m})"
                if sell_signal:
                    alert_key = f"sell:{user_name}:{stock}:{sell_reason}"
                    prev_count = alert_counters.get(alert_key, 0)
                    if prev_count < MAX_ALERTS_PER_TRIGGER:
                        msg = f"🚨 *SELL ALERT* for {stock}\nLive: ₹{live_price}\nBuy Price: ₹{buy_price}\n{sell_reason} (x{prev_count+1})"
                        if chat_id:
                            send_telegram_alert(chat_id, msg)
                        if email:
                            send_email_alert(email, f"SELL ALERT for {stock}",
                                             msg)
                        print(
                            f"[DEBUG] Sent sell alert for {stock} to {user_name}"
                        )
                        alert_counters[alert_key] = prev_count + 1

            print("[DEBUG] Sell Signal Monitor Cycle Done.")

        except Exception as e:
            print(f"[DEBUG] Exception in main loop: {e}")

        print(
            f"[DEBUG] Waiting {CHECK_INTERVAL//60} minutes before next scan...\n"
        )
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main_loop()
