# bot_final_corrected.py

import os
import json
import time
import hmac
import base64
import logging
import asyncio
import threading
from datetime import datetime, timezone, timedelta

import requests
import websocket
from telegram import Bot
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# --- إعدادات أساسية ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- جلب الإعدادات الحساسة ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
OKX_API_KEY = os.getenv("OKX_API_KEY")
OKX_API_SECRET_KEY = os.getenv("OKX_API_SECRET_KEY")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE")
REPORT_TIME_CET = os.getenv("REPORT_TIME_CET", "21:00")

# --- تهيئة بوت التلغرام ---
telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)

# --- نظام إدارة الصفقات والتاريخ ---
DATA_DIR = os.getenv("RENDER_DISK_MOUNT_PATH", ".")
POSITIONS_FILE = os.path.join(DATA_DIR, 'positions.json')
HISTORY_FILE = os.path.join(DATA_DIR, 'trade_history.json')
positions_lock = threading.Lock()
open_positions = {}
trade_history = []

# =================================================================
# SECTION 1: الدوال المساعدة
# =================================================================

def load_data():
    global open_positions, trade_history
    try:
        with open(POSITIONS_FILE, 'r') as f: open_positions = json.load(f)
        logging.info(f"Loaded {len(open_positions)} open positions.")
    except (FileNotFoundError, json.JSONDecodeError):
        open_positions = {}; save_positions()
        logging.info(f"Created new empty {POSITIONS_FILE}.")

    try:
        with open(HISTORY_FILE, 'r') as f: trade_history = json.load(f)
        logging.info(f"Loaded {len(trade_history)} closed trades from history.")
    except (FileNotFoundError, json.JSONDecodeError):
        trade_history = []; append_to_trade_history(None)
        logging.info(f"Created new empty {HISTORY_FILE}.")

def save_positions():
    with positions_lock:
        with open(POSITIONS_FILE, 'w') as f: json.dump(open_positions, f, indent=4)

def append_to_trade_history(trade_data):
    with positions_lock:
        if trade_data: trade_history.append(trade_data)
        with open(HISTORY_FILE, 'w') as f: json.dump(trade_history, f, indent=4)
    if trade_data: logging.info(f"Appended closed trade {trade_data['asset']} to history.")

async def send_telegram_message(message_text):
    try:
        await telegram_bot.send_message(chat_id=TARGET_CHANNEL_ID, text=message_text, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Failed to send message to Telegram: {e}")

# --- دوال تنسيق الرسائل ---
def format_new_buy_message(details):
    asset, price, trade_size_percent, cash_consumed_percent, remaining_cash_percent = details.values()
    return (f"💡 توصية جديدة: بناء مركز في {asset} 🟢\n" f"━━━━━━━━━━━━━━━━━━━━\n" f"الأصل: {asset}/USDT\n" f"سعر الدخول الحالي: ${price:,.4f}\n" f"━━━━━━━━━━━━━━━━━━━━\n" f"استراتيجية إدارة المحفظة:\n" f" ▪️ حجم الدخول: تم تخصيص {trade_size_percent:.2f}% من المحفظة لهذه الصفقة.\n" f" ▪️ استهلاك السيولة: استهلك هذا الدخول {cash_consumed_percent:.2f}% من السيولة النقدية المتاحة.\n" f" ▪️ السيولة المتبقية: بعد الصفقة، أصبحت السيولة تشكل {remaining_cash_percent:.2f}% من المحفظة.\n" f"━━━━━━━━━━━━━━━━━━━━\n" f"ملاحظات:\n" f"نرى في هذه المستويات فرصة واعدة. المراقبة مستمرة، وسنوافيكم بتحديثات إدارة الصفقة.\n" f"#توصية #{asset}")
def format_add_to_position_message(details):
    asset, price, new_avg_price, added_qty = details.values()
    return (f"⚙️ تحديث التوصية: تعزيز مركز {asset} 🟢\n" f"━━━━━━━━━━━━━━━━━━━━\n" f"تمت إضافة كمية `{added_qty:.6f}` بسعر ${price:,.4f}.\n\n" f"متوسط سعر الدخول الجديد للمركز هو `${new_avg_price:,.4f}`.\n" f"نستمر في متابعة الأهداف.\n" f"#إدارة_مخاطر #{asset}")
def format_partial_sell_message(details):
    asset, price, sold_percent, pnl_percent = details.values()
    pnl_emoji = "🟢" if pnl_percent >= 0 else "🔴"
    return (f"⚙️ تحديث التوصية: إدارة مركز {asset} 🟠\n" f"━━━━━━━━━━━━━━━━━━━━\n" f"الأصل: {asset}/USDT\n" f"سعر البيع الجزئي: ${price:,.4f}\n" f"━━━━━━━━━━━━━━━━━━━━\n" f"استراتيجية إدارة المحفظة:\n" f" ▪️ الإجراء: تم بيع {sold_percent:.2f}% من مركزنا لتأمين الأرباح.\n" f" ▪️ النتيجة: ربح محقق على الجزء المباع بنسبة {pnl_percent:+.2f}% {pnl_emoji}.\n" f" ▪️ حالة المركز: لا يزال المركز مفتوحًا بالكمية المتبقية.\n" f"━━━━━━━━━━━━━━━━━━━━\n" f"ملاحظات:\n" f"خطوة استباقية لإدارة المخاطر وحماية رأس المال. نستمر في متابعة الأهداف الأعلى.\n" f"#إدارة_مخاطر #{asset}")
def format_close_trade_message(details):
    asset, avg_buy_price, avg_sell_price, roi, duration_days = details.values()
    pnl_emoji = "🟢" if roi >= 0 else "🔴"
    conclusion = ("صفقة موفقة أثبتت أن الصبر على التحليل يؤتي ثماره." if roi >= 0 else "الخروج بانضباط وفقًا للخطة هو نجاح بحد ذاته. نحافظ على رأس المال للفرصة القادمة.")
    return (f"🏆 النتيجة النهائية لتوصية {asset} {'✅' if roi >= 0 else '☑️'}\n" f"━━━━━━━━━━━━━━━━━━━━\n" f"الأصل: {asset}/USDT\n" f"الحالة: تم إغلاق الصفقة بالكامل.\n" f"━━━━━━━━━━━━━━━━━━━━\n" f"ملخص أداء التوصية:\n" f" ▪️ متوسط سعر الدخول: ${avg_buy_price:,.4f}\n" f" ▪️ متوسط سعر الخروج: ${avg_sell_price:,.4f}\n" f" ▪️ العائد النهائي على الاستثمار (ROI): {roi:+.2f}% {pnl_emoji}\n" f" ▪️ مدة التوصية: {duration_days:.1f} يوم\n" f"━━━━━━━━━━━━━━━━━━━━\n" f"الخلاصة:\n{conclusion}\n\n" f"نبارك لمن اتبع التوصية. نستعد الآن للبحث عن الفرصة التالية.\n" f"#نتائجتوصيات #{asset}")

# --- دوال جلب البيانات ---
def get_auth_headers(method, request_path, body=""):
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    body_str = json.dumps(body) if isinstance(body, dict) and body else ""
    prehash = timestamp + method.upper() + request_path + body_str
    sign = base64.b64encode(hmac.new(OKX_API_SECRET_KEY.encode(), prehash.encode(), digestmod='sha256').digest()).decode()
    return {"Content-Type": "application/json", "OK-ACCESS-KEY": OKX_API_KEY, "OK-ACCESS-SIGN": sign, "OK-ACCESS-TIMESTAMP": timestamp, "OK-ACCESS-PASSPHRASE": OKX_API_PASSPHRASE}
    
async def get_full_portfolio_details():
    url = "https://www.okx.com/api/v5/account/balance"
    headers = get_auth_headers("GET", "/api/v5/account/balance")
    try:
        async with asyncio.timeout(10):
            response = await asyncio.to_thread(requests.get, url, headers=headers)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == "0":
                total_value = float(data['data'][0]['totalEq'])
                usdt_details = next((d for d in data['data'][0]['details'] if d['ccy'] == 'USDT'), None)
                usdt_value = float(usdt_details['eq']) if usdt_details else 0
                return {'total_value': total_value, 'usdt_value': usdt_value}
    except Exception as e:
        logging.error(f"Error getting full portfolio: {e}")
    return None

async def get_market_price(asset):
    url = f"https://www.okx.com/api/v5/market/ticker?instId={asset}-USDT"
    try:
        async with asyncio.timeout(5):
            response = await asyncio.to_thread(requests.get, url)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == "0" and data.get("data"):
                return float(data["data"][0]["last"])
    except Exception as e:
        logging.error(f"Could not fetch price for {asset}: {e}")
    return None

async def generate_and_send_daily_report():
    logging.info("Generating daily copy-trading report...")
    now = datetime.utcnow()
    twenty_four_hours_ago = now - timedelta(hours=24)
    recent_trades = [trade for trade in trade_history if datetime.fromisoformat(trade['closed_at'][:-1] if trade['closed_at'].endswith('Z') else trade['closed_at']) > twenty_four_hours_ago]
    if not recent_trades:
        logging.info("No trades closed in the last 24 hours. No report sent.")
        return
    report_date = datetime.now().strftime("%d/%m/%Y"); report_message = f"📊 تقرير النسخ اليومي – خلال الـ24 ساعة الماضية\n🗓 التاريخ: {report_date}\n\n"; weighted_pnl_sum = total_weight = 0
    for trade in recent_trades:
        result_emoji = "🔼" if trade['roi'] >= 0 else "🔽"
        report_message += (f"🔸اسم العملة: {trade['asset']}\n" f"🔸 نسبة الدخول من رأس المال: {trade.get('entry_capital_percent', 0):.2f}%\n" f"🔸 متوسط سعر الشراء: {trade['avg_buy_price']:.4f}\n" f"🔸 سعر الخروج: {trade['avg_sell_price']:.4f}\n" f"🔸 نسبة الخروج من الكمية: 100.00%\n" f"🔸 النتيجة: {trade['roi']:+.2f}% {result_emoji}\n\n")
        entry_capital = trade.get('entry_capital_percent', 0)
        if entry_capital > 0: weighted_pnl_sum += trade['roi'] * entry_capital; total_weight += entry_capital
    total_avg_pnl = (weighted_pnl_sum / total_weight) if total_weight > 0 else 0; total_pnl_emoji = "📈" if total_avg_pnl >= 0 else "📉"
    report_message += (f"إجمالي الربح الحالي خدمة النسخ: {total_avg_pnl:+.2f}% {total_pnl_emoji}\n\n" "✍️ يمكنك الدخول في اي وقت تراه مناسب، الخدمة مفتوحة للجميع\n\n" "📢 قناة التحديثات الرسمية:\n@abusalamachart\n\n" "🌐 رابط النسخ المباشر:\n🏦 https://t.me/abusalamachart")
    await send_telegram_message(report_message)

async def _handle_message_async(payload):
    if payload.get("arg", {}).get("channel") == "account" and "data" in payload:
        logging.info("Received account update.")
        portfolio_state = await get_full_portfolio_details()
        if not portfolio_state: return
        current_balances = {d['ccy']: float(d['eq']) for d in payload["data"][0]["details"]}
        with positions_lock:
            all_assets = set(current_balances.keys()) | set(open_positions.keys())
            for asset in all_assets:
                if asset == 'USDT': continue
                current_qty = current_balances.get(asset, 0); position = open_positions.get(asset); known_qty = position['total_qty'] if position else 0
                difference = current_qty - known_qty; price = asyncio.run(get_market_price(asset))
                if not price or abs(difference * price) < 1.0: continue
                if difference > 0: # BUY
                    trade_value = difference * price
                    if not position:
                        entry_capital_percent = (trade_value / portfolio_state['total_value']) * 100
                        open_positions[asset] = {'id': f"{asset}-{int(time.time())}", 'total_qty': current_qty, 'avg_buy_price': price, 'total_cost': trade_value, 'open_date': datetime.utcnow().isoformat(), 'total_sold_value': 0, 'total_sold_qty': 0, 'entry_capital_percent': entry_capital_percent}
                        details = {'asset': asset, 'price': price, 'trade_size_percent': entry_capital_percent, 'cash_consumed_percent': (trade_value / (portfolio_state['usdt_value'] + trade_value)) * 100, 'remaining_cash_percent': (portfolio_state['usdt_value'] / portfolio_state['total_value']) * 100}
                        asyncio.run(send_telegram_message(format_new_buy_message(details)))
                    else:
                        new_total_cost = position['total_cost'] + trade_value; new_total_qty = position['total_qty'] + difference
                        position.update({'avg_buy_price': new_total_cost / new_total_qty, 'total_qty': new_total_qty, 'total_cost': new_total_cost})
                        details = {'asset': asset, 'price': price, 'new_avg_price': position['avg_buy_price'], 'added_qty': difference}
                        asyncio.run(send_telegram_message(format_add_to_position_message(details)))
                    save_positions()
                elif difference < 0: # SELL
                    if not position: continue
                    sold_qty = abs(difference); sold_value = sold_qty * price
                    position.update({'total_sold_value': position['total_sold_value'] + sold_value, 'total_sold_qty': position['total_sold_qty'] + sold_qty})
                    if current_qty < 0.000001:
                        duration = datetime.utcnow() - datetime.fromisoformat(position['open_date']); avg_sell_price = position['total_sold_value'] / position['total_sold_qty'] if position['total_sold_qty'] > 0 else price; roi = ((avg_sell_price - position['avg_buy_price']) / position['avg_buy_price']) * 100
                        details = {'asset': asset, 'avg_buy_price': position['avg_buy_price'], 'avg_sell_price': avg_sell_price, 'roi': roi, 'duration_days': duration.total_seconds() / 86400}
                        asyncio.run(send_telegram_message(format_close_trade_message(details)))
                        history_data = {**details, 'closed_at': datetime.utcnow().isoformat(), 'entry_capital_percent': position.get('entry_capital_percent', 0)}
                        append_to_trade_history(history_data); del open_positions[asset]
                    else:
                        position['total_qty'] = current_qty; pnl_percent = ((price - position['avg_buy_price']) / position['avg_buy_price']) * 100
                        details = {'asset': asset, 'price': price, 'sold_percent': (sold_qty / known_qty) * 100, 'pnl_percent': pnl_percent}
                        asyncio.run(send_telegram_message(format_partial_sell_message(details)))
                    save_positions()

# =================================================================
# SECTION 2: OKX WebSocket Client
# =================================================================
class OKXWebSocketClient:
    def __init__(self, url):
        self.ws_url = url; self.ws_app = None; self.thread = None

    def _generate_signature(self, timestamp):
        # --- التصحيح النهائي بناءً على كودك الاحترافي ---
        message = timestamp + 'GET' + '/users/self/verify'
        mac = hmac.new(bytes(OKX_API_SECRET_KEY, 'utf-8'), bytes(message, 'utf-8'), digestmod='sha256')
        return base64.b64encode(mac.digest()).decode()

    def _on_open(self, ws):
        logging.info("WebSocket connection opened. Sending login payload...")
        # --- استخدام Unix Timestamp الرقمي ---
        current_timestamp = str(time.time())
        login_payload = {
            "op": "login",
            "args": [{
                "apiKey": OKX_API_KEY,
                "passphrase": OKX_API_PASSPHRASE,
                "timestamp": current_timestamp, 
                "sign": self._generate_signature(current_timestamp),
            }]
        }
        ws.send(json.dumps(login_payload))
        threading.Thread(target=self._keep_alive, daemon=True).start()

    def _keep_alive(self):
        while getattr(self.ws_app.sock, 'connected', False):
            try:
                self.ws_app.send("ping"); time.sleep(25)
            except websocket.WebSocketConnectionClosedException: break

    def _on_message(self, ws, message):
        if message == 'pong': return
        payload = json.loads(message)
        if payload.get("event") == "login":
            if payload.get("success") or payload.get("code") == "0":
                logging.info("WebSocket login successful.")
                ws.send(json.dumps({"op": "subscribe", "args": [{"channel": "account"}]}))
            else:
                logging.error(f"WebSocket login failed: {payload.get('msg')}")
        elif payload.get("arg", {}).get("channel") == "account":
            asyncio.run(_handle_message_async(payload))

    def _on_error(self, ws, error): logging.error(f"WebSocket Error: {error}")
    def _on_close(self, ws, close_status_code, close_msg): logging.warning("WebSocket closed. Reconnecting..."); time.sleep(5); self.connect()
    
    def connect(self):
        self.ws_app = websocket.WebSocketApp(self.ws_url, on_open=self._on_open, on_message=self._on_message, on_error=self._on_error, on_close=self._on_close)
        self.thread = threading.Thread(target=self.ws_app.run_forever, daemon=True); self.thread.start()

# =================================================================
# SECTION 3: Main Execution
# =================================================================

async def main():
    if not all([TELEGRAM_BOT_TOKEN, TARGET_CHANNEL_ID, OKX_API_KEY, OKX_API_SECRET_KEY, OKX_API_PASSPHRASE]):
        logging.critical("FATAL: One or more environment variables are missing. Exiting."); exit(1)
    logging.info("Starting Final Spy Bot...")
    load_data()
    scheduler = AsyncIOScheduler(timezone="CET")
    hour, minute = map(int, REPORT_TIME_CET.split(':'))
    scheduler.add_job(generate_and_send_daily_report, 'cron', hour=hour, minute=minute)
    scheduler.start()
    logging.info(f"Daily report scheduled for {REPORT_TIME_CET} CET.")
    ws_client = OKXWebSocketClient("wss://ws.okx.com:8443/ws/v5/private")
    ws_client.connect()
    logging.info("Bot is running and monitoring trades...")
    try:
        while True: await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown(); logging.info("Bot stopped by user.")

if __name__ == "__main__":
    asyncio.run(main())

