import os
import sys
import pytz
import asyncio
import logging
import pandas as pd
from datetime import datetime, time
from typing import Final
from telegram import Bot, Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackContext,
    JobQueue,
)

log_file = "logging.log"
logging.basicConfig(
    filename=log_file,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    encoding='utf-8'
)

taiwan_tz = pytz.timezone('Asia/Taipei')

# Telegram bot token and username
TOKEN: Final = "8025594413:AAHxereThEZ8PrlbtdUT1XxZ3RKtJoOwEfY"
BOT_USERNAME: Final = "@TW_market_info_bot"

# Telegram user ID to send messages (your user ID)
MY_USER_ID: Final = 1942679873

# 定義CSV檔案的絕對路徑 (修改為你的資料夾路徑)
BASE_DIR = "E:\\Finance_Data\\TX_daily"  # 資料夾絕對路徑
FUND_DATA_FILE_PATH = os.path.join(BASE_DIR, "tx_fund_data.csv")
TWSE_DATA_FILE_PATH = os.path.join(BASE_DIR, "tx_closed_data.csv")
TXF_DATA_FILE_PATH = os.path.join(BASE_DIR, "txf_data.csv")
TXOP_DATA_FILE_PATH = os.path.join(BASE_DIR, "txop_data.csv")

# Function to load the latest data from CSV files and compile a report message in Chinese
def compile_latest_report() -> str:
    report = ""
    try:
        # 記錄開始處理 TWSE 資料
        logging.info("開始處理 TWSE 資料...")
        # Load and extract data from TWSE closed data
        if os.path.exists(TWSE_DATA_FILE_PATH):
            twse_df = pd.read_csv(TWSE_DATA_FILE_PATH)
            latest_twse_data = twse_df.iloc[-1]

            # 將成交金額轉換為億元（假設原始數據為 "元"）
            # trading_volume_yi = round(latest_twse_data['成交金額'] / 10**8, 2)

            report += f"日期: {latest_twse_data['日期']}\n"
            report += f"加權指數: {latest_twse_data['加權指數']}\n"
            report += f"漲跌點數: {latest_twse_data['漲跌點數']}\n"
            formatted_trading_volume = latest_twse_data['成交金額'].replace(',', '')[:4]
            report += f"成交金額: {formatted_trading_volume} 億元\n\n"
            logging.info(f"TWSE 資料處理完成：{latest_twse_data['日期']}, 成交金額 {latest_twse_data['成交金額']} ")
        else:
            logging.warning("無法找到 TWSE 資料檔案，請檢查檔案路徑。")

        # 記錄開始處理三大法人買賣超資料
        logging.info("開始處理三大法人買賣超資料...")
        # Load and extract data from Fund data
        if os.path.exists(FUND_DATA_FILE_PATH):
            try:
                # 讀取三大法人資料，並設定標題行和編碼格式（根據實際檔案情況進行調整）
                fund_df = pd.read_csv(FUND_DATA_FILE_PATH, header=0, encoding='utf-8-sig')
                # 在日誌中記錄讀取到的欄位名稱
                logging.info(f"三大法人資料欄位名稱: {list(fund_df.columns)}")

                # 確認是否存在 '單位名稱' 欄位
                if '單位名稱' not in fund_df.columns:
                    raise ValueError(f"CSV 中無法找到 '單位名稱' 欄位，讀取到的欄位名稱有: {list(fund_df.columns)}")

                # 將相關欄位轉換為數值類型
                fund_df[['買進金額', '賣出金額', '買賣差額']] = fund_df[['買進金額', '賣出金額', '買賣差額']].apply(
                    lambda col: pd.to_numeric(col.str.replace(',', ''), errors='coerce')
                )

                # 取得最新的日期
                latest_date = fund_df['日期'].max()
                # 取該日期的最後六行
                last_six_rows = fund_df[fund_df['日期'] == latest_date].tail(6)

                # 建立報告
                report += f"日期: {latest_date}\n"
                # 遍歷每一行數據，將每個單位名稱的買賣差額分別列出
                for _, row in last_six_rows.iterrows():
                    # 將金額轉換為「億元」並格式化顯示
                    net_amount_yi = round(row['買賣差額'] / 10**8, 2)  # 轉換為億元顯示，保留兩位小數
                    # 根據金額正負判斷賣超或買超
                    if net_amount_yi < 0:
                        report += f"{row['單位名稱']}: {net_amount_yi} 億元 (賣超)\n"
                    else:
                        report += f"{row['單位名稱']}: {net_amount_yi} 億元 (買超)\n"

                # 顯示總合計（不進行外資合併計算）
                total_net_amount = last_six_rows[last_six_rows['單位名稱'] == '合計']['買賣差額'].values[0]
                total_net_amount_yi = round(total_net_amount / 10**8, 2)  # 轉換為億元顯示，保留兩位小數
                # 判斷總合計是賣超還是買超
                if total_net_amount_yi < 0:
                    report += f"總合計: {total_net_amount_yi} 億元 (賣超)\n\n"
                else:
                    report += f"總合計: {total_net_amount_yi} 億元 (買超)\n\n"
                logging.info(f"三大法人買賣超資料處理完成：{latest_date}, 總合計 {total_net_amount:,} 元")
            except Exception as e:
                logging.error(f"處理三大法人資料時發生錯誤: {e}")
        else:
            logging.warning("無法找到三大法人買賣超資料檔案，請檢查檔案路徑。")



        # 記錄開始處理期貨資料
        logging.info("開始處理期貨資料...")
        # Load and extract data from Futures data (using the latest date)
        if os.path.exists(TXF_DATA_FILE_PATH):
            txf_df = pd.read_csv(TXF_DATA_FILE_PATH)
            latest_date = txf_df['日期'].max()  # 找到最新的日期
            latest_txf_data = txf_df[txf_df['日期'] == latest_date]  # 針對最新日期進行篩選
            report += f"期貨多空未平倉口數淨額 (日期: {latest_date})\n"
            for entity in ['投信', '自營商', '外資']:
                entity_data = latest_txf_data[(latest_txf_data['商品名稱'] == '臺股期貨') & (latest_txf_data['身份別'] == entity)]
                if not entity_data.empty:
                    entity_net_amount = entity_data['多空未平倉口數淨額'].values[0]
                    entity_trade_amount = entity_data['多空交易口數淨額'].values[0]
                    if not entity_trade_amount.startswith('-'):
                        entity_trade_amount = f"+{entity_trade_amount}"
                    # entity_trade_amount = 0
                    report += f"{entity}: {entity_net_amount} ({entity_trade_amount})口\n"
            report += "\n"
            logging.info(f"期貨資料處理完成：{latest_date}")
        else:
            logging.warning("無法找到期貨資料檔案，請檢查檔案路徑。")

        # 記錄開始處理選擇權資料
        logging.info("開始處理選擇權資料...")
        # Load and extract data from Options data (using the latest date)
        if os.path.exists(TXOP_DATA_FILE_PATH):
            txop_df = pd.read_csv(TXOP_DATA_FILE_PATH)
            latest_date = txop_df['日期'].max()  # 找到最新的日期
            latest_txop_data = txop_df[txop_df['日期'] == latest_date]  # 針對最新日期進行篩選
            report += f"選擇權多空未平倉口數淨額 (日期: {latest_date})\n"
            for entity in ['投信', '自營商', '外資']:
                entity_data = latest_txop_data[(latest_txop_data['商品名稱'] == '選擇權') & (latest_txop_data['身份別'] == entity)]
                if not entity_data.empty:
                    entity_net_amount = entity_data['多空未平倉口數淨額'].values[0]
                    entity_trade_amount = entity_data['多空交易口數淨額'].values[0]
                    if not entity_trade_amount.startswith('-'):
                        entity_trade_amount = f"+{entity_trade_amount}"
                    report += f"{entity}: {entity_net_amount} ({entity_trade_amount})口\n"
            report += "\n"
            logging.info(f"選擇權資料處理完成：{latest_date}")
        else:
            logging.warning("無法找到選擇權資料檔案，請檢查檔案路徑。")

    except Exception as e:
        logging.error(f"讀取資料時發生錯誤: {e}")
        report += f"讀取資料時發生錯誤: {e}\n"
    
    # 最後返回報告內容
    logging.info("報告生成完成。")
    return report if report else "無可用的最新數據。"

# Function to send the latest report to the specified user
async def send_daily_report(context: CallbackContext) -> None:
    report = compile_latest_report()
    if not report.strip():  # 檢查報告是否為空
        report = "目前無可用的最新數據，請稍後再試。"

    # 從 subscribers.txt 中讀取所有的 chat_id
    try:
        with open("subscribers.txt", "r") as f:
            subscribers = f.readlines()

        # 向每個 chat_id 發送報告
        for chat_id in subscribers:
            chat_id = chat_id.strip()  # 移除換行符號或空格
            if chat_id:  # 確認 chat_id 有效
                await context.bot.send_message(chat_id=chat_id, text=report)
                logging.info(f"向 chat_id={chat_id} 發送報告成功")

    except Exception as e:
        logging.error(f"發送每日報告時發生錯誤: {e}")

async def send_daily_report_and_exit(context: CallbackContext) -> None:
    try:
        # 執行報告邏輯
        await send_daily_report(context)
    finally:
        # 優雅地停止應用程式和事件回圈
        logging.info("報告發送完成，準備結束程式...")
        
        # 停止 Telegram 應用程式
        context.application.stop()
        
        # 停止事件回圈
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.stop()


# Command to trigger the manual report
async def manual_report_command(update: Update, context: CallbackContext) -> None:
    report = compile_latest_report()
    if not report.strip():  # 檢查報告是否為空
        report = "目前無可用的最新數據，請稍後再試。"
    await update.message.reply_text(report)

# Default start command
async def start_command(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    with open("subscribers.txt", "a") as f:
        f.write(f"{chat_id}\n")  # 將 chat_id 儲存到 subscribers.txt
    await update.message.reply_text("你好，我是一個市場資訊機器人，可以每日提供市場更新資訊。")


# Default help command with detailed functionality description
async def help_command(update: Update, context: CallbackContext) -> None:
    help_text = (
        "💡 **可用的命令與功能介紹**：\n\n"
        "/start - 啟動機器人，顯示歡迎訊息。\n"
        "/help - 列出所有可用的命令與其說明。\n"
        "/report - 手動查詢最新的市場報告。\n\n"
        "📊 **機器人的功能介紹**：\n"
        "1️⃣ **每日市場報告**：\n"
        "    - 每天下午 4 點（16:00）自動發送市場報告，包括台灣加權指數、成交金額、三大法人買賣超，以及期貨與選擇權的多空未平倉口數淨額。\n"
        "    - 可手動使用 `/report` 指令隨時查詢最新的市場報告。\n\n"
        "2️⃣ **報告內容包含**：\n"
        "    - **台灣加權指數與漲跌點數**：顯示最新日期的加權指數、漲跌點數，以及成交金額（以億元為單位顯示）。\n"
        "    - **三大法人買賣超**：包含自營商(自行買賣)、自營商(避險)、投信、外資及陸資、外資自營商的買賣差額合併統計，並計算外資總合計。\n"
        "    - **期貨與選擇權的多空未平倉口數淨額**：顯示投信、自營商、外資的多空未平倉口數淨額，針對最新日期進行統計。\n\n"
        "若有任何疑問或建議，請隨時聯絡開發者！"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")

# Default error handler
async def error_handler(update: Update, context: CallbackContext) -> None:
    print(f'Update {update} caused error {context.error}')

# Main function to set up the bot and schedule the daily job
if __name__ == '__main__':
    logging.info("Telegram Bot Starting...")
    # print("Starting bot...")
    app = Application.builder().token(TOKEN).build()  

    # Add command handlers
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("report", manual_report_command))  # Manual report command

    # Set up the daily job at 16:00 (4 PM)
    job_queue: JobQueue = app.job_queue
    # job_queue.run_daily(send_daily_report, time(hour=16, minute=0, second=0, tzinfo=taiwan_tz))
    # logging.info("已設置每日 16:00 發送日報告")

    job_queue.run_once(send_daily_report_and_exit, 10)  # 立即執行 send_daily_report 函數

    # Error handling
    app.add_error_handler(error_handler)

    # Polling
    logging.info("Polling...")
    # print("Polling...")
    app.run_polling(poll_interval=5)
    logging.info("Telegram Bot Stopped.")
