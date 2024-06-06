#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import numpy as np
import pandas as pd
from deribit_exchange_ro import DeribitExchange
from utils import instrument_dict
from utils import MyException

MAIL = '../keys/mail.sec'
KEY_FILE = '../keys/deribit.sec'
PARAMETERS = "parameters.json"

def send_mail(mail_parameters, date_now, companies, attachments):
   
    msg = MIMEMultipart() 
    msg['From'] = mail_parameters["Sender"]
    msg['To'] = "".join([r + "; " for r in mail_parameters["Recipients"]])
    msg['Subject'] = "Deribit data " + str(date_now) + " (UTC)"

    body = "Deribit data " + str(date_now) + " (UTC) " + "".join([c + ", " for c in companies])
    msg.attach(MIMEText(body, 'plain'))

    for a in attachments:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachments[a])
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename=" + a + ".csv;")
        msg.attach(part)    

    message = msg.as_string()
    smtp = smtplib.SMTP(mail_parameters["SMTP"], mail_parameters["Port"])
    smtp.starttls()
    smtp.login(mail_parameters["Sender"], mail_parameters["Password"]) # less secure aps should be ON
    smtp.sendmail(mail_parameters["Sender"], mail_parameters["Recipients"], message)
    smtp.quit()
    
def add_not_existent(columns, df):
    for column in columns:
        if not column in df:
            df[column] = np.nan

def main():
    with open(PARAMETERS, 'r') as f:
        params = json.load(f)
    with open(KEY_FILE, 'r') as f:
        exchanges = json.load(f)
    with open(MAIL, 'r') as f:
        mail_params = json.load(f)

    date_now = datetime.utcnow().replace(microsecond=0)
    start_timestamp = int((date_now.replace(hour=0, minute=0, second=0) + timedelta(days=params["DAYS"][0])).timestamp() * 1000)
    end_timestamp = int((date_now.replace(hour=0, minute=0, second=0) + timedelta(days=params["DAYS"][1])).timestamp() * 1000)
    attachments = {}
    companies = list(exchanges.keys())
    list_df_wallets = []
    list_df_positions = []
    list_df_logs = []
    for company in companies:
        deribit_client = exchanges[company]["ID"]
        deribit_key = exchanges[company]["secret"]
        exchange = DeribitExchange(deribit_client, deribit_key, params["MARKETS"])
        res = exchange.authenticate()
        if res:
            raise MyException("Deribit authentication error" + str(res))
    
        _wallet_data = exchange.get_wallet()
        wallet_data = {}
        for w in _wallet_data:
            wallet_data[(company, w)] = _wallet_data[w]
            wallet_data[(company, w)]["username"] = company

        logs = {}
        for c in _wallet_data.keys():
            _log = exchange.get_transaction_log(c, start_timestamp, end_timestamp, 100000)
            for row in _log:
                row["username"] = company
                row["date"] = datetime.fromtimestamp(row["timestamp"]/1000).date()
                logs[(company, c, datetime.fromtimestamp(row["timestamp"]/1000))] = row
        
        _positions = instrument_dict(exchange.get_positions())
        positions = {}
        for p in _positions:
            positions[(company, p)] = _positions[p]
            positions[(company, p)]["username"] = company
    
        # options_summary_bulk = exchange.get_options_summary()
        # currency = exchange.get_currency()
        # trades = {}
        # for c, i in positions:
        #     _trades = exchange.get_trades(i, 100)
        #     for t in _trades:
        #         trades[(c, i, t["trade_id"])] = t
    
        list_df_wallets.append(pd.DataFrame.from_dict(wallet_data).T)
        list_df_positions.append(pd.DataFrame.from_dict(positions).T)
        list_df_logs.append(pd.DataFrame.from_dict(logs).T)
        # df_currency = pd.DataFrame(currency.values(), currency.keys(), columns=["spot"])
        # df_trades = pd.DataFrame.from_dict(trades).T

        # attachments[company + "_wallet_data_" + str(date_now)] = df_wallet_data.to_csv()
    df_wallets = pd.concat(list_df_wallets)
    df_positions = pd.concat(list_df_positions)
    df_logs = pd.concat(list_df_logs)

    wallets_columns = ["username", "currency", "available_withdrawal_funds", "total_pl", "options_pl", "margin_balance", "available_funds", "projected_maintenance_margin", "equity", "maintenance_margin", "projected_initial_margin", "maintenance_margin", "futures_pl"]
    balances_columns = ["username", "instrument_name", "size", "mark_price", "average_price_usd", "average_price", "total_profit_loss", "realized_profit_loss", "floating_profit_loss", "delta"]
    logs_columns = ["username", "currency", "date", "instrument_name", "side", "amount", "position", "price", "mark_price", "index_price", "cashflow", "commission", "change"]
    str_date = (date_now.replace(hour=0, minute=0, second=0) + timedelta(days=params["DAYS"][0])).strftime("%Y-%m-%d")
    str_date_time = date_now.strftime("%Y-%m-%d_%H-%M-%S")

    add_not_existent(wallets_columns, df_wallets)
    add_not_existent(balances_columns, df_positions)
    add_not_existent(logs_columns, df_logs)

    attachments["wallets" + str_date_time] = df_wallets.to_csv(columns=wallets_columns, index=False) if len(df_wallets) > 0 else ""
    attachments["balances" + str_date_time] = df_positions.to_csv(columns=balances_columns, index=False) if len(df_positions) > 0 else ""
    # attachments[company + "_currency_" + str(date_now)] = df_currency.to_csv()
    attachments["transaction_logs_" + str_date] = df_logs.to_csv(columns=logs_columns, index=False) if len(df_logs) > 0 else ""

    send_mail(mail_params, date_now, companies, attachments)

if __name__ == '__main__':
    main()
