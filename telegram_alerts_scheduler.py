import pandas as pd
from datetime import datetime, timedelta
from src.telegram_alerts import enviar_alertas
import os
from dotenv import load_dotenv
import time

load_dotenv()
token = os.getenv("TELEGRAM_TOKEN")
usuarios = [int(x) for x in os.getenv("TELEGRAM_USERS").split(",")]

EXCEL_PATH = "data/Jogos_de_Hoje.xlsx"

while True:
    try:
        df = pd.read_excel(EXCEL_PATH)

        agora = datetime.now()
        for idx, row in df.iterrows():
            jogo_time = datetime.strptime(row['Horário'], '%H:%M')
            jogo_time = jogo_time.replace(
                year=agora.year, month=agora.month, day=agora.day
            )

            if timedelta(minutes=0) <= jogo_time - agora <= timedelta(minutes=30):
                enviar_alertas(pd.DataFrame([row]), token, usuarios)

    except Exception as e:
        print("Erro ao enviar alertas:", e)

    time.sleep(60)  # verifica a cada minuto
