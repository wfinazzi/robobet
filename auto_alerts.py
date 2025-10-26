import time
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv
from src.scraper_soccerstats import get_today_games
from src.telegram_alerts import enviar_alertas

# --- Config ---
TIMEZONE = 'America/Sao_Paulo'
LOAD_INTERVAL = 60  # segundos
load_dotenv()

token = os.getenv("TELEGRAM_TOKEN")
usuarios = [int(x) for x in os.getenv("TELEGRAM_USERS").split(",")]

def calcular_probabilidades(df):
    """Adiciona colunas com probabilidade de Over 1.5, Over 2.5 e ambas"""
    if 'Over15_H' in df.columns and 'Over15_A' in df.columns:
        df['Over15_MEDIA'] = (df['Over15_H'] + df['Over15_A']) / 2
    if 'Over25_H' in df.columns and 'Over25_A' in df.columns:
        df['Over25_MEDIA'] = (df['Over25_H'] + df['Over25_A']) / 2
    if 'Over15_MEDIA' in df.columns and 'Over25_MEDIA' in df.columns:
        df['Over_BOTH'] = (df['Over15_MEDIA'] + df['Over25_MEDIA']) / 2
    return df

def filtrar_alertas(df, perc_min=60):
    """Filtra jogos que passaram do threshold mínimo de probabilidade"""
    df_filtrado = df.copy()
    # Mantemos apenas jogos com Over 1.5 ou Over 2.5 > perc_min
    df_filtrado = df_filtrado[
        (df_filtrado['Over15_MEDIA'] >= perc_min) |
        (df_filtrado['Over25_MEDIA'] >= perc_min) |
        (df_filtrado['Over_BOTH'] >= perc_min)
    ]
    return df_filtrado

def enviar_alertas_meia_hora(df):
    """Envia alertas 30 min antes do jogo"""
    tz = pytz.timezone(TIMEZONE)
    agora = datetime.now(tz)

    for _, row in df.iterrows():
        jogo_time = datetime.strptime(row['Horário'], '%H:%M').replace(
            year=agora.year, month=agora.month, day=agora.day, tzinfo=tz
        )
        delta = jogo_time - agora
        if timedelta(minutes=0) <= delta <= timedelta(minutes=30):
            # envia alerta
            msg = f"⚽ {row['Time 1']} x {row['Time 2']}\n" \
                  f"Over 1.5: {row.get('Over15_MEDIA',0):.0f}%\n" \
                  f"Over 2.5: {row.get('Over25_MEDIA',0):.0f}%\n" \
                  f"Ambas: {row.get('Over_BOTH',0):.0f}%"
            enviar_alertas(pd.DataFrame([row]), token, usuarios)
            print(f"[{datetime.now()}] Alerta enviado: {row['Time 1']} x {row['Time 2']}")

# --- Loop principal ---
while True:
    try:
        print(f"[{datetime.now()}] Raspando dados...")
        df = get_today_games()
        df = calcular_probabilidades(df)
        df_filtrado = filtrar_alertas(df, perc_min=60)
        enviar_alertas_meia_hora(df_filtrado)
    except Exception as e:
        print(f"Erro: {e}")
    time.sleep(LOAD_INTERVAL)
