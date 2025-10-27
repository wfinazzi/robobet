import time
from datetime import datetime, timedelta
import pandas as pd
import pytz
import os
from dotenv import load_dotenv
from src.scraper_soccerstats import get_today_games
from src.telegram_alerts import enviar_alertas

# --- Config ---
TIMEZONE = 'America/Sao_Paulo'
LOAD_INTERVAL = 600 
load_dotenv()
token = os.getenv("TELEGRAM_TOKEN")
usuarios = [int(x) for x in os.getenv("TELEGRAM_USERS").split(",")]

# ----------------------------------------------------------------------
# NOVO: Função de Limpeza e Conversão de Tipos
# ----------------------------------------------------------------------
def limpar_e_converter_dados(df):
    """Limpa '%' das colunas de porcentagem e converte para float."""
    
    # Identifica todas as colunas de porcentagem
    perc_cols = [col for col in df.columns if col.startswith(('Over', 'BTTS')) and ('H' in col or 'A' in col)]
    
    for col in perc_cols:
        if col in df.columns:
            # 1. Remove o '%' se existir
            df[col] = df[col].astype(str).str.replace('%', '', regex=False)
            
            # 2. Converte para float (errors='coerce' transforma falhas em NaN)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    # Remove linhas que não puderam ser convertidas (opcional, mas recomendado)
    df.dropna(subset=perc_cols, inplace=True)
    
    return df
# ----------------------------------------------------------------------

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
        horario_value = row['Horário']
        
        # --- LÓGICA CORRIGIDA ---
        if isinstance(horario_value, str):
            # Se for string (o caso original esperado), use strptime
            jogo_time = datetime.strptime(horario_value, '%H:%M').replace(
                year=agora.year, month=agora.month, day=agora.day, tzinfo=tz
            )
        elif isinstance(horario_value, (datetime, time)): # Se já for datetime.time ou datetime
            # Use datetime.combine para juntar o objeto de tempo com a data de hoje e o fuso
            jogo_time = datetime.combine(agora.date(), horario_value).replace(tzinfo=tz)
        else:
            # Caso não seja um tipo esperado, pule ou registre um erro
            print(f"[{datetime.now()}] Aviso: Horário inesperado '{horario_value}' para o jogo.")
            continue
        # --- FIM LÓGICA CORRIGIDA ---

        delta = jogo_time - agora

        if timedelta(minutes=0) <= delta <= timedelta(minutes=30):
            # envia alerta
            msg = f"⚽ {row['Time 1']} x {row['Time 2']}\n" \
                  f"Over 1.5: {row.get('Over15_MEDIA',0):.0f}%\n" \
                  f"Over 2.5: {row.get('Over25_MEDIA',0):.0f}%\n" \
                  f"Ambas: {row.get('Over_BOTH',0):.0f}%"
                  
            # Nota: Você está passando uma linha (Série Pandas) para enviar_alertas,
            # mas ela espera um DF. Seu código já resolve isso com pd.DataFrame([row]).
            enviar_alertas(pd.DataFrame([row]), token, usuarios)

            print(f"[{datetime.now()}] Alerta enviado: {row['Time 1']} x {row['Time 2']}")

def calcular_probabilidades(df):
    """Adiciona colunas com probabilidade de Over 1.5, Over 2.5 e ambas"""
    # ... (Sua função calcular_probabilidades permanece a mesma)
    if 'Over15_H' in df.columns and 'Over15_A' in df.columns:
        df['Over15_MEDIA'] = (df['Over15_H'] + df['Over15_A']) / 2
    if 'Over25_H' in df.columns and 'Over25_A' in df.columns:
        df['Over25_MEDIA'] = (df['Over25_H'] + df['Over25_A']) / 2
    if 'Over15_MEDIA' in df.columns and 'Over25_MEDIA' in df.columns:
        df['Over_BOTH'] = (df['Over15_MEDIA'] + df['Over25_MEDIA']) / 2
    return df
# ... (Resto das suas funções permanece o mesmo)


# --- Loop principal CORRIGIDO ---
# while True:
#     try:
#         print(f"[{datetime.now()}] Raspando dados...")
#         df = get_today_games()
        
#         # === ETAPA CRÍTICA: LIMPAR E CONVERTER DADOS ANTES DO CÁLCULO ===
#         df = limpar_e_converter_dados(df)
        
#         df = calcular_probabilidades(df)
#         df_filtrado = filtrar_alertas(df, perc_min=60)
#         enviar_alertas_meia_hora(df_filtrado)
#     except Exception as e:
#         print(f"Erro: {e}")
#     time.sleep(LOAD_INTERVAL)