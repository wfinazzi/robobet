# main.py
import time
import pandas as pd
import pytz
import os
from datetime import datetime, timedelta, date, time as dt_time
from dotenv import load_dotenv

# Dependências do projeto
from src.scraper_soccerstats import get_today_games
from src.telegram_alerts import enviar_alertas, enviar_alerta_high_prob

# --- Configurações ---
TIMEZONE = 'America/Sao_Paulo'
LOAD_INTERVAL = 600  # 10 minutos
EXCEL_PATH = "data/Jogos_de_Hoje.xlsx"

load_dotenv()
token = os.getenv("TELEGRAM_TOKEN")
usuarios = [int(x) for x in os.getenv("TELEGRAM_USERS").split(",")]
tz = pytz.timezone(TIMEZONE)

# ----------------------------------------------------------------------
# Funções de Suporte (Movidas para 'main.py' para evitar Circular Imports ou falta de definição)
# ----------------------------------------------------------------------

def limpar_e_converter_dados(df):
    """Limpa '%' das colunas de porcentagem e converte para float."""
    perc_cols = [col for col in df.columns if col.startswith(('Over', 'BTTS')) and ('H' in col or 'A' in col)]
    
    for col in perc_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('%', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df.dropna(subset=perc_cols, inplace=True)
    return df

def calcular_probabilidades(df):
    """Adiciona colunas com probabilidade de Over 1.5, Over 2.5 e ambas"""
    # 1. Cálculos de Médias Simples (usadas no filtro)
    if 'Over15_H' in df.columns and 'Over15_A' in df.columns:
        df['Over15_MEDIA'] = (df['Over15_H'] + df['Over15_A']) / 2
    if 'Over25_H' in df.columns and 'Over25_A' in df.columns:
        df['Over25_MEDIA'] = (df['Over25_H'] + df['Over25_A']) / 2
    if 'Over15_MEDIA' in df.columns and 'Over25_MEDIA' in df.columns:
        df['Over_BOTH'] = (df['Over15_MEDIA'] + df['Over25_MEDIA']) / 2

    # 2. Cálculo da MÉDIA_PROB (usada no alerta High Prob e no Streamlit)
    perc_cols = ['Over15_H', 'Over25_H', 'BTTS_H', 'Over15_A', 'Over25_A', 'BTTS_A']
    colunas_para_media = [col for col in perc_cols if col in df.columns and pd.api.types.is_numeric_dtype(df[col])]
    
    if len(colunas_para_media) == 6:
        df['MÉDIA_PROB'] = df[colunas_para_media].sum(axis=1) / 6
        df['MÉDIA_PROB'] = df['MÉDIA_PROB'].round(2)
    else:
        df['MÉDIA_PROB'] = 0 
        
    return df

def filtrar_alertas(df, perc_min=60):
    """Filtra jogos que passaram do threshold mínimo de probabilidade"""
    df_filtrado = df.copy()
    
    # Certifica-se de que as colunas de média existem para o filtro
    if 'Over15_MEDIA' not in df_filtrado.columns:
        df_filtrado = calcular_probabilidades(df_filtrado.copy())
    
    df_filtrado = df_filtrado[
        (df_filtrado['Over15_MEDIA'] >= perc_min) |
        (df_filtrado['Over25_MEDIA'] >= perc_min) |
        (df_filtrado['Over_BOTH'] >= perc_min)
    ]
    return df_filtrado

def enviar_alertas_meia_hora(df):
    """Envia alertas 30 min antes do jogo"""
    agora = datetime.now(tz)

    for _, row in df.iterrows():
        horario_value = row['Horário']
        
        # Lógica robusta para lidar com string ou objeto datetime.time
        if isinstance(horario_value, str):
            try:
                # Se for string, tenta converter.
                jogo_time = datetime.strptime(horario_value, '%H:%M').replace(
                    year=agora.year, month=agora.month, day=agora.day, tzinfo=tz
                )
            except ValueError:
                 print(f"Horário inválido: {horario_value}. Pulando.")
                 continue
        elif isinstance(horario_value, (datetime, dt_time)): 
            # Se for datetime.time (comum do Pandas/Excel)
            jogo_time = datetime.combine(agora.date(), horario_value).replace(tzinfo=tz)
        else:
            print(f"Aviso: Horário inesperado '{horario_value}' para o jogo.")
            continue

        delta = jogo_time - agora
        
        if timedelta(minutes=0) <= delta <= timedelta(minutes=30):
            # Formatação da mensagem para envio (melhor ter as colunas formatadas para o alerta)
            msg_df = pd.DataFrame([row])
            
            msg = f"⚽ {row['Time 1']} x {row['Time 2']}\n" \
                  f"Over 1.5: {row.get('Over15_MEDIA',0):.0f}%\n" \
                  f"Over 2.5: {row.get('Over25_MEDIA',0):.0f}%\n" \
                  f"Ambas: {row.get('Over_BOTH',0):.0f}%"
                  
            enviar_alertas(pd.DataFrame([row]), token, usuarios)
            print(f"[{datetime.now()}] Alerta enviado: {row['Time 1']} x {row['Time 2']}")


# --- Loop principal ---
if __name__ == '__main__':
    while True:
        agora_dt = datetime.now(tz)
        agora_str = agora_dt.strftime('%Y-%m-%d %H:%M:%S')
        DATA_DE_HOJE = agora_dt.date()
        
        print("-" * 50)
        print(f"[{agora_str}] INICIANDO NOVO CICLO. Intervalo: {LOAD_INTERVAL} segundos.")
        
        df = pd.DataFrame() 

        try:
            RASPAR_NECESSARIO = False

            if os.path.exists(EXCEL_PATH):
                data_modificacao_timestamp = os.path.getmtime(EXCEL_PATH)
                # NOTA: fromtimestamp não tem fuso, então comparamos apenas a data
                data_modificacao = datetime.fromtimestamp(data_modificacao_timestamp).date()

                if data_modificacao == DATA_DE_HOJE:
                    print(f"[{agora_str}] Arquivo Excel é de hoje ({data_modificacao}). LENDO DADOS...")
                    df = pd.read_excel(EXCEL_PATH)
                else:
                    print(f"[{agora_str}] Arquivo Excel é de {data_modificacao}. RASPAR NECESSÁRIO.")
                    RASPAR_NECESSARIO = True
            else:
                print(f"[{agora_str}] Arquivo Excel não encontrado. RASPAR NECESSÁRIO.")
                RASPAR_NECESSARIO = True

            
            # --- ETAPA DE RASPAGEM (Só executa se for necessário) ---
            if RASPAR_NECESSARIO:
                print(f"[{agora_str}] Raspando dados...")
                df = get_today_games()
                
                if not os.path.exists('data'):
                    os.makedirs('data')
                    
                df.to_excel(EXCEL_PATH, index=False)
                print(f"[{agora_str}] Dados raspados e salvos em {EXCEL_PATH}.")
                

            if df.empty:
                print(f"[{agora_str}] DataFrame vazio. Pulando verificação de alertas.")
            else:
                # --- Processamento e Alertas ---
                print(f"[{agora_str}] Processando e verificando alertas em {len(df)} jogos.")

                df = limpar_e_converter_dados(df)
                df = calcular_probabilidades(df) # Garante que as médias estejam lá

                # 1. Alerta de jogos próximos (30 minutos antes)
                print(f"[{agora_str}] Verificando alertas de 30 minutos antes...")
                df_filtrado_30min = filtrar_alertas(df, perc_min=60) 
                enviar_alertas_meia_hora(df_filtrado_30min) 

                # 2. Alerta de alta probabilidade (HIGH PROB)
                print(f"[{agora_str}] Verificando alertas HIGH PROB (>70% e >10 Partidas)...")
                enviar_alerta_high_prob(df, token, usuarios) 
                
                print(f"[{agora_str}] Verificações de alerta concluídas.")
            
            print(f"[{agora_str}] CICLO CONCLUÍDO COM SUCESSO. Indo para o SLEEP.")
            
        except Exception as e:
            agora_erro = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{agora_erro}] !!! ERRO CRÍTICO NO CICLO: {e}")
            print(f"[{agora_erro}] Tentando novamente após {LOAD_INTERVAL} segundos...")

        time.sleep(LOAD_INTERVAL)