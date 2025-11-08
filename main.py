# main.py
import time
import pandas as pd
import pytz
import os
from datetime import datetime, timedelta, time as dt_time
from dotenv import load_dotenv

# Dependências do projeto
# NOTA: Removido 'get_today_games' pois não raspamos mais aqui.
# A função 'enviar_alertes_unicos' deve ser usada no lugar de 'enviar_alertas' e 'enviar_alerta_high_prob'
# para evitar duplicidade. Vamos criar stubs/adaptações para manter a estrutura.
from src.telegram_alerts import enviar_alertes_unicos

# --- Configurações ---
TIMEZONE = 'America/Sao_Paulo'
LOAD_INTERVAL = 600  # 10 minutos
EXCEL_PATH = "data/Jogos_de_Hoje.xlsx"

load_dotenv()
token = os.getenv("TELEGRAM_TOKEN")
usuarios = [int(x) for x in os.getenv("TELEGRAM_USERS").split(",")]
tz = pytz.timezone(TIMEZONE)

# ----------------------------------------------------------------------
# Funções de Suporte (Manter para processar o DF lido)
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
    # Garante que as colunas existam antes de calcular a média das médias
    if 'Over15_MEDIA' in df.columns and 'Over25_MEDIA' in df.columns:
        df['Over_BOTH'] = (df['Over15_MEDIA'] + df['Over25_MEDIA']) / 2
    else:
        df['Over_BOTH'] = 0 # Adiciona a coluna para evitar erro no filtro

    # 2. Cálculo da MÉDIA_PROB (usada no alerta High Prob)
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
    
    # Aplica o filtro de probabilidades (se a coluna existir)
    cols_to_check = ['Over15_MEDIA', 'Over25_MEDIA', 'Over_BOTH']
    condition = False
    
    for col in cols_to_check:
        if col in df_filtrado.columns:
            # Garante que a coluna tem valores numéricos válidos antes da comparação
            valid_col = pd.to_numeric(df_filtrado[col], errors='coerce').fillna(0)
            condition = (valid_col >= perc_min) | condition
    
    if isinstance(condition, bool): # Se todas as colunas estavam faltando
         return pd.DataFrame()
         
    return df_filtrado[condition]

# Arquivo: main.py (função enviar_alertas_meia_hora)
def enviar_alertas_meia_hora(df):
    """Prepara o DF para envio até 2 horas antes do jogo com correção de fuso."""
    HORARIO_OFFSET_HORAS = -3  # corrige +3h vindas do DF
    ALERT_WINDOW_MINUTES = 120  # janela de busca: até 2 horas
    agora = datetime.now(tz)
    df_alertar = []

    for _, row in df.iterrows():
        horario_value = row.get('Horário')

        try:
            # Parse base (horário do DF, assumido como HH:MM)
            if isinstance(horario_value, str):
                jogo_time = datetime.strptime(horario_value.strip(), '%H:%M').replace(
                    year=agora.year, month=agora.month, day=agora.day, tzinfo=tz
                )
            elif isinstance(horario_value, (datetime, dt_time)):
                base_time = horario_value if isinstance(horario_value, datetime) else datetime.combine(agora.date(), horario_value)
                jogo_time = base_time.replace(tzinfo=tz)
            else:
                continue

            # Corrigir a defasagem de +3h ao enviar
            jogo_time_corrigido = jogo_time + timedelta(hours=HORARIO_OFFSET_HORAS)
            delta = jogo_time_corrigido - agora

            # Não enviar se já passou
            if delta < timedelta(minutes=0):
                continue

            # Enviar entre 0 e 120 minutos antes
            if timedelta(minutes=0) <= delta <= timedelta(minutes=ALERT_WINDOW_MINUTES):
                row = row.copy()
                row['Horário'] = jogo_time_corrigido.strftime('%H:%M')  # exibição corrigida
                row['Tipo_Alerta'] = "ALERTA_120MIN"
                df_alertar.append(row)

        except Exception:
            continue

    return pd.DataFrame(df_alertar)

def filtrar_alertas_over15_e_partidas(df):
    """Filtra jogos com Over 1.5 >= 70% e Partidas > 7."""
    df2 = df.copy()
    # Garante cálculos
    df2 = calcular_probabilidades(df2)
    # Usa Prob_Over1.5 (se existir) senão cai para Over15_MEDIA
    if 'Prob_Over1.5' in df2.columns:
        over15_col = pd.to_numeric(df2['Prob_Over1.5'], errors='coerce').fillna(0)
    else:
        over15_col = pd.to_numeric(df2.get('Over15_MEDIA', 0), errors='coerce').fillna(0)
    partidas_col = pd.to_numeric(df2.get('Partidas', 0), errors='coerce').fillna(0)
    return df2[(over15_col >= 70) & (partidas_col > 7)]

# ----------------------------------------------------------------------
# --- Loop principal (SIMPLIFICADO) ---
# ----------------------------------------------------------------------
if __name__ == '__main__':
    while True:
        agora_dt = datetime.now(tz)
        agora_str = agora_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print("-" * 50)
        print(f"[{agora_str}] INICIANDO NOVO CICLO (APENAS ALERTA). Intervalo: {LOAD_INTERVAL} segundos.")
        
        df = pd.DataFrame() 

        try:
            # --- 1. LEITURA DOS DADOS (SEM RASPAGEM) ---
            if os.path.exists(EXCEL_PATH):
                df = pd.read_excel(EXCEL_PATH)
                print(f"[{agora_str}] Arquivo '{EXCEL_PATH}' lido com sucesso. {len(df)} jogos.")
            else:
                print(f"[{agora_str}] ⚠️ Arquivo '{EXCEL_PATH}' não encontrado. Pulando alertas.")
                
            
            if df.empty:
                print(f"[{agora_str}] DataFrame vazio. Pulando verificação de alertas.")
            else:
                # --- 2. PROCESSAMENTO ---
                df = limpar_e_converter_dados(df)
                df = calcular_probabilidades(df)  # Mantém colunas de probabilidade

                # --- 3. VERIFICAÇÃO DE ALERTAS (até 2 horas antes com critérios) ---
                df_filtrado_criterios = filtrar_alertas_over15_e_partidas(df)
                df_alertas_30min = enviar_alertas_meia_hora(df_filtrado_criterios)

                # Define tipo de alerta para formatação (até 2h)
                if not df_alertas_30min.empty:
                    df_alertas_30min['Tipo_Alerta'] = "ALERTA_120MIN"

                # --- 4. ENVIO ÚNICO ---
                if not df_alertas_30min.empty:
                    df_enviados = enviar_alertes_unicos(df_alertas_30min, token, usuarios)
                    if not df_enviados.empty:
                        print(f"[{agora_str}] ✅ {len(df_enviados)} novos alertas (até 2h) enviados.")
                    else:
                        print(f"[{agora_str}] ⏸️ Nenhum novo alerta atende aos critérios de envio único.")
                else:
                    print(f"[{agora_str}] Nenhum jogo atende aos critérios (Over1.5>=70%, Partidas>7 e até 2h).")

            # --- FIM DO CICLO BEM-SUCEDIDO ---
            print(f"[{agora_str}] CICLO CONCLUÍDO COM SUCESSO. Indo para o SLEEP.")
            
        except Exception as e:
            agora_erro = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{agora_erro}] !!! ERRO CRÍTICO NO CICLO: {e}")
            print(f"[{agora_erro}] Tentando novamente após {LOAD_INTERVAL} segundos...")

        time.sleep(LOAD_INTERVAL)