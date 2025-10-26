# app.py
import streamlit as st
import pandas as pd
import os
from datetime import timedelta
import pytz
from src.scraper_soccerstats import get_today_games

TIMEZONE_TARGET = 'America/Sao_Paulo'

st.set_page_config(layout="wide")
st.title("📊 Robô de Apostas - SoccerStats")

# --- 1. Carregar ou Atualizar Dados ---
EXCEL_PATH = "data/Jogos_de_Hoje.xlsx"

# ... (Bloco de carregamento/atualização de dados permanece o mesmo) ...

if st.button("🔄 Atualizar jogos de hoje (Raspar dados)"):
    st.info("Raspando dados, por favor aguarde...")
    try:
        df = get_today_games()
        
        if not os.path.exists('data'):
            os.makedirs('data')
            
        df.to_excel(EXCEL_PATH, index=False)
        st.success("Dados atualizados com sucesso!")
        
    except Exception as e:
        st.error(f"Erro ao raspar ou salvar os dados. Erro: {e}")
        df = pd.DataFrame() 

else:
    try:
        df = pd.read_excel(EXCEL_PATH)
    except FileNotFoundError:
        st.warning(f"Arquivo '{EXCEL_PATH}' não encontrado. Clique em 'Atualizar jogos de hoje'.")
        df = pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo Excel: {e}")
        df = pd.DataFrame()

# --- 2. Processamento e Filtros ---

if not df.empty:
    st.subheader("Filtros de Apostas e Análise")

    # --- NOVO BLOCO: Renomear Colunas (troca _F por _A) ---
    colunas_renomeadas = {col: col.replace('_F', '_A') for col in df.columns if '_F' in col}
    df = df.rename(columns=colunas_renomeadas)
    # --- FIM NOVO BLOCO ---
    
    # --- 2.1 PRÉ-PROCESSAMENTO: Limpeza de dados e conversão de tipos ---
    
    # Lista de TODAS as colunas de porcentagem (agora usando _A)
    perc_cols = ['Over15_H', 'Over25_H', 'BTTS_H', 'Over15_A', 'Over25_A', 'BTTS_A']
    # Lista de colunas numéricas que não são porcentagem (agora usando _A)
    num_cols = ['PPG_Casa', 'PPG_Fora', 'Partidas'] # PPG_Fora deve ser PPG_A
    
    # Correção: Se PPG_Fora for lido como PPG_F pelo scraper, ele também será renomeado.
    # Mas vamos atualizar a lista de referência para o nome esperado:
    if 'Horário' in df.columns:
        try:
            # 1. Converte a coluna 'Horário' para o tipo datetime.
            df['DateTime_Ajustado'] = pd.to_datetime(df['Horário'], format='%H:%M:%S', errors='coerce')

            # 2. Adiciona 12 horas ao horário para corrigir o deslocamento.
            df['DateTime_Ajustado'] = df['DateTime_Ajustado'] + timedelta(hours=12)
            
            # 3. Formata a nova coluna de volta para string HH:MM para exibição.
            df['Horário'] = df['DateTime_Ajustado'].dt.strftime('%H:%M')
            
            # Opcional: Remover a coluna temporária
            df.drop(columns=['DateTime_Ajustado'], errors='ignore', inplace=True)

        except Exception as e:
            st.warning(f"Erro ao ajustar o horário da coluna 'Horário'. Verifique o formato dos dados. Erro: {e}")

    if 'PPG_Fora' in df.columns: # Se o raspador ainda usa o nome original
        num_cols.remove('PPG_Fora') 
        num_cols.append('PPG_A') # Usar o nome renomeado

    # Aplicar a limpeza de '%' e conversão para FLOAT
    for col in perc_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace('%', '', regex=False),
                errors='coerce'
            )

    # Conversão para numérico (float)
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Remover linhas que ficaram vazias (NaN)
    # Usamos uma lista de colunas existentes para o dropna
    cols_para_dropna = [col for col in perc_cols + num_cols if col in df.columns]
    df.dropna(subset=cols_para_dropna, inplace=True)
    
    
    # --- 2.2 CÁLCULO DA MÉDIA DE PROBABILIDADE (USANDO _A) ---
    
    # Colunas para calcular a média (apenas as que realmente existem no DF)
    colunas_para_media = [col for col in perc_cols if col in df.columns]

    if len(colunas_para_media) == 6:
        df['MÉDIA_PROB'] = df[colunas_para_media].sum(axis=1) / 6
        df['MÉDIA_PROB'] = df['MÉDIA_PROB'].round(2)
    else:
        st.warning(f"Atenção: Apenas {len(colunas_para_media)} de 6 colunas de porcentagem foram encontradas. A métrica 'MÉDIA_PROB' não será calculada.")
        df['MÉDIA_PROB'] = 0


    # --- 2.3 Filtros interativos (permanecem os mesmos) ---
    tipo_aposta = st.selectbox("Tipo de aposta", [
        "Todos",
        "Alta Prob. Aberto (Top)", 
        "Over 1.5",
        "Over 2.5",
        "Mandante Forte x Visitante Fraco",
        "Visitante Forte x Mandante Fraco"
    ])
    
    min_jogos = st.slider("Número mínimo de partidas", 0, 20, 0)

    perc_min = 0
    if tipo_aposta.startswith("Over") or tipo_aposta.startswith("Alta Prob."):
        perc_min = st.slider("Porcentagem mínima", 0, 100, 60)
    
    # --- 2.4 Aplicar filtros dinamicamente (USANDO _A) ---
    df_filtrado = df.copy()
    
	 # Over 1.5: média de Over15_H e Over15_A
    df_filtrado['Prob_Over1.5'] = ((df_filtrado['Over15_H'] + df_filtrado['Over15_A']) / 2).round(2)
    # Over 2.5: média de Over25_H e Over25_A
    df_filtrado['Prob_Over2.5'] = ((df_filtrado['Over25_H'] + df_filtrado['Over25_A']) / 2).round(2)
    # Ambas marcam: média de BTTS_H e BTTS_A
    df_filtrado['Prob_BTTS'] = ((df_filtrado['BTTS_H'] + df_filtrado['BTTS_A']) / 2).round(2)
    
    # Filtro básico de número de jogos
    if tipo_aposta != "Todos":
        df_filtrado = df_filtrado[df_filtrado['Partidas'] >= min_jogos] 

    # Lógica de Filtragem
    if tipo_aposta == "Mandante Forte x Visitante Fraco":
        df_filtrado = df_filtrado[
            (df_filtrado['PPG_Casa'] >= 1.5) &  
            (df_filtrado['PPG_A'] < 1.0) # PPG_A AGORA
        ]
        
    elif tipo_aposta == "Visitante Forte x Mandante Fraco":
        df_filtrado = df_filtrado[
            (df_filtrado['PPG_A'] >= 1.5) & # PPG_A AGORA
            (df_filtrado['PPG_Casa'] < 1.0)    
        ]
        
    elif tipo_aposta == "Alta Prob. Aberto (Top)":
        if 'MÉDIA_PROB' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['MÉDIA_PROB'] >= perc_min]
            df_filtrado = df_filtrado.sort_values(by='MÉDIA_PROB', ascending=False)
        
    elif tipo_aposta == "Over 1.5":
        df_filtrado = df_filtrado[df_filtrado['Over15_H'] >= perc_min]
        
    elif tipo_aposta == "Over 2.5":
        df_filtrado = df_filtrado[df_filtrado['Over25_H'] >= perc_min] 
        
    # --- 3. Exibir resultados ---
    st.subheader(f"Jogos filtrados ({len(df_filtrado)} partidas encontradas)")
    
    # Colunas essenciais para identificação e análise (USANDO _A)
    cols_to_display = [
        'País', 'Horário', 'Time 1', 'Time 2', 'MÉDIA_PROB', 
        'PPG_Casa', 'PPG_A', 'Over15_H', 'Over15_A', 'Over25_H', 'Over25_A', 'BTTS_H', 
          'BTTS_A', 'Partidas'
    ]
    
    # Filtra a lista para exibir apenas as colunas que EXISTEM no DataFrame filtrado
    final_cols = [col for col in cols_to_display if col in df_filtrado.columns]
    
    # Seu DataFrame deve ser df_filtrado.
st.subheader(f"Jogos filtrados ({len(df_filtrado)} partidas encontradas)")

if not df_filtrado.empty:
    
    # ----------------------------------------------------------------------
    # PARTE 1: EXIBIR O DATAFRAME ORIGINAL (SEM LINKS)
    # ----------------------------------------------------------------------
    st.markdown("### Tabela Original (Interativa, sem links clicáveis)")
    
    # Lista de colunas a serem exibidas na ordem desejada para a tabela simples
    cols_display_simple = [
        'País', 'Horário', 'Time 1', 'Time 2', 'MÉDIA_PROB', 
        'PPG_Casa', 'PPG_A', 'Over15_H', 'Over15_A', 'Over25_H', 'Over25_A', 'BTTS_H', 'BTTS_A', 'Partidas'
    ]
    
    # Filtra e exibe o DataFrame simples
    final_cols_simple = [col for col in cols_display_simple if col in df_filtrado.columns]
    
    # Usa st.dataframe para manter a interatividade padrão
    st.dataframe(
        df_filtrado[final_cols_simple], 
        hide_index=True, 
        use_container_width=True,
    )
    
# ----------------------------------------------------------------------
# PARTE 2: EXIBIR A TABELA COM LINKS CLICÁVEIS (USANDO MARKDOWN/HTML)
# ----------------------------------------------------------------------
st.markdown("---")
st.markdown("### Tabela com Links Clicáveis (Ordenada por Horário)")

df_html = df_filtrado.copy()

# === MUDANÇA APLICADA AQUI: ORDENAR POR HORÁRIO ===
if 'Horário' in df_html.columns:
    df_html = df_html.sort_values(by='Horário', ascending=True).reset_index(drop=True)
# =================================================

GOOGLE_SEARCH_BASE_URL = "https://www.google.com/search?q="

# Função auxiliar para pegar o nome puro
def get_clean_name(name):
    return str(name).strip() if not pd.isna(name) else ""

# Função para criar o link HTML para um time (Texto: Nome do Time)
def criar_link_google(nome_time):
    if pd.isna(nome_time) or nome_time == "":
        return nome_time
    
    query = str(nome_time).replace(' ', '+').strip()
    url = GOOGLE_SEARCH_BASE_URL + query
    return f'<a href="{url}" target="_blank">{nome_time}</a>'
    
# Função para criar o link HTML para o confronto (Texto: "Ver Jogo")
def criar_link_resultado_puro(time1, time2):
    if not time1 or not time2:
        return ""
    
    # Monta a query com os NOMES PUROS
    query = f"{time1} vs {time2}"
    query_encoded = str(query).replace(' ', '+').strip()
    url = GOOGLE_SEARCH_BASE_URL + query_encoded
    return f'<a href="{url}" target="_blank">Ver Jogo</a>'


# --- FLUXO DE PROCESSAMENTO ---

# 1. Cria a coluna 'Resultado' usando os nomes puros (ANTES da conversão para HTML)
if 'Time 1' in df_html.columns and 'Time 2' in df_html.columns:
    df_html['Resultado'] = df_html.apply(
        lambda row: criar_link_resultado_puro(
            get_clean_name(row['Time 1']), 
            get_clean_name(row['Time 2'])
        ), axis=1
    )

# 2. Converte as colunas de Times para HTML
if 'Time 1' in df_html.columns:
    df_html['Time 1'] = df_html['Time 1'].apply(criar_link_google)

if 'Time 2' in df_html.columns:
    df_html['Time 2'] = df_html['Time 2'].apply(criar_link_google)
    
# 3. Formatação de porcentagem
if 'MÉDIA_PROB' in df_html.columns:
    df_html['MÉDIA_PROB'] = df_html['MÉDIA_PROB'].apply(lambda x: f"{int(x)}%")
    
# 4. Exibição da Tabela HTML
cols_to_display_html = [
    'País', 'Horário', 'Time 1', 'Time 2', 'Resultado', 'MÉDIA_PROB', 'Prob_Over1.5', 'Prob_Over2.5', 'Prob_BTTS',
    'PPG_Casa', 'PPG_A', 'Over15_H', 'Over15_A', 'Over25_H', 'Over25_A', 'BTTS_H', 'BTTS_A', 'Partidas'
]

final_cols_html = [col for col in cols_to_display_html if col in df_html.columns]

st.markdown(
    df_html[final_cols_html].to_html(
        escape=False, 
        index=False, 
        float_format='{:,.2f}'.format
    ), 
    unsafe_allow_html=True
)

from src.telegram_alerts import enviar_alertas
from dotenv import load_dotenv
import os

load_dotenv()
token = os.getenv("TELEGRAM_TOKEN")
usuarios = [int(x) for x in os.getenv("TELEGRAM_USERS").split(",")]

# Depois de gerar o df_filtrado no Streamlit:
if st.button("🚀 Enviar alertas Telegram"):
    enviar_alertas(df_filtrado, token, usuarios)
    st.success("Alertas enviados!")