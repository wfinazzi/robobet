# app.py
import streamlit as st
import pandas as pd
import os
import time # Adicionado para garantir a definição
import pytz
from datetime import timedelta, datetime, date, time as dt_time # Adicionado dt_time
from dotenv import load_dotenv

# Funções auxiliares (apenas a função de raspagem é importada)
from src.scraper_soccerstats import get_today_games 
from src.telegram_alerts import enviar_alertas, enviar_mensagem # Importado para o botão de teste

# --- Configurações ---
TIMEZONE_TARGET = 'America/Sao_Paulo'
EXCEL_PATH = "data/Jogos_de_Hoje.xlsx"

# Configurações do Telegram para o botão de teste
load_dotenv()
token = os.getenv("TELEGRAM_TOKEN")
usuarios = [int(x) for x in os.getenv("TELEGRAM_USERS").split(",")]
primeiro_usuario = usuarios[0] if usuarios else None
tz_target = pytz.timezone(TIMEZONE_TARGET)
DATA_DE_HOJE = datetime.now(tz_target).date()

st.set_page_config(layout="wide")
st.title("📊 Robô de Apostas - SoccerStats")

# Inicializa df_filtrado como DataFrame vazio (necessário para o escopo)
df_filtrado = pd.DataFrame() 

# --- 1. Carregar ou Atualizar Dados (Lógica de data) ---
ARQUIVO_EXISTE = os.path.exists(EXCEL_PATH)
PODE_RASPAR = True
MENSAGEM_RASPAGEM = "🔄 Atualizar jogos de hoje (Raspar dados)"

if ARQUIVO_EXISTE:
    data_modificacao_timestamp = os.path.getmtime(EXCEL_PATH)
    data_modificacao = datetime.fromtimestamp(data_modificacao_timestamp).date()
    
    if data_modificacao == DATA_DE_HOJE:
        PODE_RASPAR = False
        MENSAGEM_RASPAGEM = f"✅ Raspagem de hoje ({data_modificacao.strftime('%d/%m')}) já foi realizada."

# O botão de raspagem é desabilitado se já foi feito hoje (mas o código de raspagem não deve rodar aqui, o main.py é que faz isso)
if st.button(MENSAGEM_RASPAGEM, disabled=not PODE_RASPAR): 
    # Esta ação é apenas um PLACEHOLDER para o Streamlit, o MAIN.PY que deve fazer a raspagem
    if PODE_RASPAR:
        st.warning("A raspagem é feita pelo script de backend (main.py). Lendo dados antigos e marcando a raspagem como necessária.")
        # Se você quer forçar a raspagem por aqui, descomente e use o código do main.py
        # st.info("Raspando dados, por favor aguarde...")
        # try:
        #     df = get_today_games()
        #     # ... salvar df ...
        #     st.success("Dados atualizados com sucesso!")
        # except Exception as e:
        #     st.error(f"Erro ao raspar ou salvar os dados. Erro: {e}")
        #     df = pd.DataFrame() 
    
    # Após o clique, o Streamlit executa o resto do script e carrega o arquivo.

try:
    if ARQUIVO_EXISTE:
         df = pd.read_excel(EXCEL_PATH)
         if not PODE_RASPAR:
             st.info(f"Dados carregados do arquivo salvo hoje.")
    else:
         st.warning(f"Arquivo '{EXCEL_PATH}' não encontrado. Execute o 'main.py' para raspar os dados.")
         df = pd.DataFrame()
         
except FileNotFoundError:
    st.warning(f"Arquivo '{EXCEL_PATH}' não encontrado. Execute o 'main.py' para raspar os dados.")
    df = pd.DataFrame()
except Exception as e:
    st.error(f"Erro ao carregar o arquivo Excel: {e}")
    df = pd.DataFrame()

# --- 2. Processamento e Filtros ---

if not df.empty:
    st.subheader("Filtros de Apostas e Análise")
    
    # -----------------------------------------------------------
    # REPETIÇÃO DO CÁLCULO DE MÉDIAS (ESSENCIAL)
    # -----------------------------------------------------------
    # Lista de TODAS as colunas de porcentagem
    perc_cols = ['Over15_H', 'Over25_H', 'BTTS_H', 'Over15_A', 'Over25_A', 'BTTS_A']
    num_cols = ['PPG_Casa', 'PPG_A', 'Partidas'] 

    # Aplicar a limpeza de '%' e conversão para FLOAT (caso o Excel não tenha sido salvo limpo)
    for col in perc_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace('%', '', regex=False), errors='coerce')
    for col in num_cols:
         if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=[col for col in perc_cols + num_cols if col in df.columns], inplace=True)
    
    # Cálculo das médias (Repetido do main.py para garantir que Streamlit tenha as colunas de cálculo)
    colunas_para_media = [col for col in perc_cols if col in df.columns and pd.api.types.is_numeric_dtype(df[col])]
    if len(colunas_para_media) == 6:
        df['MÉDIA_PROB'] = df[colunas_para_media].sum(axis=1) / 6
        df['MÉDIA_PROB'] = df['MÉDIA_PROB'].round(2)
    else:
        df['MÉDIA_PROB'] = 0

    # Adiciona colunas de probabilidade para filtros
    if 'Over15_H' in df.columns and 'Over15_A' in df.columns:
        df['Prob_Over1.5'] = ((df['Over15_H'] + df['Over15_A']) / 2).round(2)
    if 'Over25_H' in df.columns and 'Over25_A' in df.columns:
        df['Prob_Over2.5'] = ((df['Over25_H'] + df['Over25_A']) / 2).round(2)
    if 'BTTS_H' in df.columns and 'BTTS_A' in df.columns:
        df['Prob_BTTS'] = ((df['BTTS_H'] + df['BTTS_A']) / 2).round(2)
    # -----------------------------------------------------------


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
    
    # --- 2.4 Aplicar filtros dinamicamente ---
    df_filtrado = df.copy()
    
    df_filtrado = df_filtrado[df_filtrado['Partidas'] >= min_jogos] 

    if tipo_aposta == "Mandante Forte x Visitante Fraco":
        df_filtrado = df_filtrado[
            (df_filtrado['PPG_Casa'] >= 1.5) & 
            (df_filtrado['PPG_A'] < 1.0) 
        ]
        
    elif tipo_aposta == "Visitante Forte x Mandante Fraco":
        df_filtrado = df_filtrado[
            (df_filtrado['PPG_A'] >= 1.5) & 
            (df_filtrado['PPG_Casa'] < 1.0) 
        ]
        
    elif tipo_aposta == "Alta Prob. Aberto (Top)":
        if 'MÉDIA_PROB' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['MÉDIA_PROB'] >= perc_min]
            df_filtrado = df_filtrado.sort_values(by='MÉDIA_PROB', ascending=False)
        
    elif tipo_aposta == "Over 1.5":
        df_filtrado = df_filtrado[df_filtrado['Prob_Over1.5'] >= perc_min]
        
    elif tipo_aposta == "Over 2.5":
        df_filtrado = df_filtrado[df_filtrado['Prob_Over2.5'] >= perc_min] 
        
    # --- 3. Exibir resultados ---
    st.subheader(f"Jogos filtrados ({len(df_filtrado)} partidas encontradas)")
    
    if not df_filtrado.empty:
        
        # ----------------------------------------------------------------------
        # PARTE 1: EXIBIR O DATAFRAME ORIGINAL (SEM LINKS)
        # ----------------------------------------------------------------------
        st.markdown("### Tabela Original (Interativa, sem links clicáveis)")
        
        cols_display_simple = [
            'País', 'Horário', 'Time 1', 'Time 2', 'MÉDIA_PROB', 'Prob_Over1.5', 'Prob_Over2.5', 'Prob_BTTS',
            'PPG_Casa', 'PPG_A', 'Over15_H', 'Over15_A', 'Over25_H', 'Over25_A', 'BTTS_H', 'BTTS_A', 'Partidas'
        ]
        
        final_cols_simple = [col for col in cols_display_simple if col in df_filtrado.columns]
        
        st.dataframe(
            df_filtrado[final_cols_simple].round(2), 
            hide_index=True, 
            use_container_width=True,
        )
        
        # ----------------------------------------------------------------------
        # PARTE 2: EXIBIR A TABELA COM LINKS CLICÁVEIS (USANDO MARKDOWN/HTML)
        # ----------------------------------------------------------------------
        st.markdown("---")
        st.markdown("### Tabela com Links Clicáveis (Ordenada por Horário)")

        df_html = df_filtrado.copy()

        # === ORDENAR POR HORÁRIO ===
        if 'Horário' in df_html.columns:
            df_html = df_html.sort_values(by='Horário', ascending=True).reset_index(drop=True)
        
        GOOGLE_SEARCH_BASE_URL = "https://www.google.com/search?q="

        def get_clean_name(name):
            return str(name).strip() if not pd.isna(name) else ""

        def criar_link_google(nome_time):
            if pd.isna(nome_time) or nome_time == "": return nome_time
            query = str(nome_time).replace(' ', '+').strip()
            url = GOOGLE_SEARCH_BASE_URL + query
            return f'<a href="{url}" target="_blank">{nome_time}</a>'
            
        def criar_link_resultado_puro(time1, time2):
            if not time1 or not time2: return ""
            query = f"{time1} vs {time2}"
            query_encoded = str(query).replace(' ', '+').strip()
            url = GOOGLE_SEARCH_BASE_URL + query_encoded
            return f'<a href="{url}" target="_blank">Ver Jogo</a>'

        # 1. Cria a coluna 'Resultado'
        if 'Time 1' in df_html.columns and 'Time 2' in df_html.columns:
            df_html['Resultado'] = df_html.apply(
                lambda row: criar_link_resultado_puro(get_clean_name(row['Time 1']), get_clean_name(row['Time 2'])), axis=1
            )

        # 2. Converte as colunas de Times para HTML
        if 'Time 1' in df_html.columns: df_html['Time 1'] = df_html['Time 1'].apply(criar_link_google)
        if 'Time 2' in df_html.columns: df_html['Time 2'] = df_html['Time 2'].apply(criar_link_google)
            
        # 3. Formatação de porcentagem para o HTML
        for col in ['MÉDIA_PROB', 'Prob_Over1.5', 'Prob_Over2.5', 'Prob_BTTS']:
            if col in df_html.columns:
                df_html[col] = df_html[col].apply(lambda x: f"{int(x)}%" if pd.notna(x) else 'N/A')
            
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

# -----------------------------------------------------------
# FERRAMENTAS DE TESTE E ALERTA MANUAL
# -----------------------------------------------------------
st.markdown("---")
st.subheader("Ferramentas de Teste e Alerta Manual")

col1, col2 = st.columns(2)

# BOTÃO 1: TESTE DE CONEXÃO TELEGRAM
with col1:
    if primeiro_usuario:
        if st.button("🚨 TESTAR CONEXÃO TELEGRAM (Alerta Rápido)"):
            st.info(f"Tentando enviar mensagem de teste para o chat ID: {primeiro_usuario}...")
            
            mensagem_teste = (
                f"✅ <b>Alerta de Teste de Conexão</b>\n"
                f"Hora: {datetime.now(tz_target).strftime('%H:%M:%S')}\n"
                f"Status: Conexão bem-sucedida! O robô está online."
            )
            
            enviar_mensagem(primeiro_usuario, mensagem_teste, token)
            st.success("Mensagem de teste enviada (Verifique seu Telegram)!")
    else:
        st.error("Erro: Nenhuma ID de usuário do Telegram encontrada em TELEGRAM_USERS.")

# BOTÃO 2: ALERTA MANUAL DOS JOGOS FILTRADOS
with col2:
    if 'df_filtrado' in locals() and not df_filtrado.empty:
        if st.button("🚀 Enviar alertas Telegram (Todos os Jogos Filtrados)"):
            enviar_alertas(df_filtrado, token, usuarios)
            st.success("Alertas dos jogos filtrados enviados!")
    else:
        st.info("Filtre alguns jogos para habilitar o envio manual.")