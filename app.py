# app.py (Raspagem Direta)
import streamlit as st
import pandas as pd
import os
import time 
import pytz
from datetime import timedelta, datetime, date, time as dt_time 
from dotenv import load_dotenv

# Dependência do scraper (Deve estar disponível no src/)
from src.scraper_soccerstats import get_today_games 
# 🚨 CORREÇÃO NO IMPORT: Usar a função de envio único
from src.telegram_alerts import enviar_alertes_unicos, enviar_mensagem 

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

# --- FUNÇÕES DE PROCESSAMENTO DE DADOS (Mantidas) ---
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
    # 1. Cálculos de Médias Simples
    if 'Over15_H' in df.columns and 'Over15_A' in df.columns:
        df['Prob_Over1.5'] = ((df['Over15_H'] + df['Over15_A']) / 2).round(2)
        df['Over15_MEDIA'] = df['Prob_Over1.5'] # Manter o nome de coluna para compatibilidade
    if 'Over25_H' in df.columns and 'Over25_A' in df.columns:
        df['Prob_Over2.5'] = ((df['Over25_H'] + df['Over25_A']) / 2).round(2)
        df['Over25_MEDIA'] = df['Prob_Over2.5']
    if 'BTTS_H' in df.columns and 'BTTS_A' in df.columns:
        df['Prob_BTTS'] = ((df['BTTS_H'] + df['BTTS_A']) / 2).round(2)
        df['Over_BOTH'] = df['Prob_BTTS']

    # 2. Cálculo da MÉDIA_PROB
    perc_cols = ['Over15_H', 'Over25_H', 'BTTS_H', 'Over15_A', 'Over25_A', 'BTTS_A']
    colunas_para_media = [col for col in perc_cols if col in df.columns and pd.api.types.is_numeric_dtype(df[col])]
    
    if len(colunas_para_media) == 6:
        df['MÉDIA_PROB'] = df[colunas_para_media].sum(axis=1) / 6
        df['MÉDIA_PROB'] = df['MÉDIA_PROB'].round(2)
    else:
        df['MÉDIA_PROB'] = 0 
        
    return df
# --- FIM DAS FUNÇÕES DE PROCESSAMENTO ---


# --- FUNÇÃO PRINCIPAL DE CARREGAMENTO DE DADOS COM CACHE (Mantida) ---
@st.cache_data(ttl=3600, show_spinner="🔄 Raspando dados atualizados. Aguarde, isso pode levar 10-20 segundos...")
def load_and_process_data():
    
    # 1. Tenta carregar do arquivo se for de hoje (rápido)
    if os.path.exists(EXCEL_PATH):
        data_modificacao_timestamp = os.path.getmtime(EXCEL_PATH)
        data_modificacao = datetime.fromtimestamp(data_modificacao_timestamp).date()
        
        if data_modificacao == DATA_DE_HOJE:
            df = pd.read_excel(EXCEL_PATH)
            # Verifica se o DF tem dados e se o cache está sendo usado
            if not df.empty and 'MÉDIA_PROB' in df.columns:
                st.info(f"Dados carregados do Excel salvo em {datetime.fromtimestamp(data_modificacao_timestamp).strftime('%H:%M:%S')} (Cache ativo).")
                return df

    # 2. Raspagem (Acontece se o cache não existir ou estiver expirado)
    st.info("Iniciando raspagem no SoccerStats...")
    df = get_today_games() 
    
    # 3. Processamento
    df = limpar_e_converter_dados(df)
    df = calcular_probabilidades(df)
    
    # 4. Salvar (para o bot de backend e para carregamentos rápidos futuros)
    if not os.path.exists('data'):
        os.makedirs('data')
    df.to_excel(EXCEL_PATH, index=False)
    
    return df
# --- FIM DA FUNÇÃO DE CACHE ---


# --- LÓGICA DE BOTÃO E CARREGAMENTO (Mantida) ---

# Função para limpar o cache e forçar a nova execução
def clear_cache_and_reload():
    st.cache_data.clear()
    
# Botão que limpa o cache (força a função load_and_process_data a executar a raspagem)
st.markdown("---")
if st.button("🔄 RASPAR DADOS AGORA (Pode levar 10-20 segundos)", on_click=clear_cache_and_reload):
    st.rerun() 

# Chama a função de carregamento. O cache do Streamlit cuida da raspagem lenta.
try:
    df = load_and_process_data()
except Exception as e:
    st.error(f"Erro ao carregar ou raspar os dados: {e}")
    df = pd.DataFrame()


# --- RESTO DO CÓDIGO (FILTROS E TABELAS - MANTIDO) ---

# Inicializa df_filtrado como DataFrame vazio (necessário para o escopo)
df_filtrado = pd.DataFrame() 


if not df.empty:
    st.subheader("Filtros de Apostas e Análise")
    
    # --- FILTROS INTERATIVOS ---
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
    
    # --- Aplicar filtros dinamicamente ---
    df_filtrado = df.copy()
    
    df_filtrado = df_filtrado[df_filtrado.get('Partidas', 0) >= min_jogos] 

    if tipo_aposta == "Mandante Forte x Visitante Fraco":
        df_filtrado = df_filtrado[
            (df_filtrado.get('PPG_Casa', 0) >= 1.5) & 
            (df_filtrado.get('PPG_A', 0) < 1.0) 
        ]
        
    elif tipo_aposta == "Visitante Forte x Mandante Fraco":
        df_filtrado = df_filtrado[
            (df_filtrado.get('PPG_A', 0) >= 1.5) & 
            (df_filtrado.get('PPG_Casa', 0) < 1.0) 
        ]
        
    elif tipo_aposta == "Alta Prob. Aberto (Top)":
        if 'MÉDIA_PROB' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['MÉDIA_PROB'] >= perc_min]
            df_filtrado = df_filtrado.sort_values(by='MÉDIA_PROB', ascending=False)
        
    elif tipo_aposta == "Over 1.5":
        df_filtrado = df_filtrado[df_filtrado.get('Prob_Over1.5', 0) >= perc_min] 
        
    elif tipo_aposta == "Over 2.5":
        df_filtrado = df_filtrado[df_filtrado.get('Prob_Over2.5', 0) >= perc_min] 
        
    # --- 4. Exibir resultados ---
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
            try:
                df_html['Horário'] = df_html['Horário'].astype(str)
            except:
                pass 
            df_html = df_html.sort_values(by='Horário', ascending=True).reset_index(drop=True)
        
        GOOGLE_SEARCH_BASE_URL = "https://www.google.com/search?q="

        # Funções para criar links
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

        # Cria colunas de HTML
        if 'Time 1' in df_html.columns and 'Time 2' in df_html.columns:
            df_html['Resultado'] = df_html.apply(
                lambda row: criar_link_resultado_puro(get_clean_name(row['Time 1']), get_clean_name(row['Time 2'])), axis=1
            )
        if 'Time 1' in df_html.columns: df_html['Time 1'] = df_html['Time 1'].apply(criar_link_google)
        if 'Time 2' in df_html.columns: df_html['Time 2'] = df_html['Time 2'].apply(criar_link_google)
            
        # Formatação de porcentagem para o HTML
        for col in ['MÉDIA_PROB', 'Prob_Over1.5', 'Prob_Over2.5', 'Prob_BTTS']:
            if col in df_html.columns:
                df_html[col] = df_html[col].apply(lambda x: f"{int(x)}%" if pd.notna(x) and x is not None else 'N/A')
            
        # Exibição da Tabela HTML
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
        if st.button("🚨 TESTAR CONEXÃO TELEGRAM"):
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
    # Obtém df_filtrado do escopo local, se existir
    if 'df_filtrado' in locals() and not df_filtrado.empty:
        
        if st.button("🚀 Enviar alertas Telegram (Filtrados)"):
            
            # 🚨 CORREÇÃO DE LÓGICA: Adicionar Tipo_Alerta e usar a nova função
            df_enviar = df_filtrado.copy()
            df_enviar['Tipo_Alerta'] = "ALERTA_MANUAL_APP" # Tipo para a formatação
            
            df_enviados = enviar_alertes_unicos(df_enviar, token, usuarios)
            
            if not df_enviados.empty:
                st.success(f"✅ {len(df_enviados)} novos alertas enviados manualmente!")
            else:
                st.info("⏸️ Nenhum novo alerta enviado. Os jogos filtrados já foram alertados.")
                
    else:
        st.info("Filtre alguns jogos para habilitar o envio manual.")