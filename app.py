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
from src.database import prepare_df_for_insertion, get_mysql_connection, insert_df_into_mysql, run_results_update_workflow
from buscar_resultados import recreate_results_csv

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
# Remoção de page_link que causava erro em algumas versões
# A navegação multipáginas do Streamlit exibirá automaticamente as páginas do diretório "pages/"
st.sidebar.title("Menu")
# A navegação multipáginas do Streamlit exibirá automaticamente as páginas do diretório "pages/" no menu lateral.

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
if st.button("🔄 RASPAR DADOS AGORA (Pode levar 10-20 segundos)"):
    try:
        # Remove o arquivo Excel antes de raspar novamente
        if os.path.exists(EXCEL_PATH):
            os.remove(EXCEL_PATH)
            st.info(f"Arquivo '{EXCEL_PATH}' removido para recriação.")
    except Exception as e:
        st.warning(f"Não foi possível remover '{EXCEL_PATH}': {e}")
    # Limpa cache e força nova raspagem que irá salvar o Excel novamente
    clear_cache_and_reload()
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
    ], index=2)  # default: Over 1.5
    
    min_jogos = st.slider("Número mínimo de partidas", 0, 20, 10)  # default: 10
    
    perc_min = 0
    if tipo_aposta.startswith("Over") or tipo_aposta.startswith("Alta Prob."):
        perc_min = st.slider("Porcentagem mínima", 0, 100, 70)  # default: 70
    
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
        # Tabela Original (Interativa, sem links clicáveis)
        st.markdown("### Tabela Original (Interativa, sem links clicáveis)")
        
        cols_display_simple = [
            'País', 'Horário', 'Time 1', 'Time 2', 'MÉDIA_PROB', 'Prob_Over1.5', 'Prob_Over2.5', 'Prob_BTTS',
            'PPG_Casa', 'PPG_A', 'Over15_H', 'Over15_A', 'Over25_H', 'Over25_A', 'BTTS_H', 'BTTS_A', 'Partidas'
        ]
        final_cols_simple = [col for col in cols_display_simple if col in df_filtrado.columns]
        
        df_simple = df_filtrado.copy()
        formato_24h = '%H:%M'
        formato_12h = '%I:%M %p'
        
        from datetime import datetime
        
        def parse_to_timestamp(s: str):
            s = str(s).strip()
            for fmt in (formato_24h, formato_12h):
                try:
                    t = datetime.strptime(s, fmt).time()
                    return datetime(2000, 1, 1, t.hour, t.minute)  # timestamp neutro
                except Exception:
                    continue
            return None
        
        # Compensação: subtrai 3 horas
        OFFSET_HORAS = -3
        
        def minutes_from_ts(ts):
            if ts is None:
                return None
            ajustado = ts + timedelta(hours=OFFSET_HORAS)
            return int(ajustado.hour) * 60 + int(ajustado.minute)
        
        def format_24h_with_offset(ts, original: str):
            if ts is None:
                return str(original).strip() if original is not None else ""
            ajustado = ts + timedelta(hours=OFFSET_HORAS)
            h = int(ajustado.hour)
            m = int(ajustado.minute)
            return f"{h:02d}:{m:02d}"
        
        parsed_ts = df_simple['Horário'].apply(parse_to_timestamp)
        df_simple['Horario_sort_min'] = parsed_ts.apply(minutes_from_ts)
        df_simple['Horário'] = [format_24h_with_offset(ts, orig) for ts, orig in zip(parsed_ts, df_simple['Horário'])]
        # Não sobrescrever 'Horário': manter o valor original do DF
        # df_simple['Horário'] = [to_inverted_ampm_from_ts(ts, orig) for ts, orig in zip(parsed_ts, df_simple['Horário'])]  # removido
        
        # Ordena cronologicamente; inválidos vão para o fim
        df_simple = df_simple.sort_values('Horario_sort_min', na_position='last')
        
        st.dataframe(
            df_simple[final_cols_simple].round(2),
            hide_index=True,
            use_container_width=True,
        )
        
        # ----------------------------------------------------------------------
        # PARTE 2: EXIBIR A TABELA COM LINKS CLICÁVEIS (USANDO MARKDOWN/HTML)
        # ----------------------------------------------------------------------
        # Tabela com Links Clicáveis (Ordenada por Horário)
        st.markdown("---")
        st.markdown("### Tabela com Links Clicáveis (Ordenada por Horário)")

        df_html = df_filtrado.copy()

        formato_24h = '%H:%M'
        formato_12h = '%I:%M %p'

        from datetime import datetime

        def parse_to_timestamp(s: str):
            s = str(s).strip()
            for fmt in (formato_24h, formato_12h):
                try:
                    t = datetime.strptime(s, fmt).time()
                    return datetime(2000, 1, 1, t.hour, t.minute)
                except Exception:
                    continue
            return None

        OFFSET_HORAS = -3

        def minutes_from_ts(ts):
            if ts is None:
                return None
            ajustado = ts + timedelta(hours=OFFSET_HORAS)
            return int(ajustado.hour) * 60 + int(ajustado.minute)

        def format_24h_with_offset(ts, original: str):
            if ts is None:
                return str(original).strip() if original is not None else ""
            ajustado = ts + timedelta(hours=OFFSET_HORAS)
            h = int(ajustado.hour)
            m = int(ajustado.minute)
            return f"{h:02d}:{m:02d}"

        parsed_ts = df_html['Horário'].apply(parse_to_timestamp)
        df_html['Horario_sort_min'] = parsed_ts.apply(minutes_from_ts)
        df_html['Horário'] = [format_24h_with_offset(ts, orig) for ts, orig in zip(parsed_ts, df_html['Horário'])]

        df_html = df_html.sort_values('Horario_sort_min', na_position='last').reset_index(drop=True)

        GOOGLE_SEARCH_BASE_URL = "https://www.google.com/search?q="

        def get_clean_name(name):
            return str(name).strip().replace(" ", "+")

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
# BANCO DE DADOS (MySQL) - Integração via Streamlit
# -----------------------------------------------------------
st.markdown("---")
st.subheader("Banco de Dados (MySQL)")

# Lê direto do .env (sem inputs na UI)
mysql_host = os.getenv("MYSQL_HOST", "localhost")
mysql_user = os.getenv("MYSQL_USER", "")
mysql_password = os.getenv("MYSQL_PASSWORD", "")
mysql_db = os.getenv("MYSQL_DB", "simulador-apostas")

st.info(f"Usando configuração do .env: host='{mysql_host}', db='{mysql_db}'")

col_db1, col_db2, col_db3 = st.columns(3)

with col_db1:
    if st.button("🔌 Testar conexão MySQL"):
        try:
            conn = get_mysql_connection()
            conn.close()
            st.success("Conexão MySQL bem-sucedida!")
        except Exception as e:
            st.error(f"Erro ao conectar no MySQL: {e}")

with col_db2:
    if st.button("📥 Inserir jogos filtrados no MySQL"):
        if 'df_filtrado' in locals() and not df_filtrado.empty:
            try:
                df_ready = prepare_df_for_insertion(df_filtrado)
                conn = get_mysql_connection()
                total = insert_df_into_mysql(df_ready, conn)
                conn.close()
                st.success(f"✅ {total} registros inseridos na tabela 'jogos' (filtrados).")
            except Exception as e:
                st.error(f"Erro ao inserir jogos filtrados: {e}")
        else:
            st.info("Filtre alguns jogos para habilitar a inserção no MySQL.")

with col_db3:
    if st.button("📥 Inserir todos os jogos de hoje no MySQL"):
        if 'df' in locals() and not df.empty:
            try:
                df_ready = prepare_df_for_insertion(df)
                conn = get_mysql_connection()
                total = insert_df_into_mysql(df_ready, conn)
                conn.close()
                st.success(f"✅ {total} registros inseridos na tabela 'jogos' (todos os jogos de hoje).")
            except Exception as e:
                st.error(f"Erro ao inserir todos os jogos: {e}")
        else:
            st.info("Nenhum dado carregado. Clique em 'RASPAR DADOS AGORA' para obter jogos de hoje.")
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
# Atualização de resultados via CSV
st.markdown("---")
st.subheader("Atualização de Resultados (CSV → MySQL)")
csv_path = st.text_input("Arquivo CSV de resultados", "resultados_futebol_hoje.csv")

# Controles de log para auditoria das updates
log_enabled = st.checkbox("Gerar log de updates (SQL)", value=True)
log_path = st.text_input("Arquivo de log", "logs/results_update.sql")
fallback_like = st.checkbox("Usar fallback por LIKE com normalização/alias", value=True)

col_csv1, col_csv2 = st.columns(2)
with col_csv1:
    if st.button("♻️ Recriar arquivo de resultados CSV"):
        try:
            total_csv = recreate_results_csv(csv_path)
            st.success(f"✅ CSV recriado com {total_csv} jogos em '{csv_path}'.")
        except Exception as e:
            st.error(f"Erro ao recriar CSV: {e}")

with col_csv2:
    if st.button("🔄 Atualizar resultados do CSV no MySQL"):
        os.environ['MYSQL_HOST'] = mysql_host
        os.environ['MYSQL_USER'] = mysql_user
        os.environ['MYSQL_PASSWORD'] = mysql_password
        os.environ['MYSQL_DB'] = mysql_db
        try:
            total_proc = run_results_update_workflow(
                csv_path,
                log_file_path=log_path if log_enabled else None,
                fallback_like=fallback_like
            )
            msg = f"✅ {total_proc} partidas atualizadas a partir de '{csv_path}'."
            if log_enabled:
                msg += f" Log salvo em '{log_path}'."
            st.success(msg)
        except Exception as e:
            st.error(f"Erro ao atualizar resultados do CSV: {e}")