# Resultados (Banco de Dados)
import os
import pandas as pd
import streamlit as st
from datetime import datetime
from dotenv import load_dotenv

from src.database import get_mysql_connection

st.set_page_config(page_title="Resultados (DB)", layout="wide")
st.title("📈 Resultados de Jogos (MySQL)")

# Menu lateral simples (informativo)
st.sidebar.title("Menu")
st.sidebar.markdown("- Página: Raspagem e Análise")
st.sidebar.markdown("- Página: Resultados (DB)")

# Credenciais do MySQL
load_dotenv()
mysql_host = st.text_input("Host MySQL", os.getenv("MYSQL_HOST", "localhost"))
mysql_user = st.text_input("Usuário MySQL", os.getenv("MYSQL_USER", ""))
mysql_password = st.text_input("Senha MySQL", os.getenv("MYSQL_PASSWORD", ""), type="password")
mysql_db = st.text_input("Banco de Dados", os.getenv("MYSQL_DB", "simulador-apostas"))

# Filtro de data
hoje = datetime.now().date()
selected_date = st.date_input("Data do jogo", value=hoje)

col1, col2 = st.columns(2)
with col1:
    buscar = st.button("🔎 Buscar jogos com resultados (FT)")
with col2:
    testar = st.button("🔌 Testar conexão")

if testar:
    os.environ['MYSQL_HOST'] = mysql_host
    os.environ['MYSQL_USER'] = mysql_user
    os.environ['MYSQL_PASSWORD'] = mysql_password
    os.environ['MYSQL_DB'] = mysql_db
    try:
        conn = get_mysql_connection()
        conn.close()
        st.success("Conexão MySQL bem-sucedida!")
    except Exception as e:
        st.error(f"Erro ao conectar no MySQL: {e}")

if buscar:
    os.environ['MYSQL_HOST'] = mysql_host
    os.environ['MYSQL_USER'] = mysql_user
    os.environ['MYSQL_PASSWORD'] = mysql_password
    os.environ['MYSQL_DB'] = mysql_db
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)
        sql = (
            "SELECT DATA_JOGO, TIME_CASA, TIME_FORA, PAIS, LIGA, "
            "GOLS_CASA, GOLS_FORA, PROB_OVER_1_5, PROB_OVER_2_5, PROB_BTTS, MEDIA_PROB "
            "FROM jogos "
            "WHERE DATA_JOGO = %s AND GOLS_CASA IS NOT NULL AND GOLS_FORA IS NOT NULL AND MEDIA_PROB IS NOT NULL "
            "ORDER BY TIME_CASA, TIME_FORA"
        )
        cursor.execute(sql, (selected_date,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if not rows:
            st.info("Nenhum jogo com resultados encontrado para a data selecionada.")
        else:
            df = pd.DataFrame(rows)
            # Calcular condições dos resultados
            df['TOTAL_GOLS'] = (df['GOLS_CASA'].fillna(0)).astype(int) + (df['GOLS_FORA'].fillna(0)).astype(int)
            df['HIT_OVER15'] = df['TOTAL_GOLS'] >= 2
            df['HIT_OVER25'] = df['TOTAL_GOLS'] >= 3
            df['HIT_BTTS'] = (df['GOLS_CASA'].fillna(0)).astype(int) > 0
            df['HIT_BTTS'] = df['HIT_BTTS'] & ((df['GOLS_FORA'].fillna(0)).astype(int) > 0)

            # Organizar exibição
            display_cols = [
                'DATA_JOGO', 'TIME_CASA', 'TIME_FORA', 'PAIS', 'LIGA',
                'GOLS_CASA', 'GOLS_FORA',
                'PROB_OVER_1_5', 'PROB_OVER_2_5', 'PROB_BTTS', 'MEDIA_PROB'
            ]
            df_display = df[display_cols].copy()

            # Formatar porcentagens
            for pcol in ['PROB_OVER_1_5', 'PROB_OVER_2_5', 'PROB_BTTS', 'MEDIA_PROB']:
                if pcol in df_display.columns:
                    df_display[pcol] = pd.to_numeric(df_display[pcol], errors='coerce')

            # Funções de estilo (usar índice para consultar os HIT_*)
            def style_over15(row):
                hit = bool(df.loc[row.name, 'HIT_OVER15'])
                return ['background-color: #d4edda; color: #155724' if hit else 'background-color: #f8d7da; color: #721c24']

            def style_over25(row):
                hit = bool(df.loc[row.name, 'HIT_OVER25'])
                return ['background-color: #d4edda; color: #155724' if hit else 'background-color: #f8d7da; color: #721c24']

            def style_btts(row):
                hit = bool(df.loc[row.name, 'HIT_BTTS'])
                return ['background-color: #d4edda; color: #155724' if hit else 'background-color: #f8d7da; color: #721c24']

            styler = df_display.style.format({
                'PROB_OVER_1_5': '{:.0f}%',
                'PROB_OVER_2_5': '{:.0f}%',
                'PROB_BTTS': '{:.0f}%',
                'MEDIA_PROB': '{:.0f}%'
            })
            # Aplicar cores com base nos resultados
            styler = styler.apply(style_over15, axis=1, subset=['PROB_OVER_1_5'])
            styler = styler.apply(style_over25, axis=1, subset=['PROB_OVER_2_5'])
            styler = styler.apply(style_btts, axis=1, subset=['PROB_BTTS'])

            st.markdown("### Jogos com Resultados e Probabilidades")
            st.dataframe(styler, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao buscar resultados: {e}")