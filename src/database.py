import os
import pytz
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import re

# Preferência: mysql.connector
import mysql.connector
from mysql.connector import Error

# Raspagem existente
from src.scraper_soccerstats import get_today_games

TIMEZONE_TARGET = 'America/Sao_Paulo'
EXCEL_PATH = "data/Jogos_de_Hoje.xlsx"
STOPWORDS_PREFIXES = {
    'fc', 'club', 'cf', 'ac', 'sc', 'sd', 'cd', 'ud', 'fk', 'sk',
    'al', 'el', 'de', 'da', 'do', 'la', 'las', 'los', 'sv', 'if', 'afc'
}
# Sufixos ou marcadores de gênero comuns
STOPWORDS_SUFFIXES = {
    'w', 'women', 'fem', 'femenino', 'ladies'
}
# Categorias por idade / times B/II comumente geram variações
CATEGORY_TOKENS = {
    'u15', 'u16', 'u17', 'u18', 'u19', 'u20', 'u21', 'u23',
    'reserves', 'reserve', 'b', 'ii'
}

# -------------------------------------------------------------
# Funções replicadas de processamento (sem Streamlit)
# -------------------------------------------------------------

def limpar_e_converter_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa '%' das colunas de porcentagem, converte vírgula para ponto e padroniza para float."""
    perc_cols = [
        col for col in df.columns
        if col.startswith(('Over', 'BTTS')) and ('H' in col or 'A' in col)
    ]

    # Remove '%' das colunas de porcentagem
    for col in perc_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('%', '', regex=False)

    # Lista de colunas que podem ter decimais com vírgula
    num_cols = set(perc_cols + [
        'Media_Gols_Casa', 'MediaGols_Fora', 'PPG_Casa', 'PPG_Fora',
        'Gols_Marcados_Casa', 'Gols_Marcados_Fora',
        'Vitorias_A', 'Vitorias_H'
    ])

    # Converte vírgula para ponto e transforma em numérico
    for col in num_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Remove linhas com NaN nas colunas de porcentagem após conversão
    df.dropna(subset=[c for c in perc_cols if c in df.columns], inplace=True)
    return df


def calcular_probabilidades(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona colunas com probabilidade de Over 1.5, Over 2.5 e BTTS, e MÉDIA_PROB."""
    # 1. Cálculos de Médias Simples
    if 'Over15_H' in df.columns and 'Over15_A' in df.columns:
        df['Prob_Over1.5'] = ((df['Over15_H'] + df['Over15_A']) / 2).round(2)
        df['Over15_MEDIA'] = df['Prob_Over1.5']
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


# -------------------------------------------------------------
# Carregamento de dados (cache simples via Excel, sem Streamlit)
# -------------------------------------------------------------

def load_and_process_data_cli() -> pd.DataFrame:
    tz_target = pytz.timezone(TIMEZONE_TARGET)
    data_de_hoje = datetime.now(tz_target).date()

    # 1) Tenta carregar do Excel se for de hoje
    if os.path.exists(EXCEL_PATH):
        data_modificacao_timestamp = os.path.getmtime(EXCEL_PATH)
        data_modificacao = datetime.fromtimestamp(data_modificacao_timestamp).date()
        if data_modificacao == data_de_hoje:
            df = pd.read_excel(EXCEL_PATH)
            if not df.empty:
                # Garante o processamento
                df = limpar_e_converter_dados(df)
                df = calcular_probabilidades(df)
                return df

    # 2) Caso contrário, raspa novamente
    df = get_today_games()
    df = limpar_e_converter_dados(df)
    df = calcular_probabilidades(df)

    # 3) Salva para reuso rápido
    if not os.path.exists('data'):
        os.makedirs('data')
    df.to_excel(EXCEL_PATH, index=False)

    return df


# -------------------------------------------------------------
# Preparação do DataFrame para Inserção em MySQL
# -------------------------------------------------------------

def prepare_df_for_insertion(df: pd.DataFrame) -> pd.DataFrame:
    tz_target = pytz.timezone(TIMEZONE_TARGET)
    data_de_hoje = datetime.now(tz_target).date()

    df_prep = df.copy()

    # Se vier uma DATA_JOGO do scraper, usa; caso contrário, data de hoje
    if 'DATA_JOGO' in df_prep.columns:
        df_prep['DATA_JOGO'] = pd.to_datetime(df_prep['DATA_JOGO'], errors='coerce').dt.date
        df_prep['DATA_JOGO'] = df_prep['DATA_JOGO'].fillna(data_de_hoje)
    else:
        df_prep['DATA_JOGO'] = data_de_hoje

    # Renomeios principais
    rename_map = {
        'Time 1': 'TIME_CASA',
        'Time 2': 'TIME_FORA',
        # Removido: não preencher gols na inserção inicial
        # 'Gols_Marcados_Casa': 'GOLS_CASA',
        # 'Gols_Marcados_Fora': 'GOLS_FORA',
        'Media_Gols_Casa': 'MEDIA_HOME',
        'MediaGols_Fora': 'MEDIA_AWAY',
    }

    for src, dst in rename_map.items():
        if src in df_prep.columns:
            df_prep[dst] = df_prep[src]
        else:
            # Se não existir, cria com None
            df_prep[dst] = None

    # Garante tipos
    for col in ['MEDIA_HOME', 'MEDIA_AWAY', 'Prob_Over1.5', 'Prob_Over2.5', 'Prob_BTTS', 'MÉDIA_PROB']:
        if col in df_prep.columns:
            df_prep[col] = pd.to_numeric(df_prep[col], errors='coerce')

    # Mapear contagens a partir de 'Partidas' (quando disponível)
    if 'Partidas' in df_prep.columns:
        df_prep['CONT_HOME'] = pd.to_numeric(df_prep['Partidas'], errors='coerce')
        df_prep['CONT_AWAY'] = pd.to_numeric(df_prep['Partidas'], errors='coerce')
    else:
        df_prep['CONT_HOME'] = None
        df_prep['CONT_AWAY'] = None

    # Mapear país do DF para a coluna PAIS do banco
    if 'País' in df_prep.columns:
        df_prep['PAIS'] = df_prep['País']
    else:
        df_prep['PAIS'] = None

    # Mantém apenas colunas necessárias para inserir
    keep_cols = [
        'DATA_JOGO', 'TIME_CASA', 'TIME_FORA',
        'MEDIA_HOME', 'MEDIA_AWAY',
        'Prob_Over1.5', 'Prob_Over2.5', 'Prob_BTTS', 'MÉDIA_PROB',
        'CONT_HOME', 'CONT_AWAY',
        'PAIS'  # novo: salvar país
    ]
    df_prep = df_prep[[col for col in keep_cols if col in df_prep.columns]]

    return df_prep


# -------------------------------------------------------------
# Conexão MySQL e Inserção
# -------------------------------------------------------------

def get_mysql_connection():
    load_dotenv()
    host = os.getenv('MYSQL_HOST', 'localhost')
    user = os.getenv('MYSQL_USER', 'SEU_USUARIO_MYSQL')
    password = os.getenv('MYSQL_PASSWORD', 'SUA_SENHA_MYSQL')
    database = os.getenv('MYSQL_DB', 'simulador-apostas')

    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            autocommit=False,
        )
        return conn
    except Error as e:
        raise RuntimeError(f"Erro ao conectar no MySQL: {e}")


def insert_df_into_mysql(df: pd.DataFrame, conn, log_file_path: str | None = None) -> int:
    """Insere linhas do DataFrame na tabela `jogos`. Retorna o número de registros inseridos."""
    if df is None or df.empty:
        return 0

    cursor = conn.cursor()

    def get_table_columns(connection, table: str) -> set[str]:
        cur = connection.cursor()
        try:
            cur.execute(f"SHOW COLUMNS FROM {table}")
            return {row[0] for row in cur.fetchall()}
        finally:
            cur.close()

    schema_cols = get_table_columns(conn, "jogos")

    dest_candidates: dict[str, list[str]] = {
        # DATA_JOGO deve vir da coluna DATA_JOGO (não usar Horário)
        'DATA_JOGO': ['DATA_JOGO'],
        'TIME_CASA': ['TIME_CASA', 'Time 1'],
        'TIME_FORA': ['TIME_FORA', 'Time 2'],
        'MEDIA_HOME': ['MEDIA_HOME', 'Media_Gols_Casa'],
        'MEDIA_AWAY': ['MEDIA_AWAY', 'MediaGols_Fora'],
        'PROB_OVER_1_5': ['PROB_OVER_1_5', 'Prob_Over1.5'],
        'PROB_OVER_2_5': ['PROB_OVER_2_5', 'Prob_Over2.5'],
        'PROB_BTTS': ['PROB_BTTS', 'Prob_BTTS'],
        'MEDIA_PROB': ['MEDIA_PROB', 'MÉDIA_PROB'],
        'CONT_HOME': ['CONT_HOME', 'Partidas'],
        'CONT_AWAY': ['CONT_AWAY', 'Partidas']
    }

    insert_cols = ['DATA_JOGO', 'TIME_CASA', 'TIME_FORA']
    optional_dests = [
        'MEDIA_HOME', 'MEDIA_AWAY',
        'PROB_OVER_1_5', 'PROB_OVER_2_5', 'PROB_BTTS', 'MEDIA_PROB',
        'CONT_HOME', 'CONT_AWAY', 'PAIS', 'LIGA',
    ]
    for dest in optional_dests:
        if dest in schema_cols and any(src in df.columns for src in dest_candidates.get(dest, [dest])):
            insert_cols.append(dest)

    insert_cols = [c for c in insert_cols if c in schema_cols]

    placeholders = ', '.join(['%s'] * len(insert_cols))
    columns_sql = ', '.join(insert_cols)
    sql = f"INSERT IGNORE INTO jogos ({columns_sql}) VALUES ({placeholders})"

    # Inicializa log de inserção
    log_f = None
    if log_file_path:
        try:
            dir_ = os.path.dirname(log_file_path)
            if dir_:
                os.makedirs(dir_, exist_ok=True)
            log_f = open(log_file_path, "a", encoding="utf-8")
            log_f.write(f"-- Início inserção: {datetime.now().isoformat()}\n")
            log_f.write(f"-- Schema cols: {sorted(schema_cols)}\n")
            log_f.write(f"-- Insert cols({len(insert_cols)}): {insert_cols}\n")
            log_f.write(f"INSERT INTO jogos ({columns_sql}) VALUES ({placeholders});\n")
        except Exception:
            log_f = None  # fallback silencioso se não conseguir abrir

    def pick_from_row(row: pd.Series, dest: str):
        candidates = dest_candidates.get(dest, [dest])
        for src in candidates:
            if src in row:
                val = row.get(src, None)
                if dest == 'DATA_JOGO':
                    # Aceitar date, Timestamp ou string de data; caso contrário, usar data atual
                    if isinstance(val, (pd.Timestamp, datetime)):
                        return val.date()
                    try:
                        # strings ou objetos date
                        parsed = pd.to_datetime(val, errors='coerce')
                        if not pd.isna(parsed):
                            return parsed.date()
                    except Exception:
                        pass
                    tz_target = pytz.timezone(TIMEZONE_TARGET)
                    return datetime.now(tz_target).date()
                if dest in ('CONT_HOME', 'CONT_AWAY') and src == 'Partidas':
                    return pd.to_numeric(val, errors='coerce')
                if dest in ('MEDIA_HOME', 'MEDIA_AWAY', 'PROB_OVER_1_5', 'PROB_OVER_2_5', 'PROB_BTTS', 'MEDIA_PROB'):
                    return pd.to_numeric(val, errors='coerce')
                return val
        # Fallback: DATA_JOGO vira hoje; numéricos/strings -> None
        if dest == 'DATA_JOGO':
            tz_target = pytz.timezone(TIMEZONE_TARGET)
            return datetime.now(tz_target).date()
        return None

    inserted = 0
    try:
        for idx, (_, row) in enumerate(df.iterrows()):
            values = []
            row_mapping = []

            for col in insert_cols:
                val = pick_from_row(row, col)

                # Normalização de valores antes do INSERT
                if isinstance(val, str):
                    val = val.strip()
                # Converter NaN para None
                try:
                    if pd.isna(val):
                        val = None
                except Exception:
                    pass
                # Garantir tipos para colunas decimais e inteiras
                if col in ['MEDIA_HOME', 'MEDIA_AWAY', 'PROB_OVER_1_5', 'PROB_OVER_2_5', 'PROB_BTTS', 'MEDIA_PROB']:
                    val = None if val is None else float(round(pd.to_numeric(val, errors='coerce'), 2))
                    if val is not None and pd.isna(val):
                        val = None
                if col in ['CONT_HOME', 'CONT_AWAY']:
                    val = None if val is None else int(pd.to_numeric(val, errors='coerce')) if not pd.isna(pd.to_numeric(val, errors='coerce')) else None
                if col == 'DATA_JOGO':
                    if isinstance(val, pd.Timestamp):
                        val = val.date()
                    elif isinstance(val, datetime):
                        val = val.date()

                values.append(val)
                used_src = next((src for src in dest_candidates.get(col, [col]) if src in row), col)
                row_mapping.append((col, used_src, val))

            if len(values) != len(insert_cols):
                if log_f:
                    log_f.write("-- DESALINHAMENTO DE VALORES NO INSERT\n")
                    log_f.write(f"-- Linha DF: {idx}\n")
                    log_f.write(f"-- colunas({len(insert_cols)}): {insert_cols}\n")
                    log_f.write(f"-- valores({len(values)}): {values}\n")
                    log_f.write(f"-- mapeamentos: {row_mapping}\n\n")
                raise RuntimeError(f"Alinhamento inválido: {len(values)} valores para {len(insert_cols)} colunas.")

            try:
                cursor.execute(sql, tuple(values))
                if log_f and cursor.rowcount > 0:
                    # Opcional: logar sucesso resumido
                    log_f.write(f"-- OK linha {idx}: rows={cursor.rowcount}\n")
            except Error as e:
                if log_f:
                    log_f.write("-- ERRO NO INSERT (execute falhou)\n")
                    log_f.write(f"-- Linha DF: {idx}\n")
                    log_f.write(f"-- colunas({len(insert_cols)}): {insert_cols}\n")
                    log_f.write(f"-- valores({len(values)}): {values}\n")
                    log_f.write(f"-- mapeamentos: {row_mapping}\n")
                    log_f.write(f"-- erro: {str(e)}\n\n")
                raise RuntimeError(f"Erro ao inserir dados: {e}")

            if cursor.rowcount > 0:
                inserted += cursor.rowcount
            else:
                updatable = [
                    'PAIS', 'MEDIA_PROB', 'PROB_OVER_1_5', 'PROB_OVER_2_5', 'PROB_BTTS',
                    'MEDIA_HOME', 'MEDIA_AWAY', 'CONT_HOME', 'CONT_AWAY', 'LIGA'
                ]
                update_fields = []
                update_params = []
                for field in updatable:
                    if field in insert_cols:
                        update_fields.append(f"{field} = %s")
                        update_params.append(pick_from_row(row, field))

                if update_fields:
                    update_sql = f"UPDATE jogos SET {', '.join(update_fields)} WHERE DATA_JOGO = %s AND TIME_CASA = %s AND TIME_FORA = %s"
                    dt_val = values[insert_cols.index('DATA_JOGO')]
                    casa_val = values[insert_cols.index('TIME_CASA')]
                    fora_val = values[insert_cols.index('TIME_FORA')]
                    cursor.execute(update_sql, tuple(update_params + [dt_val, casa_val, fora_val]))
                    if log_f:
                        log_f.write(f"-- UPDATE duplicata linha {idx}: rows={cursor.rowcount}\n")

        conn.commit()
        return inserted
    except Error as e:
        conn.rollback()
        raise RuntimeError(f"Erro ao inserir dados: {e}")
    finally:
        cursor.close()
        if log_f:
            log_f.write(f"-- Fim inserção: inseridos={inserted}, tentadas={len(df)}\n\n")
            log_f.close()


# -------------------------------------------------------------
# Execução completa (modular)
# -------------------------------------------------------------

def run_insertion_workflow(log_file_path: str | None = None) -> int:
    """Executa: carregar/raspar, processar, preparar e inserir no MySQL. Retorna total inserido."""
    df = load_and_process_data_cli()
    df_ready = prepare_df_for_insertion(df)

    conn = get_mysql_connection()
    try:
        total = insert_df_into_mysql(df_ready, conn, log_file_path=log_file_path)
        return total
    finally:
        conn.close()


def run_results_update_workflow(
    csv_path: str = "resultados_futebol_hoje.csv",
    log_file_path: str | None = None,
    fallback_like: bool = True,
    remove_prefixes: bool = True,
    remove_suffixes: bool = True,
    remove_categories: bool = True
) -> int:
    """Abre conexão e roda o UPDATE de resultados a partir de um CSV local, com fallback por LIKE e normalização configuráveis."""
    conn = get_mysql_connection()
    try:
        return upsert_results_from_csv(
            csv_path,
            conn,
            log_file_path=log_file_path,
            fallback_like=fallback_like,
            remove_prefixes=remove_prefixes,
            remove_suffixes=remove_suffixes,
            remove_categories=remove_categories
        )
    finally:
        conn.close()


def upsert_results_from_csv(
    csv_path: str,
    conn,
    log_file_path: str | None = None,
    fallback_like: bool = True,
    remove_prefixes: bool = True,
    remove_suffixes: bool = True,
    remove_categories: bool = True
) -> int:
    """Atualiza resultados (gols) do CSV na tabela `jogos` usando UPDATE.
    Fluxo: tenta UPDATE exato; se rows=0 e fallback_like=True, tenta localizar match único via LIKE e atualiza por ID.
    A normalização pode remover prefixos (FC/Club), sufixos (W/Women) e categorias (U17/U19/U23) para aumentar match."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

    df = pd.read_csv(csv_path)
    df = df.rename(columns={
        'Data': 'DATA_JOGO',
        'Time_Casa': 'TIME_CASA',
        'Time_Fora': 'TIME_FORA',
        'Gols_Casa': 'GOLS_CASA',
        'Gols_Fora': 'GOLS_FORA',
        'Status': 'STATUS',
        'Liga': 'LIGA'
    })

    # Considerar apenas partidas finalizadas
    status_col = df.get('STATUS')
    if status_col is not None:
        df['STATUS'] = status_col.astype(str).str.upper()
        df = df[df['STATUS'].isin(['FT', 'FINAL', 'FULL', 'AET', 'AP'])]

    # Converter tipos
    df['DATA_JOGO'] = pd.to_datetime(df['DATA_JOGO'], errors='coerce').dt.date
    for gcol in ['GOLS_CASA', 'GOLS_FORA']:
        df[gcol] = pd.to_numeric(df[gcol], errors='coerce')

    # Remover linhas inválidas
    df = df.dropna(subset=['DATA_JOGO', 'TIME_CASA', 'TIME_FORA', 'GOLS_CASA', 'GOLS_FORA'])

    if df.empty:
        return 0

    # Descobrir se a coluna LIGA existe no schema do banco
    has_liga = False
    try:
        tmp_cur = conn.cursor()
        tmp_cur.execute("SHOW COLUMNS FROM jogos")
        schema_cols = {row[0] for row in tmp_cur.fetchall()}
        has_liga = ('LIGA' in schema_cols)
    except Exception:
        has_liga = False
    finally:
        try:
            tmp_cur.close()
        except Exception:
            pass

    # Helpers de normalização e padrões LIKE (replicando lógica de tokens)
    def _normalize_keep_diacritics(name: str) -> str:
        s = str(name).lower().strip()
        # mantém diacríticos, espaços e separadores comuns, remove símbolos estranhos
        s = re.sub(r'[^0-9a-zA-ZÀ-ÿ\s/.\-]', ' ', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    def _tokenize_filtered(name: str) -> tuple[list[str], str]:
        base = _normalize_keep_diacritics(name)
        tokens = [t for t in re.split(r'[\s/.\-]+', base) if t]
        to_remove = set()
        if remove_prefixes:
            to_remove |= STOPWORDS_PREFIXES
        if remove_suffixes:
            to_remove |= STOPWORDS_SUFFIXES
        if remove_categories:
            to_remove |= CATEGORY_TOKENS
        filtered = [t for t in tokens if t not in to_remove]
        return filtered, base

    def _build_like_patterns(name: str) -> list[str]:
        filtered_tokens, base = _tokenize_filtered(name)

        patterns = []
        # Padrão com a string completa (como veio)
        if base:
            patterns.append(f"%{base}%")
        # Padrão com tokens filtrados (sem prefixos/sufixos/categorias)
        if filtered_tokens:
            name_filtered = ' '.join(filtered_tokens)
            patterns.append(f"%{name_filtered}%")
        # Padrão com coringa entre tokens filtrados
        if len(filtered_tokens) > 1:
            patterns.append('%' + '%'.join(filtered_tokens) + '%')

        # Deduplicar mantendo ordem
        return list(dict.fromkeys(patterns))

    def _find_unique_match_id(cur, dt, casa, fora, logf=None) -> int | None:
        patterns_casa = _build_like_patterns(casa)
        patterns_fora = _build_like_patterns(fora)

        def _run_like(p_tc: str, p_tf: str, reversed_order: bool = False) -> int | None:
            sql_sel = (
                "SELECT ID, TIME_CASA, TIME_FORA "
                "FROM jogos "
                "WHERE DATA_JOGO = %s AND LOWER(TIME_CASA) LIKE %s AND LOWER(TIME_FORA) LIKE %s"
            )
            cur.execute(sql_sel, (dt, p_tc, p_tf))
            rows = cur.fetchall()
            matches = len(rows)
            if logf:
                ordem = "" if not reversed_order else " (reversed)"
                logf.write(f"-- Fallback LIKE{ordem}: DATA_JOGO='{dt}', TC LIKE '{p_tc}', TF LIKE '{p_tf}'; matches={matches}\n")
            if matches == 1:
                return rows[0][0]  # ID
            return None

        # Ordem normal
        for p_tc in patterns_casa:
            for p_tf in patterns_fora:
                id_ = _run_like(p_tc, p_tf, reversed_order=False)
                if id_ is not None:
                    return id_
        # Ordem invertida (casos em que CSV troca mandante/visitante)
        for p_tc in patterns_fora:
            for p_tf in patterns_casa:
                id_ = _run_like(p_tc, p_tf, reversed_order=True)
                if id_ is not None:
                    return id_
        return None

    # Inicializa log (opcional)
    log_f = None
    if log_file_path:
        try:
            dir_ = os.path.dirname(log_file_path)
            if dir_:
                os.makedirs(dir_, exist_ok=True)
            log_f = open(log_file_path, "a", encoding="utf-8")
            log_f.write(f"-- Início UPDATE resultados: {datetime.now().isoformat()}\n")
            log_f.write(f"-- CSV: {csv_path}; fallback_like={fallback_like}; remove_prefixes={remove_prefixes}; remove_suffixes={remove_suffixes}; remove_categories={remove_categories}\n")
        except Exception:
            log_f = None  # fallback silencioso

    cursor = conn.cursor()
    processed = 0
    try:
        for _, row in df.iterrows():
            dt = row['DATA_JOGO']
            tc = str(row['TIME_CASA']).strip()
            tf = str(row['TIME_FORA']).strip()
            gc = int(row['GOLS_CASA'])
            gf = int(row['GOLS_FORA'])
            liga_val = None
            if 'LIGA' in df.columns and pd.notna(row.get('LIGA')):
                liga_val = str(row['LIGA']).strip()

            # 1) Tenta UPDATE exato (inclui LIGA se existir no schema e vier no CSV)
            update_fields = ["GOLS_CASA = %s", "GOLS_FORA = %s"]
            params = [gc, gf]
            if has_liga and liga_val is not None:
                update_fields.append("LIGA = %s")
                params.append(liga_val)

            sql_exact = f"UPDATE jogos SET {', '.join(update_fields)} WHERE DATA_JOGO = %s AND TIME_CASA = %s AND TIME_FORA = %s"
            cursor.execute(sql_exact, tuple(params + [dt, tc, tf]))
            rows_affected = cursor.rowcount
            if log_f:
                base_log = f"UPDATE jogos SET GOLS_CASA = {gc}, GOLS_FORA = {gf}"
                if has_liga and liga_val is not None:
                    base_log += f", LIGA = '{liga_val}'"
                log_f.write(
                    f"{base_log} WHERE DATA_JOGO = '{dt}' AND TIME_CASA = '{tc}' AND TIME_FORA = '{tf}'; -- rows={rows_affected}\n"
                )

            # 2) Fallback via LIKE procurando match único e atualizando por ID
            if rows_affected == 0 and fallback_like:
                match_id = _find_unique_match_id(cursor, dt, tc, tf, logf=log_f)
                if match_id is not None:
                    sql_by_id = f"UPDATE jogos SET {', '.join(update_fields)} WHERE ID = %s"
                    cursor.execute(sql_by_id, tuple(params + [match_id]))
                    rows_affected = cursor.rowcount
                    if log_f:
                        base_log = f"UPDATE jogos SET GOLS_CASA = {gc}, GOLS_FORA = {gf}"
                        if has_liga and liga_val is not None:
                            base_log += f", LIGA = '{liga_val}'"
                        log_f.write(f"{base_log} WHERE ID = {match_id}; -- rows={rows_affected}\n")
                else:
                    if log_f:
                        log_f.write(
                            f"-- Nenhum match unívoco por LIKE para: '{tc}' vs '{tf}' na data {dt}\n"
                        )

            processed += 1

        conn.commit()
        if log_f:
            log_f.write(f"-- Fim UPDATE: processadas={processed}\n\n")
        return processed
    except Error as e:
        conn.rollback()
        if log_f:
            log_f.write(f"-- ERRO UPDATE: {str(e)}\n\n")
        raise RuntimeError(f"Erro ao atualizar resultados: {e}")
    finally:
        cursor.close()
        if log_f:
            log_f.close()