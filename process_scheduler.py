import os
import time
from datetime import datetime
from dotenv import load_dotenv

# Pipelines existentes
from src.database import run_insertion_workflow, run_results_update_workflow
from buscar_resultados import recreate_results_csv
from src.quota import remaining_quota_today

# Configurações
load_dotenv()

CYCLE_INTERVAL_SECONDS = int(os.getenv("SCHEDULER_INTERVAL_SECONDS", str(6 * 3600)))  # 6 horas
CSV_PATH = os.getenv("RESULTS_CSV_PATH", "resultados_futebol_hoje.csv")
API_DAILY_LIMIT = int(os.getenv("API_DAILY_LIMIT", "100"))
LOG_RESULTS_PATH = os.getenv("LOG_RESULTS_PATH", "logs/results_update.sql")
INSERT_LOG_PATH = os.getenv("LOG_INSERT_PATH", None)  # opcional, pode ser 'logs/insert.log'

def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")

def run_once() -> None:
    log("-" * 60)
    log("Iniciando ciclo: raspagem + inserção + atualização de resultados")

    # 1) Raspagem e inserção no MySQL
    try:
        total_inserted = run_insertion_workflow(log_file_path=INSERT_LOG_PATH)
        log(f"Raspagem/Inserção concluída. Registros inseridos/atualizados: {total_inserted}")
    except Exception as e:
        log(f"Erro em run_insertion_workflow: {e}")

    # 2) CSV de resultados via API (com cota diária)
    try:
        quota_restante = remaining_quota_today(API_DAILY_LIMIT)
        if quota_restante <= 0:
            log(f"Limite diário de API atingido ({API_DAILY_LIMIT}). Pulando geração do CSV.")
        else:
            total_csv = recreate_results_csv(csv_path=CSV_PATH)
            log(f"CSV recriado: {CSV_PATH}. Jogos no CSV: {total_csv}")
    except Exception as e:
        log(f"Erro ao recriar CSV de resultados: {e}")

    # 3) Atualizar resultados (GOLS/STATUS/LIGA) no MySQL
    try:
        total_updates = run_results_update_workflow(
            csv_path=CSV_PATH,
            log_file_path=LOG_RESULTS_PATH,
            fallback_like=True,       # tenta localizar por LIKE quando não encontra match exato
            remove_prefixes=True,     # normalização (ex.: 'FC', 'Club', etc.)
            remove_suffixes=True,     # normalização (ex.: 'W', 'Women', etc.)
            remove_categories=True    # normalização (ex.: 'U21', 'B', 'II', etc.)
        )
        log(f"Atualização de resultados concluída. Linhas processadas: {total_updates}")
    except Exception as e:
        log(f"Erro em run_results_update_workflow: {e}")

    # 4) Status de cota após ciclo
    try:
        quota_restante = remaining_quota_today(API_DAILY_LIMIT)
        log(f"Ciclo concluído. Cota restante da API hoje: {quota_restante}/{API_DAILY_LIMIT}")
    except Exception as e:
        log(f"Erro ao consultar cota restante: {e}")

    log("Fim do ciclo.")
    log("-" * 60)

def main():
    while True:
        try:
            run_once()
        except Exception as e:
            log(f"Erro inesperado no ciclo: {e}")
        # Aguarda para próximo ciclo
        time.sleep(CYCLE_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()