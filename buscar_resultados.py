import requests
from datetime import datetime
import pandas as pd
import os

API_KEY = "19316383aa95e288fe9d50c14d4748d0"
BASE_URL = "https://v3.football.api-sports.io/"
ENDPOINT = "fixtures"

headers = {
    'x-rapidapi-key': API_KEY,
    'x-rapidapi-host': 'v3.football.api-sports.io'
}

def recreate_results_csv(csv_path: str = 'resultados_futebol_hoje.csv', date: str | None = None) -> int:
    """
    Recria o arquivo CSV de resultados para a data informada (ou hoje).
    Remove o arquivo antigo (se existir) e salva um novo com o mesmo nome.
    Retorna o número de jogos salvos no CSV.
    """
    # Controle de cota antes da requisição
    from src.quota import allow_request, remaining_quota_today
    limit = int(os.getenv("API_DAILY_LIMIT", "100"))
    if not allow_request("fixtures", max_per_day=limit):
        raise RuntimeError(f"Limite diário da API atingido ({limit}). Restante: {remaining_quota_today(limit)}")

    target_date = date or datetime.now().strftime('%Y-%m-%d')
    params = {"date": target_date}
    try:
        response = requests.get(BASE_URL + ENDPOINT, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        fixtures = data.get('response', [])

        if not fixtures:
            df = pd.DataFrame(columns=[
                'Data', 'Horário', 'Liga', 'Temporada', 'Time_Casa', 'Time_Fora',
                'Gols_Casa', 'Gols_Fora', 'Status'
            ])
        else:
            rows = []
            for fixture in fixtures:
                info_jogo = fixture['fixture']
                info_times = fixture['teams']
                info_score = fixture['score']
    
                data_hora_utc = datetime.strptime(info_jogo['date'], '%Y-%m-%dT%H:%M:%S%z')
                # Converte de UTC para América/São_Paulo para evitar +3h
                import pytz
                tz_sp = pytz.timezone('America/Sao_Paulo')
                data_hora_sp = data_hora_utc.astimezone(tz_sp)
                horario_local = data_hora_sp.strftime('%H:%M')
    
                placar_casa = info_score['fulltime']['home']
                placar_fora = info_score['fulltime']['away']

                rows.append({
                    'Data': target_date,
                    'Horário': horario_local,
                    'Liga': fixture['league']['name'],
                    'Temporada': fixture['league']['season'],
                    'Time_Casa': info_times['home']['name'],
                    'Time_Fora': info_times['away']['name'],
                    'Gols_Casa': placar_casa if placar_casa is not None else 'N/A',
                    'Gols_Fora': placar_fora if placar_fora is not None else 'N/A',
                    'Status': info_jogo['status']['short']
                })

            df = pd.DataFrame(rows)

        try:
            if os.path.exists(csv_path):
                os.remove(csv_path)
        except Exception:
            pass

        df.to_csv(csv_path, index=False, encoding='utf-8')
        return len(df)

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Erro na requisição à API: {e}")
    except Exception as e:
        raise RuntimeError(f"Ocorreu um erro ao recriar o CSV: {e}")

if __name__ == '__main__':
    total = recreate_results_csv('resultados_futebol_hoje.csv', date=datetime.now().strftime('%Y-%m-%d'))
    print(f"\n✅ Dados de {total} jogos salvos com sucesso em: {os.path.abspath('resultados_futebol_hoje.csv')}")