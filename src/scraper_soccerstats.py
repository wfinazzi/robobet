import pandas as pd
import requests

def get_today_games():
    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) appleWebKit/537.36 (KHTML, LIKE Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

    url_link1 = "https://www.soccerstats.com/matches.asp?matchday=1&listing=1"
    url_link2 = "https://www.soccerstats.com/matches.asp?matchday=1&listing=2"

    r1 = requests.get(url_link1, headers=header)
    r2 = requests.get(url_link2, headers=header)

    df1 = pd.read_html(r1.text)
    df2 = pd.read_html(r2.text)
    

    # Preparando a tabela 1
    jogos_today1 = df1[8]
    print(jogos_today1)
    jogos_today1 = jogos_today1[['Country','2.5+','1.5+','GA','GF','TG','PPG','scope', 
                                 'Unnamed: 10', 'Unnamed: 11','Unnamed: 12', 'scope.1', 
                                 'PPG.1', 'TG.1', 'GF.1', 'GA.1', '1.5+.1', '2.5+.1']]
    jogos_today1.columns = ['País', 'Over25_H', 'Over15_H', 'Gols_Sofridos_Casa', 'Gols_Marcados_Casa', 
                            'Media_Gols_Casa', 'PPG_Casa', 'Casa', 'Time 1', 'Horário', 'Time 2', 
                            'Fora', 'PPG_Fora', 'MediaGols_Fora', 'Gols_Marcados_Fora', 
                            'Gols_Sofridos_Fora', 'Over15_A', 'Over25_A']

    # Preparando a tabela 2
    jogos_today2 = df2[8]
    print("Chaves do dicionário:", jogos_today2.keys())
    jogos_today2 = jogos_today2[['BTS','W%','BTS.1','W%.1', 'GP']]
    jogos_today2.columns = ['BTTS_H', '%Vitorias_H', 'BTTS_A', '%Vitorias_A', 'Partidas']

    # Concatenando tabelas
    jogos_today = pd.concat([jogos_today1, jogos_today2], axis=1)
    jogos_today = jogos_today[['País','Partidas','Time 1','Time 2', 'Horário', '%Vitorias_H','%Vitorias_A', 
                               'Over15_H', 'Over25_H', 'Over15_A', 'Over25_A', 'BTTS_H', 'BTTS_A', 
                               'Gols_Marcados_Casa', 'Gols_Sofridos_Casa', 'Gols_Marcados_Fora', 
                               'Gols_Sofridos_Fora', 'Media_Gols_Casa', 'MediaGols_Fora', 'PPG_Casa','PPG_Fora']]

    # Tratamento de horários e porcentagens
    formato_correto = '%H:%M'
    jogos_today = jogos_today.sort_values('Horário').dropna()
    jogos_today['Horário'] = pd.to_datetime(jogos_today['Horário'], format=formato_correto) + pd.DateOffset(hours=8)
    jogos_today['Horário'] = pd.to_datetime(jogos_today['Horário'], format=formato_correto).dt.time
    jogos_today['Vitorias_A'] = jogos_today['%Vitorias_A'].str.replace('%','').astype("float")
    jogos_today['Vitorias_H'] = jogos_today['%Vitorias_H'].str.replace('%','').astype("float")
    # jogos_today['BTTS_H'] = jogos_today['BTTS_H'].str.replace('%', '').astype("float")
    # jogos_today['BTTS_A'] = jogos_today['BTTS_A'].str.replace('%', '').astype("float")

    # Resetar índice
    jogos_today.reset_index(inplace=True, drop=True)
    jogos_today.index = jogos_today.index.set_names(['Nº'])
    jogos_today = jogos_today.rename(index=lambda x: x + 1)

    # Salvar Excel
    jogos_today.to_excel("data/Jogos_de_Hoje.xlsx", index=False)

    return jogos_today