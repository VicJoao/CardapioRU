import pandas as pd
from tabula import read_pdf
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
import requests
import os

app = Flask(__name__)
CORS(app)

# Mapeia meses abreviados para números
months_map = {
    'jan': 1,
    'fev': 2,
    'mar': 3,
    'abr': 4,
    'mai': 5,
    'jun': 6,
    'jul': 7,
    'ago': 8,
    'set': 9,
    'out': 10,
    'nov': 11,
    'dez': 12
}

def clean_cell(cell):
    if isinstance(cell, str):
        return cell.replace('\r', '').strip()  # Remove '\r' e espaços
    return cell

def is_valid_date(date_string):
    try:
        day, month = date_string.split('/')
        month_num = months_map[month]  # Mapeia o mês abreviado para número
        pd.to_datetime(f"{day}/{month_num}/2024", format='%d/%m/%Y')  # Define um ano fixo (2024)
        return True
    except (ValueError, KeyError):
        return False

def is_today(date_string):
    try:
        day, month = date_string.split('/')
        month_num = months_map[month]
        return pd.to_datetime(f"{day}/{month_num}/{datetime.today().year}", format='%d/%m/%Y').date() == datetime.today().date()
    except (ValueError, KeyError):
        return False

def make_unique_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns = cols
    return df

def download_pdf(pdf_url, local_path):
    response = requests.get(pdf_url)
    with open(local_path, 'wb') as f:
        f.write(response.content)

def process_pdf_to_data(pdf_path):
    try:
        # Se o caminho for um URL, faça o download do PDF
        if pdf_path.startswith('http'):
            local_pdf_path = 'static/cardapio.pdf'
            download_pdf(pdf_path, local_pdf_path)
            pdf_path = local_pdf_path  # Atualiza o caminho para o local

        # Lê todas as tabelas do PDF
        dfs = read_pdf(pdf_path, pages='all', multiple_tables=True, lattice=True)

        meals = {
            "Café da Manhã": [],
            "Almoço": [],
            "Jantar": []
        }
        meal_index = 0

        for df in dfs:
            # Limpa os dados
            df = df.apply(lambda col: col.map(clean_cell) if col.dtype == 'object' else col)

            # Garante nomes de colunas únicos
            df = make_unique_columns(df)

            # Filtra apenas as linhas com a data de hoje
            df = df[df[df.columns[0]].apply(is_valid_date)]
            today_rows = df[df[df.columns[0]].apply(is_today)]

            # Organiza as refeições
            if not today_rows.empty:
                today_rows = today_rows.copy()
                today_rows.fillna('N/A', inplace=True)
                # Inicia o índice de refeições
                for i, row in today_rows.iterrows():
                    meal_index += 1
                    if meal_index == 1:
                        meals["Café da Manhã"].append(row.values[1:].tolist())
                    elif meal_index == 2:
                        meals["Almoço"].append(row.values[1:].tolist())
                    elif meal_index == 3:
                        meals["Jantar"].append(row.values[1:].tolist())

        return meals

    except Exception as e:
        print(f"Ocorreu um erro ao ler o PDF: {e}")
        return {}

@app.route('/api/meals', methods=['GET'])
def get_meals():
    pdf_path = 'https://cardapioru.onrender.com/static/cardapio.pdf'  # Caminho local para o PDF
    result_json = process_pdf_to_data(pdf_path)
    return jsonify(result_json)

if __name__ == "__main__":
    app.run(debug=True)
