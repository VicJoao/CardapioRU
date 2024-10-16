import pandas as pd
import pdfplumber
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
import requests
import logging
import os
import json
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
CORS(app)

# Configurando logging
logging.basicConfig(level=logging.INFO)

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

# Caminho para armazenar os dados pré-processados
DATA_FILE = 'preprocessed_meals.json'

meals_data = {}

def clean_cell(cell):
    """Remove caracteres indesejados de uma célula."""
    if isinstance(cell, str):
        return cell.replace('\r', '').strip()  # Remove '\r' e espaços
    return cell


def is_valid_date(date_string):
    """Verifica se a string da data está no formato válido."""
    try:
        day, month = date_string.split('/')
        month_num = months_map[month.lower()]  # Mapeia o mês abreviado para número
        pd.to_datetime(f"{day}/{month_num}/2024", format='%d/%m/%Y')  # Define um ano fixo (2024)
        return True
    except (ValueError, KeyError):
        return False


def is_today(date_string):
    """Verifica se a string da data é a data de hoje."""
    try:
        day, month = date_string.split('/')
        month_num = months_map[month.lower()]
        return pd.to_datetime(f"{day}/{month_num}/{datetime.today().year}",
                              format='%d/%m/%Y').date() == datetime.today().date()
    except (ValueError, KeyError):
        return False


def make_unique_columns(df):
    """Garante que os nomes das colunas no DataFrame sejam únicos."""
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in
                                                         range(sum(cols == dup))]
    df.columns = cols
    return df


def download_pdf(pdf_url, local_path):
    """Baixa um PDF de uma URL para um caminho local."""
    response = requests.get(pdf_url)
    with open(local_path, 'wb') as f:
        f.write(response.content)


def process_pdf_to_data(pdf_path):
    """Processa o PDF e extrai dados das refeições."""
    global meals_data
    meals_data = {
        "Café da Manhã": [],
        "Almoço": [],
        "Jantar": []
    }

    try:
        # Se o caminho for um URL, baixe o PDF
        if pdf_path.startswith('http'):
            local_pdf_path = 'static/cardapio.pdf'
            download_pdf(pdf_path, local_pdf_path)
            pdf_path = local_pdf_path  # Atualiza o caminho para o local

        with pdfplumber.open(pdf_path) as pdf:
            meal_index = 0

            for page in pdf.pages:
                # Extrai as tabelas da página
                tables = page.extract_tables()

                for table in tables:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    df = df.apply(lambda col: col.map(clean_cell) if col.dtype == 'object' else col)
                    df = make_unique_columns(df)

                    # Filtra apenas as linhas com a data de hoje
                    df = df[df[df.columns[0]].apply(is_valid_date)]
                    today_rows = df[df[df.columns[0]].apply(is_today)].copy()  # Adiciona .copy()

                    # Organiza as refeições
                    if not today_rows.empty:
                        today_rows = today_rows.fillna('N/A')  # Evita inplace
                        for i, row in today_rows.iterrows():
                            meal_index += 1
                            if meal_index == 1:
                                meals_data["Café da Manhã"].append(row.values[1:].tolist())
                            elif meal_index == 2:
                                meals_data["Almoço"].append(row.values[1:].tolist())
                            elif meal_index == 3:
                                meals_data["Jantar"].append(row.values[1:].tolist())

    except Exception as e:
        logging.error(f"Ocorreu um erro ao ler o PDF: {e}")
        return {}

    return meals_data


def update_meals_data():
    """Função agendada para atualizar os dados das refeições diariamente."""
    pdf_url = 'https://cardapioru.onrender.com/static/cardapio.pdf'  # URL do PDF
    logging.info("Iniciando o processamento diário do PDF.")
    data = process_pdf_to_data(pdf_url)
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info("Dados das refeições atualizados com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao salvar os dados das refeições: {e}")


@app.route('/api/meals', methods=['GET'])
def get_meals():
    """Endpoint da API para recuperar dados das refeições."""
    global meals_data
    if not meals_data and not os.path.exists(DATA_FILE):
        logging.info("Dados pré-processados não encontrados. Processando agora.")
        update_meals_data()

    if not meals_data:
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                meals_data = json.load(f)
        except Exception as e:
            logging.error(f"Erro ao ler os dados pré-processados: {e}")
            return jsonify({"error": "Não foi possível recuperar os dados das refeições."}), 500

    return jsonify(meals_data)


@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de verificação de saúde para confirmar que o serviço está em execução."""
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    # Configurar o scheduler
    scheduler = BackgroundScheduler()
    # Agendar a função para rodar diariamente às 00:00
    scheduler.add_job(update_meals_data, 'cron', hour=0, minute=0)
    scheduler.start()

    # Atualizar os dados imediatamente na inicialização
    update_meals_data()

    try:
        app.run(debug=True, port=int(os.getenv('PORT', 5000)))  # Usa o PORT da variável de ambiente ou padrão 5000
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
