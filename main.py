import pandas as pd
from tabula import read_pdf
import re

def clean_cell(cell):
    if isinstance(cell, str):
        return cell.strip()
    return cell

def is_valid_date(date_string):
    try:
        pd.to_datetime(date_string, format='%d/%m/%Y')
        return True
    except ValueError:
        return False

def make_unique_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns = cols
    return df

def is_new_table_title(text):
    # Verifica se o texto inicia com "CARDÁPIO" e termina com um dos tipos de refeição
    if isinstance(text, str):  # Verifica se text é uma string
        pattern = r'^CARDÁPIO.*(ALMOÇO|JANTAR|DESJEJUM)$'
        return re.match(pattern, text) is not None
    return False

def extract_text_from_page(pdf_path, page_number):
    # Extrai o texto da página para verificar se há um título
    text = read_pdf(pdf_path, pages=page_number, multiple_tables=False, pandas_options={'header': None})
    if text:
        return text[0].iloc[0, 0]  # Retorna o texto da primeira célula
    return ""

def process_pdf_to_data(pdf_path):
    try:
        # Lê todas as tabelas do PDF
        dfs = read_pdf(pdf_path, pages='all', multiple_tables=True, lattice=True)

        all_data = []  # Lista para armazenar todas as tabelas processadas
        current_table = None  # Tabela atual
        current_title = ""  # Título atual da tabela

        for i in range(len(dfs)):
            df = dfs[i]
            print(f"\nTabela {i + 1} extraída do PDF:")
            print(df.head())  # Exibe as primeiras linhas de cada tabela extraída

            # Limpa os dados
            df = df.apply(lambda col: col.map(clean_cell) if col.dtype == 'object' else col)

            # Garante nomes de colunas únicos
            df = make_unique_columns(df)

            # Extrai o texto da página atual para verificar se há um título
            title_from_page = extract_text_from_page(pdf_path, i + 1)  # +1 porque as páginas começam em 1
            if is_new_table_title(title_from_page):  # Se é um título de nova tabela
                if current_table is not None:
                    # Adiciona a tabela atual à lista antes de começar uma nova
                    all_data.append(current_table)

                # Atualiza a tabela atual com a nova
                df.columns = df.iloc[0]  # Define a primeira linha como os nomes das colunas
                current_table = df[1:]  # Remove a linha do título
                current_title = title_from_page  # Atualiza o título atual
            else:
                # Se não é um novo título, continua a tabela anterior
                if current_table is not None:
                    # Adiciona a nova tabela à tabela atual
                    df.columns = current_table.columns  # Alinha as colunas
                    current_table = pd.concat([current_table, df], ignore_index=True)

            # Verifica se a primeira coluna é uma data válida e remove linhas inválidas
            if current_table is not None:
                current_table = current_table[current_table[current_table.columns[0]].apply(is_valid_date)]

                # Preenche valores NaN com 'N/A' para exibição
                current_table.fillna('N/A', inplace=True)

                # Adiciona uma coluna de refeição e título
                current_table.insert(0, 'Refeição', 'CARDÁPIO DE MARIO 2024 – UFV FLORESTAL – ALMOÇO')
                current_table.insert(1, 'Título', current_title)  # Adiciona o título da tabela

                # Reset index
                current_table.reset_index(drop=True, inplace=True)

        # Após o loop, adicione a última tabela se existir
        if current_table is not None:
            all_data.append(current_table)

        # Verifica se all_data não está vazio antes de concatenar
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df.drop_duplicates(inplace=True)  # Remove duplicatas, se necessário
            return final_df
        else:
            print("Nenhuma tabela foi extraída do PDF.")
            return pd.DataFrame()  # Retorna um DataFrame vazio

    except Exception as e:
        print(f"Ocorreu um erro ao ler o PDF: {e}")

# Exemplo de uso:
if __name__ == "__main__":
    pdf_path = '10c578b7-6c83-44bb-8680-c51b869ad8a2_ilovepdf-merged--1-.pdf'  # Substitua pelo caminho do seu PDF
    result_df = process_pdf_to_data(pdf_path)
    print("\nDataFrame final:")
    print(result_df)
