import pandas as pd
import requests as r
import zipfile as z
import os
import io
import unicodedata
import shutil
from datetime import datetime


def renomear_arquivos_extraidos(dest_dir, ano, mes):
    """
    Renomeia arquivos extraídos do ZIP do BACEN conforme o padrão:
    - ranking_{ano}-{mes}_mensal.csv
    - ranking_{ano}-{mes}_acumulado.csv
    - ranking_{ano}-{mes}.xlsx
    """
    for nome_original in os.listdir(dest_dir):
        caminho_antigo = os.path.join(dest_dir, nome_original)
        nome_limpo = unicodedata.normalize('NFKD', nome_original).encode('ascii', 'ignore').decode('ascii').lower()

        if not os.path.isfile(caminho_antigo):
            continue

        ext = os.path.splitext(nome_original)[1].lower()
        novo_nome = None

        if "acumulado" in nome_limpo:
            novo_nome = f"ranking_{ano}-{mes}_acumulado{ext}"

        elif ext == '.csv':
            novo_nome = f"ranking_{ano}-{mes}_mensal{ext}"

        elif ext in ['.xlsx', '.xls']:
            novo_nome = f"ranking_{ano}-{mes}{ext}"

        if novo_nome:
            caminho_novo = os.path.join(dest_dir, novo_nome)
            try:
                os.rename(caminho_antigo, caminho_novo)
                print(f"✅ Renomeado: {nome_original} → {novo_nome}")
            except Exception as e:
                print(f"❌ Erro ao renomear {nome_original}: {e}")
        else:
            print(f"⚠️ Ignorado: {nome_original}")


# ==============================
# Configurações principais
# ==============================

ano_inicial = 2014
ano_atual = datetime.now().year
mes_atual = datetime.now().month

anos = [str(a) for a in range(ano_inicial, ano_atual + 1)]

base_dir = 'C:\\Users\\lucas\\PycharmProjects\\AlmeidaFX\\case_final'
os.makedirs(base_dir, exist_ok=True)

dest_dir = os.path.join(base_dir, 'zipfiles')
os.makedirs(dest_dir, exist_ok=True)

# ==============================
# Loop de download e extração
# ==============================
for ano in anos:
    for mes in range(1, 13):
        if int(ano) == ano_atual and mes > mes_atual:
            break

        mes = str(mes).zfill(2)
        print(f'\n📦 Baixando dados de {mes}/{ano}')

        padroes = [
            f'ESTATCABIF{ano}{mes}IF-{ano}{mes}.zip',
            f'ESTATCAMBIF{ano}{mes}IF-{ano}{mes}.zip',
            f'ESTATCAMBIF{ano}{mes}-IF-{ano}{mes}.zip',
            f'ESTATCAMBIF{ano}{mes}-{ano}{mes}.zip',
            f'ESTATCAMBIF{ano}{mes}-IF_{ano}{mes}.zip',
            f'ESTATCAMBIF{ano}{mes}-IF-{ano}-{mes}.zip',
            f'ESTATCAMBIF{ano}{mes}-AT_{ano}-{mes}.zip',
            f'ESTATCAMBIF{ano}{mes}-IF_{ano}{mes}.zip',
            f'ESTATCAMBIF201411-Ranking%20Institui%C3%A7%C3%A3o%202014%2011.xls'
        ]

        sucesso = False
        for padrao in padroes:
            url = f'https://www.bcb.gov.br/content/estatisticas/rankingcambioinstituicoes/{padrao}'
            a = r.get(url)

            if a.status_code == 200:
                print('✅ Sucesso ao baixar ZIP!')

                try:
                    # 🔹 CRIA PASTA TEMPORÁRIA para evitar sobrescrita
                    temp_dir = os.path.join(dest_dir, f"temp_{ano}_{mes}")
                    os.makedirs(temp_dir, exist_ok=True)

                    # Extrai dentro da pasta temporária
                    with z.ZipFile(io.BytesIO(a.content)) as zip_file_read:
                        zip_file_read.extractall(temp_dir)

                    # Renomeia os arquivos dentro da pasta temporária
                    renomear_arquivos_extraidos(temp_dir, ano, mes)

                    # Move os arquivos renomeados para o diretório final
                    for arquivo in os.listdir(temp_dir):
                        origem = os.path.join(temp_dir, arquivo)
                        destino = os.path.join(dest_dir, arquivo)
                        if not os.path.exists(destino):
                            shutil.move(origem, destino)
                        else:
                            print(f"⚠️ Já existe: {arquivo}, ignorando.")

                    # Remove a pasta temporária
                    shutil.rmtree(temp_dir)

                    sucesso = True
                    break

                except z.BadZipFile:
                    print('❌ Erro: arquivo não é um ZIP válido.')
                    break

            else:
                print(f'❌ Falhou: {url}')

        if sucesso:
            pd_set_path = f'{dest_dir}\\ranking_{ano}-{mes}.xlsx'
            try:
                df_raw = pd.read_excel(pd_set_path, header=5)
                print(f'📊 Dados carregados de {ano}-{mes}: {df_raw.shape[0]} linhas')
            except FileNotFoundError:
                print(f'⚠️ Arquivo não encontrado: {pd_set_path}')
        else:
            print(f'🚨 Erro ao baixar dados de {mes}/{ano}')
            log_path = os.path.join(base_dir, 'falhas_download.txt')
            with open(log_path, 'a') as log_file:
                log_file.write(f'{ano}-{mes}: {url}\n')

