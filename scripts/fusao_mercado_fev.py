import json
import csv

def leitura_json(path_json):
    """
    Função para ler um arquivo JSON e retornar os dados como uma lista de dicionários.

    Args:
        path_json (str): O caminho do arquivo JSON a ser lido.

    Returns:
        list: Os dados do arquivo JSON como uma lista de dicionários.
    """
    dados_json = []
    with open(path_json, 'r') as file:
        dados_json = json.load(file)
    return dados_json

def leitura_csv(path_csv):
    """
    Função que lê um arquivo CSV e retorna os dados como uma lista de dicionários.

    Args:
        path_csv (str): O caminho do arquivo CSV a ser lido.

    Returns:
        list: Uma lista de dicionários contendo os dados do arquivo CSV.
    """
    dados_csv = []
    with open(path_csv, 'r', encoding='utf-8-sig') as file:
        spamreader = csv.DictReader(file, delimiter=',')
        for row in spamreader:
            dados_csv.append(row)

    return dados_csv

def leitura_dados(path, tipo_arquivo):
    """
    Função para ler dados de um arquivo.

    Args:
        path (str): O caminho do arquivo.
        tipo_arquivo (str): O tipo de arquivo ('csv' ou 'json').

    Returns:
        list: Os dados lidos do arquivo.
    """
    dados = []

    if tipo_arquivo == 'csv':
        dados = leitura_csv(path)
    
    elif tipo_arquivo == 'json':
        dados = leitura_json(path)

    return dados

def get_columns(dados):
    """
    Obtém a lista de nomes das colunas a partir dos dados fornecidos.

    Args:
        dados (list): Uma lista de dicionários representando os dados.

    Returns:
        list: Uma lista de nomes das colunas.
    """
    return list(dados[-1].keys())

def rename_columns(dados, key_mapping):
    """
    Renomeia as colunas de um conjunto de dados com base em um mapeamento de chaves fornecido.

    Args:
        dados (list[dict]): O conjunto de dados a ser processado, onde cada elemento é um dicionário representando uma linha.
        key_mapping (dict): Um dicionário que mapeia os nomes antigos das colunas para os novos nomes das colunas.

    Returns:
        list[dict]: O conjunto de dados com as colunas renomeadas.
    """
    new_dados_csv = []

    for old_dict in dados:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_dados_csv.append(dict_temp)
    
    return new_dados_csv

def size_data(dados):
    """
    Retorna o tamanho dos dados fornecidos.

    Parâmetros:
    dados (list): Os dados para calcular o tamanho.

    Retorna:
    int: O tamanho dos dados.
    """
    return len(dados)

def join(dadosA, dadosB):
    """
    Junta duas listas e retorna a lista combinada.

    Args:
        dadosA (list): A primeira lista a ser juntada.
        dadosB (list): A segunda lista a ser juntada.

    Returns:
        list: A lista combinada de dadosA e dadosB.
    """
    lista_combinada = []
    lista_combinada.extend(dadosA)
    lista_combinada.extend(dadosB)
    return lista_combinada

def transformando_dados_tabela(dados, nomes_colunas):
    """
    Transforma os dados fornecidos em um formato de tabela.

    Args:
        dados (list[dict]): Os dados a serem transformados.
        nomes_colunas (list[str]): Os nomes das colunas na tabela.

    Returns:
        list[list]: Os dados transformados em formato de tabela.
    """
    dados_combinados_tabela = [nomes_colunas]

    for row in dados:
        linha = []
        for coluna in nomes_colunas:
            linha.append(row.get(coluna, 'Indisponivel'))
        dados_combinados_tabela.append(linha)
    
    return dados_combinados_tabela

def salvando_dados(dados, path):
    """
    Salva os dados fornecidos em um arquivo.

    Args:
        data (list): Os dados a serem salvos.
        path (str): O caminho do arquivo para salvar os dados.

    Returns:
        None
    """
    with open(path, 'w', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        writer.writerows(dados)

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'


# Iniciando a leitura
dados_json = leitura_dados(path_json,'json')
nome_colunas_json = get_columns(dados_json)
tamanho_dados_json = size_data(dados_json)

print(f"Nome colunas dados json: {nome_colunas_json}")
print(f"Tamanho dos dados json: {tamanho_dados_json}")

dados_csv = leitura_dados(path_csv, 'csv')
nome_colunas_csv = get_columns(dados_csv)
tamanho_dados_csv = size_data(dados_csv)

print(f'Nome colunas dados csv: {nome_colunas_csv}')
print(f'Tamanho dos dados csv: {tamanho_dados_csv}')

# Transformacao dos dados

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

dados_csv = rename_columns(dados_csv, key_mapping)
nome_colunas_csv = get_columns(dados_csv)
print(f'Nome das colunas csv: {nome_colunas_csv}')

dados_fusao = join(dados_json, dados_csv)
nome_colunas_fusao = get_columns(dados_fusao)
tamanho_dados_fusao = size_data(dados_fusao)
print(f'Nome das colunas fusao: {nome_colunas_fusao}')
print(f'Tamanho dos dados fusao: {tamanho_dados_fusao}')


# Salvando dados

dados_fusao_tabela = transformando_dados_tabela(dados_fusao, nome_colunas_fusao)

path_dados_combinados = 'data_processed/dados_combinados.csv'

salvando_dados(dados_fusao_tabela, path_dados_combinados)

print(path_dados_combinados)
print(f'Dados salvos com sucesso em: {path_dados_combinados}')