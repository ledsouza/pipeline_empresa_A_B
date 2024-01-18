from processamento_dados import Dados

# Extract

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

dados_empresaA = Dados.leitura_dados(path_json, 'json')
dados_empresaB = Dados.leitura_dados(path_csv, 'csv')

print(f'Nome das colunas empresa A: {dados_empresaA.nomes_colunas}')
print(f'Tamanho dos dados empresa A: {dados_empresaA.qtd_linhas}')
print(f'Nome das colunas empresa B: {dados_empresaB.nomes_colunas}')
print(f'Tamanho dos dados empresa B: {dados_empresaB.qtd_linhas}')

# Transform

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

dados_empresaB.rename_columns(key_mapping)
print(f'Novo nome das colunas empresa B: {dados_empresaB.nomes_colunas}')

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(f'Nome das colunas após fusao: {dados_fusao.nomes_colunas}')
print(f'Tamanho dos dados após fusao: {dados_fusao.qtd_linhas}')

# Load

path_dados_combinados = 'data_processed/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)
print(f'Dados salvos com sucesso em: {path_dados_combinados}')