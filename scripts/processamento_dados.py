import json
import csv

class Dados:
    
    def __init__(self, dados) -> None:
        self.dados = dados
        self.nomes_colunas = self.__get_columns()
        self.qtd_linhas = self.__size_data()
        
    def __leitura_json(path):
        """
        Função para ler um arquivo JSON e retornar os dados como uma lista de dicionários.

        Returns:
            list: Os dados do arquivo JSON como uma lista de dicionários.
        """
        dados_json = []
        with open(path, 'r') as file:
            dados_json = json.load(file)
        return dados_json

    def __leitura_csv(path):
        """
        Função que lê um arquivo CSV e retorna os dados como uma lista de dicionários.

        Returns:
            list: Uma lista de dicionários contendo os dados do arquivo CSV.
        """
        dados_csv = []
        with open(path, 'r', encoding='utf-8-sig') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                dados_csv.append(row)

        return dados_csv

    @classmethod
    def leitura_dados(cls, path, tipo_dados):
        """
        Lê os dados de um arquivo e retorna uma instância da classe com os dados.

        Args:
            path (str): O caminho para o arquivo.
            tipo_dados (str): O tipo de dados a ser lido ('csv' ou 'json').

        Returns:
            cls: Uma instância da classe com os dados lidos.
        """
        dados = []

        if tipo_dados == 'csv':
            dados = cls.__leitura_csv(path)
        
        elif tipo_dados == 'json':
            dados = cls.__leitura_json(path)

        return cls(dados)

    def __get_columns(self):
        """
        Obtém a lista de nomes das colunas a partir dos dados fornecidos.

        Returns:
            list: Uma lista de nomes das colunas.
        """
        return list(self.dados[-1].keys())

    def rename_columns(self, key_mapping):
        """
        Renomeia as colunas do conjunto de dados com base em um mapeamento de chaves fornecido.

        Args:
            key_mapping (dict): Um dicionário que mapeia os nomes antigos das colunas para os novos nomes das colunas.

        Returns:
            list[dict]: O conjunto de dados com as colunas renomeadas.
        """
        new_dados = []

        for old_dict in self.dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_dados.append(dict_temp)
        
        self.dados = new_dados
        self.nomes_colunas = self.__get_columns()
    
    def __size_data(self):
        """
        Retorna o tamanho dos dados fornecidos.

        Returns:
            int: O tamanho dos dados.
        """
        return len(self.dados)
    
    def join(dadosA, dadosB):
        """
        Junta duas listas e retorna a lista combinada.

        Args:
            dadosA (Dados): A primeira lista a ser juntada.
            dadosB (Dados): A segunda lista a ser juntada.

        Returns:
            Dados: A lista combinada de dadosA e dadosB.
        """
        lista_combinada = []
        lista_combinada.extend(dadosA.dados)
        lista_combinada.extend(dadosB.dados)
        return Dados(lista_combinada)
    
    def __transformando_dados_tabela(self):
        """
        Transforma os dados fornecidos em um formato de tabela.

        Returns:
            list[list]: Os dados transformados em formato de tabela.
        """
        dados_combinados_tabela = [self.nomes_colunas]

        for row in self.dados:
            linha = []
            for coluna in self.nomes_colunas:
                linha.append(row.get(coluna, 'Indisponivel'))
            dados_combinados_tabela.append(linha)
        
        return dados_combinados_tabela
    
    def salvando_dados(self, path):
        """
        Salva os dados fornecidos em um arquivo csv.

        Args:
            path (str): O caminho do arquivo para salvar os dados.

        Returns:
            None
        """
        dados_combinados_tabela = self.__transformando_dados_tabela()
        with open(path, 'w', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerows(dados_combinados_tabela)