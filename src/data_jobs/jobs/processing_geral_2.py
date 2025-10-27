# ==============================================================================
# processing_geral_2.py
# FUNÇÕES DE LEVANTAMENTO DE DADOS PARA OS RELATORIOS (SERVIÇOS)
# Base: consolidado_cnes_serv.parquet
# ==============================================================================

import locale
import os
import re
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# --- CONFIGURAÇÃO DE AMBIENTE ---
# Adiciona o locale brasileiro para formatação de números
try:
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
        except locale.Error:
            locale.setlocale(locale.LC_ALL, 'C')
except (ImportError, locale.Error):
    pass


# ------------------------------------------------------------------------------
# --- DEFINIÇÕES DE AMBIENTE E CAMINHOS ---
# ------------------------------------------------------------------------------

# Define o diretório base subindo três níveis a partir do diretório atual
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', '..')
)

# CAMINHO DO NOVO ARQUIVO DE SERVIÇOS
CAMINHO_PARQUET_CNES_SERV = os.path.join(
    BASE_DIR, 'db', 'cnes', 'consolidado_cnes_serv.parquet'
)


# --- Mapeamento de UF para Região do País ---
# Reutilizado da versão anterior
UF_TO_REGIAO = {
    'ACRE': 'NORTE',
    'AMAPA': 'NORTE',
    'AMAZONAS': 'NORTE',
    'PARA': 'NORTE',
    'RONDONIA': 'NORTE',
    'RORAIMA': 'NORTE',
    'TOCANTINS': 'NORTE',
    'ALAGOAS': 'NORDESTE',
    'BAHIA': 'NORDESTE',
    'CEARA': 'NORDESTE',
    'MARANHAO': 'NORDESTE',
    'PARAIBA': 'NORDESTE',
    'PERNAMBUCO': 'NORDESTE',
    'PIAUI': 'NORDESTE',
    'RIO GRANDE DO NORTE': 'NORDESTE',
    'SERGIPE': 'NORDESTE',
    'ESPIRITO SANTO': 'SUDESTE',
    'MINAS GERAIS': 'SUDESTE',
    'RIO DE JANEIRO': 'SUDESTE',
    'SAO PAULO': 'SUDESTE',
    'PARANA': 'SUL',
    'RIO GRANDE DO SUL': 'SUL',
    'SANTA CATARINA': 'SUL',
    'DISTRITO FEDERAL': 'CENTRO-OESTE',
    'GOIAS': 'CENTRO-OESTE',
    'MATO GROSSO': 'CENTRO-OESTE',
    'MATO GROSSO DO SUL': 'CENTRO-OESTE',
}

# ------------------------------------------------------------------------------
# --- FUNÇÕES AUXILIARES DE GEOGRAFIA E PADRONIZAÇÃO (REUTILIZADAS) ---
# ------------------------------------------------------------------------------


def padronizar_nome_geografico(nome):
    """Remove acentos, caracteres especiais, e padroniza para UPPERCASE para filtros."""
    if pd.isna(nome) or nome is None:
        return ''
    nome = str(nome).upper().strip()

    # 1. Remove acentos e Ç/Ñ
    nome = (
        nome.replace('Á', 'A')
        .replace('À', 'A')
        .replace('Â', 'A')
        .replace('Ã', 'A')
        .replace('Ä', 'A')
        .replace('É', 'E')
        .replace('Ê', 'E')
        .replace('Ë', 'E')
        .replace('Í', 'I')
        .replace('Î', 'I')
        .replace('Ï', 'I')
        .replace('Ó', 'O')
        .replace('Ô', 'O')
        .replace('Õ', 'O')
        .replace('Ö', 'O')
        .replace('Ú', 'U')
        .replace('Ü', 'U')
        .replace('Û', 'U')
        .replace('Ç', 'C')
        .replace('Ñ', 'N')
    )

    # 2. Remove o código numérico inicial de Macrorregiões/Regiões de Saúde
    if re.match(r'^\d{3,4}[\-\s]', nome):
        nome = re.sub(r'^\d{3,4}[\-\s]', '', nome)

    # 3. Substitui *qualquer* caractere que não seja letra ou número ou espaço por espaço
    nome = re.sub(r'[^A-Z0-9\s]', ' ', nome)

    # 4. Colapsa múltiplos espaços em um único espaço
    nome = ' '.join(nome.split())

    return nome.strip()


def formatar_populacao(pop):
    """Formata o número usando separador de milhares brasileiro."""
    if pd.isna(pop) or pop is None:
        return '0'
    try:
        # Usa o locale pt_BR para formatação
        return locale.format_string('%d', int(pop), grouping=True)
    except Exception:
        # Fallback de formatação manual
        try:
            return (
                f'{int(pop):,}'.replace(',', '_TEMP_')
                .replace('.', ',')
                .replace('_TEMP_', '.')
            )
        except:
            return str(pop)


def get_descricao(nome):
    """Função auxiliar para extrair descrição legível (sem código) para CNES/Geografia."""
    nome = str(nome)

    # Remove códigos numéricos iniciais (ex: '0001 - NOME')
    if re.match(r'^\d{3,4}[\-\s]', nome):
        nome = re.sub(r'^\d{3,4}[\-\s]', '', nome).strip()

    # Lógica específica para Regiões de Saúde (remove prefixos como 'RRAS')
    if nome and ('RRAS' in nome.upper() or 'REGIAO DE SAUDE' in nome.upper()):
        try:
            partes = nome.split(' ', 3)
            if len(partes) > 3:
                return ' '.join(partes[3:]).title()

        except IndexError:
            pass

    # Remove números no final (muitas vezes códigos)
    if nome and nome.strip().endswith(tuple(str(i) for i in range(10))):
        parts = nome.rsplit(' ', 1)
        if len(parts) > 1 and parts[-1].isdigit():
            nome = parts[0]

    return nome.title() if nome else '-'


# ------------------------------------------------------------------------------
# --- FUNÇÃO DE MAPEAMENTO GERAL DE FILTROS (REUTILIZADA) ---
# ------------------------------------------------------------------------------


def mapear_selecao_geral(dados_selecao: Dict[str, str]) -> Dict[str, Any]:
    """
    Traduz os parâmetros de seleção do frontend para nomes de colunas internas
    e determina o nível de agregação final.
    """
    HIERARQUIA = [
        ('regiao', 'REGIAO', 'NO_REGIAO'),
        ('uf', 'UF', 'NO_UF'),
        (
            'macro',
            'MACRORREGIÃO',
            'NO_MACRO_REG_SAUDE',
        ),  # MACRORREGIÃO para evitar conflito
        ('regiaoSaude', 'REGIAO_SAUDE', 'NO_REGIAO_SAUDE'),
        (
            'municipio',
            'MUNICÍPIO',
            'NO_MUNICIPIO',
        ),  # MUNICÍPIO para evitar conflito
        ('unidade', 'TIPO_UNIDADE', 'DS_TIPO_UNIDADE'),
        ('cnes', 'CNES', 'CO_CNES'),
    ]

    filtros = {}
    nivel_agregacao = 'NACIONAL'

    for chave_frontend, nivel_nome, coluna_df in HIERARQUIA:
        valor_original = dados_selecao.get(chave_frontend)
        valor_padronizado = padronizar_nome_geografico(valor_original)

        if valor_original and valor_padronizado != 'TODOS':
            if nivel_nome == 'CNES':
                filtros['CO_CNES'] = valor_original
            else:
                filtros[coluna_df] = valor_padronizado

            nivel_agregacao = nivel_nome

    return {'NIVEL_AGREGACAO': nivel_agregacao, 'FILTROS': filtros}


# ------------------------------------------------------------------------------
# --- DADOS CNES -- SERVIÇOS (gerar_tabela_cnes_srv) ---
# ------------------------------------------------------------------------------

# Colunas brutas esperadas no Parquet de Serviços
PARQUET_COLUMNS_SERV_RAW = [
    'SERV_HABILITACAO',
    'UF_DESC',
    'DS_MACROREGIAO_ATEND',
    'DS_REGIAO_SAUDE_ATEND',
    'MUNICIPIO',
    'CNES',
    'NOME_FANTASIA',
    'TIPO_UNIDADE',
]

# Mapeamento dos nomes brutos para os nomes internos
PARQUET_COLUMN_SERV_RENAMING = {
    'CNES': 'CO_CNES',
    'UF_DESC': 'NO_UF',
    'DS_MACROREGIAO_ATEND': 'NO_MACRO_REG_SAUDE',
    'DS_REGIAO_SAUDE_ATEND': 'NO_REGIAO_SAUDE',
    'MUNICIPIO': 'NO_MUNICIPIO',
    'TIPO_UNIDADE': 'DS_TIPO_UNIDADE',
    'NOME_FANTASIA': 'NO_FANTASIA',
    'SERV_HABILITACAO': 'NO_SERVICO',  # Coluna chave de agregação
}

_DF_CNES_SERV_CACHE: Optional[pd.DataFrame] = None


def _carregar_base_cnes_srv() -> Optional[pd.DataFrame]:
    """Função interna para carregar e pré-processar a base de dados de Serviços."""
    global _DF_CNES_SERV_CACHE

    if _DF_CNES_SERV_CACHE is not None:
        return _DF_CNES_SERV_CACHE

    try:
        # 1. Carrega apenas as colunas necessárias com os nomes brutos
        df = pd.read_parquet(
            CAMINHO_PARQUET_CNES_SERV, columns=PARQUET_COLUMNS_SERV_RAW
        )

        # 2. Renomeia as colunas para os nomes internos
        df.rename(columns=PARQUET_COLUMN_SERV_RENAMING, inplace=True)

        # 3. Padronização e Conversão de Tipo
        COLUNAS_PARA_PADRONIZAR = [
            'NO_UF',
            'NO_MACRO_REG_SAUDE',
            'NO_REGIAO_SAUDE',
            'NO_MUNICIPIO',
            'DS_TIPO_UNIDADE',
            'NO_SERVICO',
            'NO_FANTASIA',
        ]

        for col in COLUNAS_PARA_PADRONIZAR:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .apply(padronizar_nome_geografico)
                    .astype('category')
                )

        # 4. ADIÇÃO CRÍTICA: Cria NO_REGIAO inferindo de NO_UF
        if 'NO_UF' in df.columns:
            uf_to_regiao_padronizado = {
                padronizar_nome_geografico(k): padronizar_nome_geografico(v)
                for k, v in UF_TO_REGIAO.items()
            }
            df['NO_REGIAO'] = (
                df['NO_UF']
                .map(uf_to_regiao_padronizado)
                .fillna('NAO IDENTIFICADO')
                .astype('category')
            )
        else:
            df['NO_REGIAO'] = 'NAO IDENTIFICADO'.astype('category')

        # 5. CNES
        df['CO_CNES'] = df['CO_CNES'].astype(str)

        _DF_CNES_SERV_CACHE = df
        return df

    except Exception as e:
        print(
            f'ERRO FATAL ao carregar CNES_SERV: {e}. Caminho: {CAMINHO_PARQUET_CNES_SERV}'
        )
        traceback.print_exc(file=sys.stdout)
        return None


def gerar_tabela_cnes_srv(
    dados_selecao: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Gera a tabela hierárquica de SERVIÇOS/CLASSIFICAÇÕES CNES,
    contando o número de CNES (Unidades) que possuem o serviço, respeitando o
    nível de detalhe do filtro (Nacional, UF, Município, etc.) e usando a
    formatação visual hierárquica com hífen (-).
    """
    df = _carregar_base_cnes_srv()

    if df is None or df.empty:
        print(
            '⚠️ Aviso: Dados de serviço CNES temporariamente indisponíveis (base vazia ou erro de carregamento).'
        )
        return []

    # Mapeamento do nível para o prefixo visual desejado
    # Este dicionário contém a string formatada para a coluna NIVEL
    NIVEL_FORMATADO = {
        'NACIONAL': 'NACIONAL',
        'REGIAO': 'REGIÃO',
        'UF': ' - UF',
        'MACRORREGIÃO': ' - - MACRORREGIÃO',
        'REGIAO_SAUDE': ' - - - REGIÃO DE SAÚDE',
        'MUNICÍPIO': ' - - - - MUNICÍPIO',
        'CNES': ' - - - - - CNES',
    }

    COL_SERVICO = 'NO_SERVICO'
    COL_AGREGACAO = 'CO_CNES'

    mapa_selecao = mapear_selecao_geral(dados_selecao)
    # Garante que o nome do nível do filtro corresponde à chave do dicionário (UF, MUNICÍPIO, etc.)
    nivel_selecionado = mapa_selecao.get('NIVEL_AGREGACAO', 'NACIONAL')
    filtros = mapa_selecao.get('FILTROS', {})

    df_trabalho = df.copy()

    # 1. Aplicação dos Filtros (inclusão de lógica de filtro parcial)
    if filtros:
        mascara = pd.Series(True, index=df_trabalho.index)

        for coluna, valor_padronizado in filtros.items():
            if coluna in df_trabalho.columns:
                # Filtros parciais para Macrorregião e Região de Saúde (devido à variabilidade de nome)
                if coluna in ['NO_MACRO_REG_SAUDE', 'NO_REGIAO_SAUDE']:
                    mascara &= (
                        df_trabalho[coluna]
                        .astype(str)
                        .str.contains(valor_padronizado, na=False)
                    )
                else:
                    mascara &= df_trabalho[coluna] == valor_padronizado

        df_trabalho = df_trabalho[mascara].reset_index(drop=True)

    if df_trabalho.empty:
        return []

    tabelas = []

    # Agrupa o dataframe de trabalho pelos Serviços para iterar
    df_grouped_servico = df_trabalho.groupby(COL_SERVICO, observed=True)

    # 2. Iteração por CADA SERVIÇO ENCONTRADO
    for servico, df_servico_filtrado in df_grouped_servico:

        # Cria a base de contagem (1 linha por CNES único para este serviço)
        df_base_contagem = df_servico_filtrado.drop_duplicates(
            subset=[COL_AGREGACAO]
        ).copy()
        df_base_contagem['QUANT_CNES'] = 1

        if df_base_contagem.empty:
            continue

        tabela = []
        tabela.append(['NIVEL', 'DESCRIÇÃO', 'QUANT'])

        # --- NÍVEL NACIONAL (Sempre o primeiro) ---
        quant_nacional = df[df[COL_SERVICO] == servico][
            COL_AGREGACAO
        ].nunique()
        tabela.append(
            [
                NIVEL_FORMATADO['NACIONAL'],
                'BRASIL',
                formatar_populacao(quant_nacional),
            ]
        )

        # --- LÓGICA DE DETALHE HIERÁRQUICO COM CORTE E PREFIXO ---

        # 1. Agrega por Região
        df_reg = (
            df_base_contagem.groupby('NO_REGIAO', observed=True)['QUANT_CNES']
            .sum()
            .reset_index(name='QUANT')
        )

        # 2. Itera a hierarquia (Região)
        for regiao_padronizada in df_reg['NO_REGIAO'].sort_values().unique():

            df_regiao_linha = df_reg[df_reg['NO_REGIAO'] == regiao_padronizada]
            if df_regiao_linha.empty:
                continue

            quant_regiao = df_regiao_linha['QUANT'].iloc[0]

            # Adiciona a Linha da REGIÃO
            tabela.append(
                [
                    NIVEL_FORMATADO['REGIAO'],
                    get_descricao(regiao_padronizada),
                    formatar_populacao(quant_regiao),
                ]
            )

            # Filtra a base para a REGIÃO atual
            df_base_regiao = df_base_contagem[
                df_base_contagem['NO_REGIAO'] == regiao_padronizada
            ]

            # Nível UF
            df_uf = (
                df_base_regiao.groupby('NO_UF', observed=True)['QUANT_CNES']
                .sum()
                .reset_index(name='QUANT')
            )
            for uf_padronizada in df_uf['NO_UF'].sort_values().unique():

                df_uf_linha = df_uf[df_uf['NO_UF'] == uf_padronizada]
                if df_uf_linha.empty:
                    continue

                quant_uf = df_uf_linha['QUANT'].iloc[0]

                # Adiciona a Linha da UF
                tabela.append(
                    [
                        NIVEL_FORMATADO['UF'],
                        get_descricao(uf_padronizada),
                        formatar_populacao(quant_uf),
                    ]
                )

                # 🚨 CORTE 1: Se o filtro é NACIONAL, para a iteração (sai do loop de UF e vai para a próxima REGIÃO)
                if nivel_selecionado == 'NACIONAL':
                    continue

                # Filtra a base para a UF atual
                df_base_uf = df_base_regiao[
                    df_base_regiao['NO_UF'] == uf_padronizada
                ]

                # Nível MACRORREGIÃO
                if 'NO_MACRO_REG_SAUDE' in df_base_uf.columns:
                    df_macro = (
                        df_base_uf.groupby(
                            'NO_MACRO_REG_SAUDE', observed=True
                        )['QUANT_CNES']
                        .sum()
                        .reset_index(name='QUANT')
                    )
                    for macro_padronizada in (
                        df_macro['NO_MACRO_REG_SAUDE'].sort_values().unique()
                    ):

                        df_macro_linha = df_macro[
                            df_macro['NO_MACRO_REG_SAUDE'] == macro_padronizada
                        ]
                        if df_macro_linha.empty:
                            continue

                        quant_macro = df_macro_linha['QUANT'].iloc[0]

                        # Adiciona a Linha da MACRORREGIÃO
                        tabela.append(
                            [
                                NIVEL_FORMATADO['MACRORREGIÃO'],
                                get_descricao(macro_padronizada),
                                formatar_populacao(quant_macro),
                            ]
                        )

                        # 🚨 CORTE 2: Se o filtro é REGIÃO, para a iteração (sai do loop de MACRORREGIÃO e vai para a próxima UF)
                        if nivel_selecionado == 'REGIAO':
                            continue

                        # Filtra a base para a MACRORREGIÃO atual
                        df_base_macro = df_base_uf[
                            df_base_uf['NO_MACRO_REG_SAUDE']
                            == macro_padronizada
                        ]

                        # Nível REGIÃO DE SAÚDE
                        if 'NO_REGIAO_SAUDE' in df_base_macro.columns:
                            df_rs = (
                                df_base_macro.groupby(
                                    'NO_REGIAO_SAUDE', observed=True
                                )['QUANT_CNES']
                                .sum()
                                .reset_index(name='QUANT')
                            )
                            for rs_padronizada in (
                                df_rs['NO_REGIAO_SAUDE'].sort_values().unique()
                            ):

                                df_rs_linha = df_rs[
                                    df_rs['NO_REGIAO_SAUDE'] == rs_padronizada
                                ]
                                if df_rs_linha.empty:
                                    continue

                                quant_rs = df_rs_linha['QUANT'].iloc[0]

                                # Adiciona a Linha da REGIÃO DE SAÚDE
                                tabela.append(
                                    [
                                        NIVEL_FORMATADO['REGIAO_SAUDE'],
                                        get_descricao(rs_padronizada),
                                        formatar_populacao(quant_rs),
                                    ]
                                )

                                # 🚨 CORTE 3: Se o filtro é UF, MACRORREGIÃO ou REGIAO_SAUDE, para a iteração
                                if nivel_selecionado in [
                                    'UF',
                                    'MACRORREGIÃO',
                                    'REGIAO_SAUDE',
                                ]:
                                    continue

                                # Filtra a base para a REGIÃO DE SAÚDE atual
                                df_base_rs = df_base_macro[
                                    df_base_macro['NO_REGIAO_SAUDE']
                                    == rs_padronizada
                                ]

                                # Nível MUNICÍPIO
                                if 'NO_MUNICIPIO' in df_base_rs.columns:
                                    df_mun = (
                                        df_base_rs.groupby(
                                            'NO_MUNICIPIO', observed=True
                                        )['QUANT_CNES']
                                        .sum()
                                        .reset_index(name='QUANT')
                                    )
                                    for mun_padronizado in (
                                        df_mun['NO_MUNICIPIO']
                                        .sort_values()
                                        .unique()
                                    ):

                                        df_mun_linha = df_mun[
                                            df_mun['NO_MUNICIPIO']
                                            == mun_padronizado
                                        ]
                                        if df_mun_linha.empty:
                                            continue

                                        quant_mun = df_mun_linha['QUANT'].iloc[
                                            0
                                        ]

                                        # Adiciona a Linha do MUNICÍPIO
                                        tabela.append(
                                            [
                                                NIVEL_FORMATADO['MUNICÍPIO'],
                                                get_descricao(mun_padronizado),
                                                formatar_populacao(quant_mun),
                                            ]
                                        )

                                        # Nível UNIDADE (CNES)
                                        # Detalhe de Unidade CNES só é exibido se o nível de filtro for MUNICÍPIO ou mais detalhado
                                        if nivel_selecionado in [
                                            'MUNICÍPIO',
                                            'TIPO_UNIDADE',
                                            'CNES',
                                        ]:
                                            df_base_mun = df_base_rs[
                                                df_base_rs['NO_MUNICIPIO']
                                                == mun_padronizado
                                            ]
                                            df_cnes = (
                                                df_base_mun.groupby(
                                                    ['CO_CNES', 'NO_FANTASIA'],
                                                    observed=True,
                                                )['QUANT_CNES']
                                                .sum()
                                                .reset_index(name='QUANT')
                                            )
                                            for (
                                                _,
                                                row_cnes,
                                            ) in df_cnes.iterrows():
                                                cnes = row_cnes['CO_CNES']
                                                nome_fantasia = row_cnes[
                                                    'NO_FANTASIA'
                                                ]
                                                quant_cnes = row_cnes['QUANT']

                                                # Adiciona a Linha da UNIDADE (CNES)
                                                descricao_unidade = (
                                                    get_descricao(
                                                        nome_fantasia
                                                    )
                                                )
                                                tabela.append(
                                                    [
                                                        NIVEL_FORMATADO[
                                                            'CNES'
                                                        ],
                                                        f'{descricao_unidade} ({cnes})',
                                                        formatar_populacao(
                                                            quant_cnes
                                                        ),
                                                    ]
                                                )

        # 3. ADICIONA TABELA NA LISTA DE RESULTADOS
        if len(tabela) > 2:  # Verifica se tem mais que cabeçalho + nacional
            tabelas.append({'tipo_habilitacao': servico, 'dados': tabela})

    return tabelas


# ------------------------------------------------------------------------------
# --- DADOS SIA - OCI (gerar_tabela_sia_oci) ---
# ------------------------------------------------------------------------------

# Colunas brutas esperadas no Parquet de OCI
PARQUET_COLUMNS_OCI_RAW = [
    'UF_DESC_ATEND',
    'DS_MACROREGIAO_ATEND',
    'DS_REGIAO_SAUDE_ATEND',
    'MUNICIPIO_ATEND',
    'CNES_ATEND',
    'NOME_UNIDADE_ATEND',
    'FORMA_REGISTRO_PROCEDIMENTOS',
    'SUBGRUPO_PROCEDIMENTO',
    'QUANT_APROV',
    'VALOR_APROV',
]

# Mapeamento dos nomes brutos para os nomes internos
PARQUET_COLUMN_OCI_RENAMING = {
    'CNES_ATEND': 'CO_CNES',
    'UF_DESC_ATEND': 'NO_UF',
    'DS_MACROREGIAO_ATEND': 'NO_MACRO_REG_SAUDE',
    'DS_REGIAO_SAUDE_ATEND': 'NO_REGIAO_SAUDE',
    'MUNICIPIO_ATEND': 'NO_MUNICIPIO',
    'NOME_UNIDADE_ATEND': 'NO_FANTASIA',
    'FORMA_REGISTRO_PROCEDIMENTOS': 'TP_REGISTRO',
    'SUBGRUPO_PROCEDIMENTO': 'NO_SUBGRUPO_PROCED',
    'QUANT_APROV': 'QUANT_APROV',
    'VALOR_APROV': 'VALOR_APROV',
}

_DF_SIA_OCI_CACHE: Optional[pd.DataFrame] = None


def _carregar_base_sia_oci() -> Optional[pd.DataFrame]:
    """Função interna para carregar e pré-processar a base de dados de OCI."""
    global _DF_SIA_OCI_CACHE

    CAMINHO_PARQUET_SIA_OCI = os.path.join(
        BASE_DIR, 'db', 'sia', 'consolidado_oci.parquet'
    )

    print(f'📁 Caminho do parquet OCI: {CAMINHO_PARQUET_SIA_OCI}')

    if _DF_SIA_OCI_CACHE is not None:
        print('✅ Usando cache de dados OCI')
        return _DF_SIA_OCI_CACHE

    try:
        # Verifica se o arquivo existe
        if not os.path.exists(CAMINHO_PARQUET_SIA_OCI):
            print(f'❌ Arquivo OCI não encontrado: {CAMINHO_PARQUET_SIA_OCI}')
            return None

        # 1. Carrega apenas as colunas necessárias com os nomes brutos
        df = pd.read_parquet(
            CAMINHO_PARQUET_SIA_OCI, columns=PARQUET_COLUMNS_OCI_RAW
        )

        print(f'✅ Parquet OCI carregado. Total de registros: {len(df)}')

        # 2. Renomeia as colunas para os nomes internos
        df.rename(columns=PARQUET_COLUMN_OCI_RENAMING, inplace=True)
        print(f'🎯 Mapeamento OCI aplicado: {PARQUET_COLUMN_OCI_RENAMING}')

        # 3. Filtra apenas registros PRINCIPAIS
        registros_antes = len(df)
        df = df[df['TP_REGISTRO'] == 'PRINCIPAL'].copy()
        registros_depois = len(df)
        print(
            f'📊 Filtro PRINCIPAL: {registros_antes} -> {registros_depois} registros'
        )

        # 4. Padronização e Conversão de Tipo
        COLUNAS_PARA_PADRONIZAR = [
            'NO_UF',
            'NO_MACRO_REG_SAUDE',
            'NO_REGIAO_SAUDE',
            'NO_MUNICIPIO',
            'NO_FANTASIA',
            'NO_SUBGRUPO_PROCED',
        ]

        print('🔄 Padronizando colunas OCI...')
        for col in COLUNAS_PARA_PADRONIZAR:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .apply(padronizar_nome_geografico)
                    .astype('category')
                )

        # 5. ADIÇÃO CRÍTICA: Cria NO_REGIAO inferindo de NO_UF
        if 'NO_UF' in df.columns:
            uf_to_regiao_padronizado = {
                padronizar_nome_geografico(k): padronizar_nome_geografico(v)
                for k, v in UF_TO_REGIAO.items()
            }
            df['NO_REGIAO'] = (
                df['NO_UF']
                .map(uf_to_regiao_padronizado)
                .fillna('NAO IDENTIFICADO')
                .astype('category')
            )
        else:
            df['NO_REGIAO'] = 'NAO IDENTIFICADO'

        # 6. CNES
        df['CO_CNES'] = df['CO_CNES'].astype(str)

        # 7. Garantir que as colunas numéricas são numéricas
        df['QUANT_APROV'] = pd.to_numeric(
            df['QUANT_APROV'], errors='coerce'
        ).fillna(0)
        df['VALOR_APROV'] = pd.to_numeric(
            df['VALOR_APROV'], errors='coerce'
        ).fillna(0)

        print(f'📊 Dados OCI processados - Colunas: {list(df.columns)}')
        print(
            f"🎯 Subgrupos de procedimento encontrados: {df['NO_SUBGRUPO_PROCED'].nunique()}"
        )
        print(
            f"📊 Amostra de subgrupos: {df['NO_SUBGRUPO_PROCED'].unique()[:5]}"
        )

        _DF_SIA_OCI_CACHE = df
        return df

    except Exception as e:
        print(
            f'❌ ERRO FATAL ao carregar SIA_OCI: {e}. Caminho: {CAMINHO_PARQUET_SIA_OCI}'
        )
        traceback.print_exc(file=sys.stdout)
        return None


def formatar_valor_monetario(valor):
    """Formata valores monetários com separador de milhares e 2 casas decimais."""
    if pd.isna(valor) or valor is None:
        return '0,00'
    try:
        # Usa o locale pt_BR para formatação monetária
        return locale.format_string('%.2f', float(valor), grouping=True)
    except Exception:
        # Fallback de formatação manual
        try:
            valor_float = float(valor)
            return (
                f'{valor_float:,.2f}'.replace(',', '_TEMP_')
                .replace('.', ',')
                .replace('_TEMP_', '.')
            )
        except:
            return str(valor)


def gerar_tabela_sia_oci(
    dados_selecao: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Gera a tabela hierárquica de OCI (Procedimentos Oncológicos),
    somando QUANT_APROV e VALOR_APROV, respeitando o nível de detalhe do filtro
    (Nacional, UF, Município, etc.) e usando a formatação visual hierárquica com hífen (-).
    Agrupa por SUBGRUPO_PROCEDIMENTO similar à função de serviços.
    """
    print('🔍 INICIANDO gerar_tabela_sia_oci...')

    df = _carregar_base_sia_oci()

    if df is None or df.empty:
        print(
            '⚠️ Aviso: Dados de OCI temporariamente indisponíveis (base vazia ou erro de carregamento).'
        )
        return []

    # Mapeamento do nível para o prefixo visual desejado
    NIVEL_FORMATADO = {
        'NACIONAL': 'NACIONAL',
        'REGIAO': 'REGIÃO',
        'UF': ' - UF',
        'MACRORREGIÃO': ' - - MACRORREGIÃO',
        'REGIAO_SAUDE': ' - - - REGIÃO DE SAÚDE',
        'MUNICÍPIO': ' - - - - MUNICÍPIO',
        'CNES': ' - - - - - CNES',
    }

    COL_SUBGRUPO = 'NO_SUBGRUPO_PROCED'

    mapa_selecao = mapear_selecao_geral(dados_selecao)
    nivel_selecionado = mapa_selecao.get('NIVEL_AGREGACAO', 'NACIONAL')
    filtros = mapa_selecao.get('FILTROS', {})

    print(f'🎯 Nível selecionado OCI: {nivel_selecionado}')
    print(f'🎯 Filtros aplicados OCI: {filtros}')

    df_trabalho = df.copy()
    print(f'📊 Registros antes do filtro OCI: {len(df_trabalho)}')

    # 1. Aplicação dos Filtros (inclusão de lógica de filtro parcial)
    if filtros:
        mascara = pd.Series(True, index=df_trabalho.index)

        for coluna, valor_padronizado in filtros.items():
            if coluna in df_trabalho.columns:
                # Filtros parciais para Macrorregião e Região de Saúde (devido à variabilidade de nome)
                if coluna in ['NO_MACRO_REG_SAUDE', 'NO_REGIAO_SAUDE']:
                    mascara &= (
                        df_trabalho[coluna]
                        .astype(str)
                        .str.contains(valor_padronizado, na=False)
                    )
                else:
                    mascara &= df_trabalho[coluna] == valor_padronizado

        df_trabalho = df_trabalho[mascara].reset_index(drop=True)
        print(f'📊 Após filtros OCI: {len(df_trabalho)} registros')

    if df_trabalho.empty:
        print('📊 Nenhum registro encontrado após filtros OCI')
        return []

    tabelas = []

    # Agrupa o dataframe de trabalho por SUBGRUPO_PROCEDIMENTO para iterar
    df_grouped_subgrupo = df_trabalho.groupby(COL_SUBGRUPO, observed=True)

    print(
        f'🎯 Subgrupos de procedimento encontrados após filtro: {df_trabalho[COL_SUBGRUPO].nunique()}'
    )

    # 2. Iteração por CADA SUBGRUPO DE PROCEDIMENTO ENCONTRADO
    for subgrupo, df_subgrupo_filtrado in df_grouped_subgrupo:

        print(f'📋 Processando subgrupo: {subgrupo}')
        print(f'   - Registros filtrados: {len(df_subgrupo_filtrado)}')

        # Cria a base de agregação para este subgrupo
        df_base_agregacao = df_subgrupo_filtrado.copy()

        tabela = []
        tabela.append(
            [
                'NIVEL',
                'DESCRIÇÃO',
                'QUANTIDADE APROVADA',
                'VALOR APROVADO (R$)',
            ]
        )

        # --- NÍVEL NACIONAL (Sempre o primeiro) ---
        # Para o nacional, usa a base completa (df) deste subgrupo
        df_subgrupo_completo = df[df[COL_SUBGRUPO] == subgrupo]
        quant_nacional = df_subgrupo_completo['QUANT_APROV'].sum()
        valor_nacional = df_subgrupo_completo['VALOR_APROV'].sum()
        tabela.append(
            [
                NIVEL_FORMATADO['NACIONAL'],
                'BRASIL',
                formatar_populacao(quant_nacional),
                formatar_valor_monetario(valor_nacional),
            ]
        )

        # --- LÓGICA DE DETALHE HIERÁRQUICO COM CORTE E PREFIXO ---

        # Se o filtro é NACIONAL, mostra todas as regiões
        if nivel_selecionado == 'NACIONAL':
            # Para nível nacional, mostra todas as regiões com base completa
            df_reg_base = df[df[COL_SUBGRUPO] == subgrupo]

            # 1. Agrega por Região (base completa)
            df_reg = (
                df_reg_base.groupby('NO_REGIAO', observed=True)[
                    ['QUANT_APROV', 'VALOR_APROV']
                ]
                .sum()
                .reset_index()
            )

            # 2. Itera a hierarquia (Região)
            for regiao_padronizada in (
                df_reg['NO_REGIAO'].sort_values().unique()
            ):

                df_regiao_linha = df_reg[
                    df_reg['NO_REGIAO'] == regiao_padronizada
                ]
                if df_regiao_linha.empty:
                    continue

                quant_regiao = df_regiao_linha['QUANT_APROV'].iloc[0]
                valor_regiao = df_regiao_linha['VALOR_APROV'].iloc[0]

                # Adiciona a Linha da REGIÃO
                tabela.append(
                    [
                        NIVEL_FORMATADO['REGIAO'],
                        get_descricao(regiao_padronizada),
                        formatar_populacao(quant_regiao),
                        formatar_valor_monetario(valor_regiao),
                    ]
                )

                # Para níveis abaixo de REGIÃO, usa base completa
                df_base_regiao = df_reg_base[
                    df_reg_base['NO_REGIAO'] == regiao_padronizada
                ]

                # Nível UF
                df_uf = (
                    df_base_regiao.groupby('NO_UF', observed=True)[
                        ['QUANT_APROV', 'VALOR_APROV']
                    ]
                    .sum()
                    .reset_index()
                )
                for uf_padronizada in df_uf['NO_UF'].sort_values().unique():

                    df_uf_linha = df_uf[df_uf['NO_UF'] == uf_padronizada]
                    if df_uf_linha.empty:
                        continue

                    quant_uf = df_uf_linha['QUANT_APROV'].iloc[0]
                    valor_uf = df_uf_linha['VALOR_APROV'].iloc[0]

                    # Adiciona a Linha da UF
                    tabela.append(
                        [
                            NIVEL_FORMATADO['UF'],
                            get_descricao(uf_padronizada),
                            formatar_populacao(quant_uf),
                            formatar_valor_monetario(valor_uf),
                        ]
                    )

                    # 🚨 CORTE: Se o filtro é NACIONAL, para a iteração (sai do loop de UF)
                    continue

        # Se o filtro é REGIÃO ou mais específico
        else:
            # Para filtros específicos, mostra apenas a região do filtro (se aplicável)
            if 'NO_REGIAO' in filtros:
                # Filtro por região específica
                regiao_filtro = filtros['NO_REGIAO']
                df_reg_filtrado = df[df[COL_SUBGRUPO] == subgrupo]
                df_reg_filtrado = df_reg_filtrado[
                    df_reg_filtrado['NO_REGIAO'] == regiao_filtro
                ]

                quant_regiao = df_reg_filtrado['QUANT_APROV'].sum()
                valor_regiao = df_reg_filtrado['VALOR_APROV'].sum()

                tabela.append(
                    [
                        NIVEL_FORMATADO['REGIAO'],
                        get_descricao(regiao_filtro),
                        formatar_populacao(quant_regiao),
                        formatar_valor_monetario(valor_regiao),
                    ]
                )

                # Continua com a lógica normal para níveis abaixo usando base filtrada
                df_base_regiao = df_base_agregacao[
                    df_base_agregacao['NO_REGIAO'] == regiao_filtro
                ]

            else:
                # Para outros filtros (UF, etc.), mostra apenas a região correspondente aos dados filtrados
                df_reg_filtrado = df_base_agregacao
                regioes_afetadas = df_reg_filtrado['NO_REGIAO'].unique()

                for regiao_padronizada in regioes_afetadas:
                    # Para mostrar o total REAL da região, usa base completa
                    df_regiao_completa = df[
                        (df[COL_SUBGRUPO] == subgrupo)
                        & (df['NO_REGIAO'] == regiao_padronizada)
                    ]
                    quant_regiao_real = df_regiao_completa['QUANT_APROV'].sum()
                    valor_regiao_real = df_regiao_completa['VALOR_APROV'].sum()

                    tabela.append(
                        [
                            NIVEL_FORMATADO['REGIAO'],
                            get_descricao(regiao_padronizada),
                            formatar_populacao(
                                quant_regiao_real
                            ),  # Total real da região
                            formatar_valor_monetario(valor_regiao_real),
                        ]
                    )

                    df_base_regiao = df_base_agregacao[
                        df_base_agregacao['NO_REGIAO'] == regiao_padronizada
                    ]

            # Continuação da hierarquia para níveis abaixo de REGIÃO
            if 'df_base_regiao' in locals() and not df_base_regiao.empty:
                # Nível UF
                df_uf = (
                    df_base_regiao.groupby('NO_UF', observed=True)[
                        ['QUANT_APROV', 'VALOR_APROV']
                    ]
                    .sum()
                    .reset_index()
                )
                for uf_padronizada in df_uf['NO_UF'].sort_values().unique():

                    df_uf_linha = df_uf[df_uf['NO_UF'] == uf_padronizada]
                    if df_uf_linha.empty:
                        continue

                    quant_uf = df_uf_linha['QUANT_APROV'].iloc[0]
                    valor_uf = df_uf_linha['VALOR_APROV'].iloc[0]

                    # Adiciona a Linha da UF
                    tabela.append(
                        [
                            NIVEL_FORMATADO['UF'],
                            get_descricao(uf_padronizada),
                            formatar_populacao(quant_uf),
                            formatar_valor_monetario(valor_uf),
                        ]
                    )

                    # 🚨 CORTE 1: Se o filtro é REGIÃO, para a iteração (sai do loop de UF)
                    if nivel_selecionado == 'REGIAO':
                        continue

                    # Filtra a base para a UF atual
                    df_base_uf = df_base_regiao[
                        df_base_regiao['NO_UF'] == uf_padronizada
                    ]

                    # Nível MACRORREGIÃO
                    if 'NO_MACRO_REG_SAUDE' in df_base_uf.columns:
                        df_macro = (
                            df_base_uf.groupby(
                                'NO_MACRO_REG_SAUDE', observed=True
                            )[['QUANT_APROV', 'VALOR_APROV']]
                            .sum()
                            .reset_index()
                        )
                        for macro_padronizada in (
                            df_macro['NO_MACRO_REG_SAUDE']
                            .sort_values()
                            .unique()
                        ):

                            df_macro_linha = df_macro[
                                df_macro['NO_MACRO_REG_SAUDE']
                                == macro_padronizada
                            ]
                            if df_macro_linha.empty:
                                continue

                            quant_macro = df_macro_linha['QUANT_APROV'].iloc[0]
                            valor_macro = df_macro_linha['VALOR_APROV'].iloc[0]

                            # Adiciona a Linha da MACRORREGIÃO
                            tabela.append(
                                [
                                    NIVEL_FORMATADO['MACRORREGIÃO'],
                                    get_descricao(macro_padronizada),
                                    formatar_populacao(quant_macro),
                                    formatar_valor_monetario(valor_macro),
                                ]
                            )

                            # 🚨 CORTE 2: Se o filtro é UF, para a iteração (sai do loop de MACRORREGIÃO)
                            if nivel_selecionado == 'UF':
                                continue

                            # Filtra a base para a MACRORREGIÃO atual
                            df_base_macro = df_base_uf[
                                df_base_uf['NO_MACRO_REG_SAUDE']
                                == macro_padronizada
                            ]

                            # Nível REGIÃO DE SAÚDE
                            if 'NO_REGIAO_SAUDE' in df_base_macro.columns:
                                df_rs = (
                                    df_base_macro.groupby(
                                        'NO_REGIAO_SAUDE', observed=True
                                    )[['QUANT_APROV', 'VALOR_APROV']]
                                    .sum()
                                    .reset_index()
                                )
                                for rs_padronizada in (
                                    df_rs['NO_REGIAO_SAUDE']
                                    .sort_values()
                                    .unique()
                                ):

                                    df_rs_linha = df_rs[
                                        df_rs['NO_REGIAO_SAUDE']
                                        == rs_padronizada
                                    ]
                                    if df_rs_linha.empty:
                                        continue

                                    quant_rs = df_rs_linha['QUANT_APROV'].iloc[
                                        0
                                    ]
                                    valor_rs = df_rs_linha['VALOR_APROV'].iloc[
                                        0
                                    ]

                                    # Adiciona a Linha da REGIÃO DE SAÚDE
                                    tabela.append(
                                        [
                                            NIVEL_FORMATADO['REGIAO_SAUDE'],
                                            get_descricao(rs_padronizada),
                                            formatar_populacao(quant_rs),
                                            formatar_valor_monetario(valor_rs),
                                        ]
                                    )

                                    # 🚨 CORTE 3: Se o filtro é MACRORREGIÃO ou REGIAO_SAUDE, para a iteração
                                    if nivel_selecionado in [
                                        'MACRORREGIÃO',
                                        'REGIAO_SAUDE',
                                    ]:
                                        continue

                                    # Filtra a base para a REGIÃO DE SAÚDE atual
                                    df_base_rs = df_base_macro[
                                        df_base_macro['NO_REGIAO_SAUDE']
                                        == rs_padronizada
                                    ]

                                    # Nível MUNICÍPIO
                                    if 'NO_MUNICIPIO' in df_base_rs.columns:
                                        df_mun = (
                                            df_base_rs.groupby(
                                                'NO_MUNICIPIO', observed=True
                                            )[['QUANT_APROV', 'VALOR_APROV']]
                                            .sum()
                                            .reset_index()
                                        )
                                        for mun_padronizado in (
                                            df_mun['NO_MUNICIPIO']
                                            .sort_values()
                                            .unique()
                                        ):

                                            df_mun_linha = df_mun[
                                                df_mun['NO_MUNICIPIO']
                                                == mun_padronizado
                                            ]
                                            if df_mun_linha.empty:
                                                continue

                                            quant_mun = df_mun_linha[
                                                'QUANT_APROV'
                                            ].iloc[0]
                                            valor_mun = df_mun_linha[
                                                'VALOR_APROV'
                                            ].iloc[0]

                                            # Adiciona a Linha do MUNICÍPIO
                                            tabela.append(
                                                [
                                                    NIVEL_FORMATADO[
                                                        'MUNICÍPIO'
                                                    ],
                                                    get_descricao(
                                                        mun_padronizado
                                                    ),
                                                    formatar_populacao(
                                                        quant_mun
                                                    ),
                                                    formatar_valor_monetario(
                                                        valor_mun
                                                    ),
                                                ]
                                            )

                                            # Nível UNIDADE (CNES)
                                            # Detalhe de Unidade CNES só é exibido se o nível de filtro for MUNICÍPIO ou mais detalhado
                                            if nivel_selecionado in [
                                                'MUNICÍPIO',
                                                'TIPO_UNIDADE',
                                                'CNES',
                                            ]:
                                                df_base_mun = df_base_rs[
                                                    df_base_rs['NO_MUNICIPIO']
                                                    == mun_padronizado
                                                ]
                                                df_cnes = (
                                                    df_base_mun.groupby(
                                                        [
                                                            'CO_CNES',
                                                            'NO_FANTASIA',
                                                        ],
                                                        observed=True,
                                                    )[
                                                        [
                                                            'QUANT_APROV',
                                                            'VALOR_APROV',
                                                        ]
                                                    ]
                                                    .sum()
                                                    .reset_index()
                                                )
                                                for (
                                                    _,
                                                    row_cnes,
                                                ) in df_cnes.iterrows():
                                                    cnes = row_cnes['CO_CNES']
                                                    nome_fantasia = row_cnes[
                                                        'NO_FANTASIA'
                                                    ]
                                                    quant_cnes = row_cnes[
                                                        'QUANT_APROV'
                                                    ]
                                                    valor_cnes = row_cnes[
                                                        'VALOR_APROV'
                                                    ]

                                                    # Adiciona a Linha da UNIDADE (CNES)
                                                    descricao_unidade = (
                                                        get_descricao(
                                                            nome_fantasia
                                                        )
                                                    )
                                                    tabela.append(
                                                        [
                                                            NIVEL_FORMATADO[
                                                                'CNES'
                                                            ],
                                                            f'{descricao_unidade} ({cnes})',
                                                            formatar_populacao(
                                                                quant_cnes
                                                            ),
                                                            formatar_valor_monetario(
                                                                valor_cnes
                                                            ),
                                                        ]
                                                    )

        # 3. ADICIONA TABELA NA LISTA DE RESULTADOS
        if len(tabela) > 2:  # Verifica se tem mais que cabeçalho + nacional
            tabelas.append(
                {'subgrupo_procedimento': subgrupo, 'dados': tabela}
            )
            print(
                f'✅ Tabela gerada para subgrupo {subgrupo}: {len(tabela)} linhas'
            )

    print(f'✅ Total de tabelas OCI geradas: {len(tabelas)}')
    return tabelas
