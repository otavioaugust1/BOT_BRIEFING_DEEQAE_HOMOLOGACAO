# ==============================================================================
# processing_geral.py
# FUNÇÕES DE LEVANTAMENTO DE DADOS PARA OS RELATORIOS
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
# Adiciona o locale brasileiro para formatação de números (ex: 1.000.000,00)
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
# --- DEFINIÇÕES DE AMBIENTE E CAMINHOS (CNES e IBGE) ---
# ------------------------------------------------------------------------------

# Define o diretório base subindo três níveis a partir do diretório atual
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', '..')
)

# Define os caminhos absolutos para os arquivos Parquet de dados
CAMINHO_PARQUET_CNES = os.path.join(
    BASE_DIR, 'db', 'cnes', 'consolidado_cnes_hab.parquet'
)
CAMINHO_PARQUET_IBGE = os.path.join(
    BASE_DIR, 'db', 'ibge', 'pop_municipal_brasil_2022.parquet'
)


# --- Mapeamento de UF para Região do País ---
# Usado para inferir a região geográfica, já que o arquivo CNES pode não a conter.
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
# --- FUNÇÕES AUXILIARES DE GEOGRAFIA E PADRONIZAÇÃO ---
# ------------------------------------------------------------------------------


def padronizar_nome_geografico(nome):
    """Remove acentos, caracteres especiais, e padroniza para UPPERCASE para filtros."""
    if pd.isna(nome) or nome is None:
        return ''
    nome = str(nome).upper().strip()

    # 1. Remove acentos e Ç/Ñ (garantindo que Ñ vire N)
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

    # Retorna o nome formatado como título (Primeiras letras maiúsculas)
    return nome.title() if nome else '-'


# ------------------------------------------------------------------------------
# --- FUNÇÃO DE MAPEAMENTO GERAL DE FILTROS ---
# ------------------------------------------------------------------------------


def mapear_selecao_geral(dados_selecao: Dict[str, str]) -> Dict[str, Any]:
    """
    Traduz os parâmetros de seleção do frontend ('regiao', 'uf', etc.) para
    nomes de colunas internas do DataFrame ('NO_REGIAO', 'NO_UF', etc.)
    e determina o nível de agregação final.
    """
    HIERARQUIA = [
        ('regiao', 'REGIAO', 'NO_REGIAO'),
        ('uf', 'UF', 'NO_UF'),
        ('macro', 'MACRORREGIAO', 'NO_MACRO_REG_SAUDE'),
        ('regiaoSaude', 'REGIAO_SAUDE', 'NO_REGIAO_SAUDE'),
        ('municipio', 'MUNICIPIO', 'NO_MUNICIPIO'),
        ('unidade', 'TIPO_UNIDADE', 'DS_TIPO_UNIDADE'),
        ('cnes', 'CNES', 'CO_CNES'),
    ]

    filtros = {}
    nivel_agregacao = 'NACIONAL'

    for chave_frontend, nivel_nome, coluna_df in HIERARQUIA:
        valor_original = dados_selecao.get(chave_frontend)
        valor_padronizado = padronizar_nome_geografico(valor_original)

        # Se o valor não for nulo e for diferente de 'TODOS'
        if valor_original and valor_padronizado != 'TODOS':
            if nivel_nome == 'CNES':
                # O CNES é aplicado pelo valor original, pois é um código
                filtros['CO_CNES'] = valor_original
            else:
                # Outros filtros usam o valor padronizado (sem acentos/caps)
                filtros[coluna_df] = valor_padronizado

            # Atualiza o nível de agregação para o nível mais detalhado selecionado
            nivel_agregacao = nivel_nome

    return {'NIVEL_AGREGACAO': nivel_agregacao, 'FILTROS': filtros}


# ------------------------------------------------------------------------------
# --- DADOS IBGE -- PARA O RELATORIO (gerar_descricao_demografica) ---
# ------------------------------------------------------------------------------


def gerar_descricao_demografica(dados_selecao: Dict[str, Any]):
    """
    Gera a descrição demográfica e o contexto geográfico (texto) para o nível
    de agregação selecionado, utilizando os dados de população do IBGE.
    """
    mapa_selecao = mapear_selecao_geral(dados_selecao)
    nivel_agregacao = mapa_selecao['NIVEL_AGREGACAO']
    filtros = mapa_selecao['FILTROS']

    # Lógica de saída para o nível mais detalhado (Unidade/CNES)
    if nivel_agregacao in ['TIPO_UNIDADE', 'CNES']:
        # ... (cálculo de nomes e CNES para o texto descritivo)
        nome_unidade_raw = dados_selecao.get('unidade', '')
        cnes_code_raw = dados_selecao.get('cnes', '')

        if not cnes_code_raw or cnes_code_raw == 'TODOS':
            try:
                cnes_code_from_unidade = nome_unidade_raw.split('-')[
                    -1
                ].strip()
                if cnes_code_from_unidade.isdigit():
                    cnes_code = cnes_code_from_unidade
                else:
                    cnes_code = cnes_code_raw
            except:
                cnes_code = cnes_code_raw
        else:
            cnes_code = cnes_code_raw

        nome_municipio_original = dados_selecao.get('municipio', 'N/A')
        nome_uf_original = dados_selecao.get('uf', 'N/A')

        nome_fantasia_final = get_descricao(
            nome_unidade_raw.replace(cnes_code, '').replace('-', '').strip()
        )

        if (
            nome_fantasia_final != 'Estabelecimento De Saúde'
            and cnes_code
            and cnes_code != 'TODOS'
        ):
            return (
                f'O estabelecimento de saúde {nome_fantasia_final}, identificado pelo número {cnes_code}, '
                f'está localizado no município de {nome_municipio_original.title()}, no estado de {nome_uf_original.title()}.\n'
            )
        return 'Não foi possível gerar a descrição demográfica para o Nível Unidade/CNES devido a dados incompletos ou mal formatados.'

    caminho_parquet = Path(CAMINHO_PARQUET_IBGE)

    # 1. Carregamento dos dados do IBGE
    if not caminho_parquet.exists():
        return f'Erro: Arquivo de dados demográficos do IBGE não encontrado. Caminho verificado: {CAMINHO_PARQUET_IBGE}'

    try:
        df = pd.read_parquet(caminho_parquet)
    except Exception as e:
        return f'Erro ao carregar o arquivo de dados demográficos: {str(e)}'

    # 2. Padronização das colunas geográficas do IBGE
    IBGE_COLUMNS = {
        'Regiao do Pais': 'NO_REGIAO',
        'UF': 'NO_UF',
        'Macrorregiao de Saude': 'NO_MACRO_REG_SAUDE',
        'Regiao de Saude': 'NO_REGIAO_SAUDE',
        'Municipio': 'NO_MUNICIPIO',
    }

    for col_ibge, col_cnes in IBGE_COLUMNS.items():
        if col_ibge in df.columns:
            df[col_ibge] = df[col_ibge].apply(padronizar_nome_geografico)

    COLUNA_POP_ESTIMADA = 'Populacao Estimada IBGE 2022'

    if COLUNA_POP_ESTIMADA not in df.columns:
        return 'Erro: Coluna de população não encontrada no arquivo IBGE.'

    # 3. Conversão da coluna de população para numérica
    if COLUNA_POP_ESTIMADA in df.columns:
        df[COLUNA_POP_ESTIMADA] = (
            df[COLUNA_POP_ESTIMADA]
            .astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
        )
        df[COLUNA_POP_ESTIMADA] = pd.to_numeric(
            df[COLUNA_POP_ESTIMADA], errors='coerce'
        )

    df_trabalho = df.copy()

    # 4. Aplicação dos filtros na base de população
    if filtros:
        mascara = pd.Series(True, index=df_trabalho.index)

        MAP_FILTRO_CNES_IBGE = {
            'NO_REGIAO': 'Regiao do Pais',
            'NO_UF': 'UF',
            'NO_MACRO_REG_SAUDE': 'Macrorregiao de Saude',
            'NO_REGIAO_SAUDE': 'Regiao de Saude',
            'NO_MUNICIPIO': 'Municipio',
        }

        filtros_a_aplicar = filtros

        # Lógica especial para filtro de MUNICIPIO (garante UF e MUNICIPIO)
        if nivel_agregacao == 'MUNICIPIO':
            filtros_a_aplicar = {}
            if 'NO_UF' in filtros:
                filtros_a_aplicar['NO_UF'] = filtros['NO_UF']
            if 'NO_MUNICIPIO' in filtros:
                filtros_a_aplicar['NO_MUNICIPIO'] = filtros['NO_MUNICIPIO']

        for coluna_cnes, valor_padronizado in filtros_a_aplicar.items():
            coluna_ibge = MAP_FILTRO_CNES_IBGE.get(coluna_cnes)

            if coluna_ibge in df_trabalho.columns:
                if coluna_cnes in [
                    'NO_MACRO_REG_SAUDE',
                    'NO_REGIAO_SAUDE',
                ] and nivel_agregacao in ['MACRORREGIAO', 'REGIAO_SAUDE']:
                    # Filtro usando 'contains' para Macro/Região de Saúde
                    mascara &= (
                        df_trabalho[coluna_ibge]
                        .astype(str)
                        .str.contains(valor_padronizado, na=False)
                    )
                else:
                    # Filtro de igualdade para outros níveis
                    mascara &= df_trabalho[coluna_ibge] == valor_padronizado

        df_trabalho = df_trabalho[mascara].reset_index(drop=True)

    if df_trabalho.empty:
        return f'Não foram encontrados dados demográficos para a seleção: {nivel_agregacao} (Filtros: {filtros}).'

    # 5. Cálculo e formatação da população total
    pop_total = int(df_trabalho[COLUNA_POP_ESTIMADA].sum())
    pop_formatada = formatar_populacao(pop_total)

    # 6. Geração do texto descritivo com base no nível de agregação
    if nivel_agregacao == 'NACIONAL':
        total_macrorregioes = df['Macrorregiao de Saude'].nunique()
        total_regioes_saude = df['Regiao de Saude'].nunique()

        return (
            f'O Brasil, em sua totalidade, é composto por 5 regiões geográficas, 27 unidades federativas '
            f'e 5.572 municípios. O território nacional está organizado em {total_macrorregioes} Macrorregiões de Saúde '
            f'e {total_regioes_saude} Regiões de Saúde, abrangendo uma população total estimada em {pop_formatada} habitantes '
            f'(IBGE, 2022).'
        )

    elif nivel_agregacao == 'REGIAO':
        # ... (texto descritivo para Região)
        nome_regiao_original = dados_selecao.get('regiao', 'N/A').title()

        return (
            f"A região {nome_regiao_original}, composta por {df_trabalho['UF'].nunique()} estados e {df_trabalho['Municipio'].nunique()} municípios, "
            f"está organizada em {df_trabalho['Macrorregiao de Saude'].nunique()} Macrorregiões de Saúde e "
            f"{df_trabalho['Regiao de Saude'].nunique()} Regiões de Saúde, reunindo uma população estimada em {pop_formatada} habitantes (IBGE, 2022).\n"
        )

    elif nivel_agregacao == 'UF':
        # ... (texto descritivo para UF)
        nome_uf_original = dados_selecao.get('uf', 'N/A').title()

        return (
            f"O estado de {nome_uf_original} é formado por {df_trabalho['Municipio'].nunique()} municípios, distribuídos em "
            f"{df_trabalho['Macrorregiao de Saude'].nunique()} Macrorregiões de Saúde e {df_trabalho['Regiao de Saude'].nunique()} Regiões de Saúde, "
            f'totalizando uma população estimada em {pop_formatada} habitantes (IBGE, 2022).\n'
        )

    elif nivel_agregacao == 'MACRORREGIAO':
        # ... (texto descritivo para Macrorregião)
        nome_uf_original = dados_selecao.get('uf', 'N/A').title()

        macro_val = (
            df_trabalho['Macrorregiao de Saude'].iloc[0]
            if not df_trabalho.empty
            else 'N/A'
        )
        macro_display_name = get_descricao(macro_val)

        return (
            f"A Macrorregião de Saúde {macro_display_name}, localizada no estado de {nome_uf_original}, abrange {df_trabalho['Municipio'].nunique()} municípios, "
            f"organizados em {df_trabalho['Regiao de Saude'].nunique()} Regiões de Saúde, com uma população estimada em {pop_formatada} habitantes (IBGE, 2022).\n"
        )

    elif nivel_agregacao == 'REGIAO_SAUDE':
        # ... (texto descritivo para Região de Saúde)
        uf_val = df_trabalho['UF'].iloc[0] if not df_trabalho.empty else 'N/A'
        regiao_val = (
            df_trabalho['Regiao de Saude'].iloc[0]
            if not df_trabalho.empty
            else 'N/A'
        )
        macro_val = (
            df_trabalho['Macrorregiao de Saude'].iloc[0]
            if not df_trabalho.empty
            else 'N/A'
        )

        regiao_display_name = get_descricao(regiao_val)
        macro_display_name = get_descricao(macro_val)

        return (
            f'A Região de Saúde {regiao_display_name}, pertencente à Macrorregião de Saúde {macro_display_name} do estado de {uf_val.title()}, '
            f"é composta por {df_trabalho['Municipio'].nunique()} municípios, reunindo uma população estimada em {pop_formatada} habitantes (IBGE, 2022).\n"
        )

    elif nivel_agregacao == 'MUNICIPIO':
        # ... (texto descritivo para Município)
        nome_municipio_original = dados_selecao.get('municipio', 'N/A').title()
        nome_uf_original = dados_selecao.get('uf', 'N/A').title()

        macro_val = (
            df_trabalho['Macrorregiao de Saude'].iloc[0]
            if not df_trabalho.empty
            else 'N/A'
        )
        regiao_val = (
            df_trabalho['Regiao de Saude'].iloc[0]
            if not df_trabalho.empty
            else 'N/A'
        )

        regiao_display_name = get_descricao(regiao_val)
        macro_display_name = get_descricao(macro_val)

        return (
            f'O município de {nome_municipio_original}, localizado no estado de {nome_uf_original}, integra a Macrorregião {macro_display_name} '
            f'e a Região de Saúde {regiao_display_name}, possuindo uma população estimada em {pop_formatada} habitantes (IBGE, 2022).\n'
        )

    return 'Não foi possível gerar a descrição demográfica com os dados fornecidos.'


# ------------------------------------------------------------------------------
# --- DADOS CNES -- HABILITAÇÕES (gerar_tabela_cnes_hab) - ESTRUTURA HIERÁRQUICA ---
# ------------------------------------------------------------------------------


def gerar_tabela_cnes_hab(
    dados_selecao: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Gera tabela hierárquica de habilitações CNES.
    O formato de saída é uma lista de dicionários, onde cada dicionário
    representa uma habilitação e contém a estrutura de dados hierárquica (lista de listas).
    """
    try:
        print('🔍 INICIANDO gerar_tabela_cnes_hab...')

        caminho_parquet = Path(CAMINHO_PARQUET_CNES)
        print(f'📁 Caminho do parquet: {caminho_parquet}')

        if not caminho_parquet.exists():
            print('❌ ARQUIVO PARQUET NÃO ENCONTRADO!')
            return []

        # 1. Carregamento da base CNES
        df = pd.read_parquet(caminho_parquet)
        print(f'✅ Parquet carregado. Total de registros: {len(df)}')

        # 2. MAPEAMENTO FLEXÍVEL DE COLUNAS
        # Tenta identificar as colunas reais no DF com base em padrões (mapa_colunas)
        mapa_colunas = {}
        padroes_busca = {
            'NO_UF': ['UF_DESC', 'UF', 'ESTADO', 'UNIDADE FEDERATIVA'],
            'NO_REGIAO': ['REGIAO', 'REGIÃO', 'REGIONAL'],
            'NO_MACRO_REG_SAUDE': [
                'DS_MACROREGIAO_ATEND',
                'MACRO',
                'MACRORREGIAO',
                'RRAS',
            ],
            'NO_REGIAO_SAUDE': [
                'DS_REGIAO_SAUDE_ATEND',
                'REGIAO_SAUDE',
                'REGIÃO_SAÚDE',
            ],
            'NO_MUNICIPIO': ['MUNICIPIO', 'MUNICÍPIO', 'CIDADE'],
            'DS_TIPO_UNIDADE': [
                'TIPO_UNIDADE',
                'TIPO',
                'TIPO ESTABELECIMENTO',
            ],
            'CO_CNES': ['CNES', 'CODIGO', 'CÓDIGO', 'ESTABELECIMENTO'],
            'NO_FANTASIA': ['FANTASIA', 'NOME', 'NOME FANTASIA'],
            'DS_HABILITACAO': [
                'HABILITACAO',
                'HABILITAÇÃO',
                'TIPO_HABILITACAO',
                'PROGRAMA',
            ],
        }

        for col_padrao, padroes in padroes_busca.items():
            for padrao in padroes:
                for col_real in df.columns:
                    if padrao.upper() in col_real.upper():
                        mapa_colunas[col_padrao] = col_real
                        break
                if col_padrao in mapa_colunas:
                    break

        print(f'🎯 Mapeamento final: {mapa_colunas}')

        # 3. VERIFICA COLUNAS ESSENCIAIS
        colunas_essenciais = ['DS_HABILITACAO', 'NO_UF']
        for col_essencial in colunas_essenciais:
            if col_essencial not in mapa_colunas:
                print(f'❌ COLUNA ESSENCIAL NÃO ENCONTRADA: {col_essencial}')
                return []

        # 4. PADRONIZA COLUNAS (UPPERCASE e sem acentos)
        print('🔄 Padronizando colunas...')
        for col_padrao, col_real in mapa_colunas.items():
            if col_real in df.columns:
                df[col_real] = (
                    df[col_real].astype(str).apply(padronizar_nome_geografico)
                )

        # 5. OBTÉM NÍVEL E FILTROS
        mapa_selecao = mapear_selecao_geral(dados_selecao)
        nivel_selecionado = mapa_selecao['NIVEL_AGREGACAO']
        filtros = mapa_selecao['FILTROS']

        print(f'🎯 Nível selecionado: {nivel_selecionado}')
        print(f'🎯 Filtros aplicados: {filtros}')

        # 6. APLICAÇÃO DE FILTROS HIERÁRQUICOS
        df_filtrado = df.copy()
        df_total = (
            df.copy()
        )   # Base total para o cálculo dos totais hierárquicos
        print(f'📊 Registros antes do filtro: {len(df_filtrado)}')

        # Filtro para REGIÃO (lógica que usa o mapeamento UF_TO_REGIAO)
        if nivel_selecionado == 'REGIAO' and 'NO_REGIAO' in filtros:
            regiao_selecionada = filtros['NO_REGIAO']
            # Obtém todas as UFs que pertencem à Região selecionada
            ufs_da_regiao = [
                uf
                for uf, reg in UF_TO_REGIAO.items()
                if padronizar_nome_geografico(reg) == regiao_selecionada
            ]
            if ufs_da_regiao:
                coluna_uf = mapa_colunas['NO_UF']
                # Filtra o DataFrame apenas pelas UFs da Região
                df_filtrado = df_filtrado[
                    df_filtrado[coluna_uf].isin(ufs_da_regiao)
                ]
                print(f'📊 Após filtro REGIÃO: {len(df_filtrado)} registros')

        # Filtros diretos para outros níveis (UF, Macro, RS, Município, CNES)
        filtros_aplicaveis = {
            'UF': 'NO_UF',
            'MACRORREGIAO': 'NO_MACRO_REG_SAUDE',
            'REGIAO_SAUDE': 'NO_REGIAO_SAUDE',
            'MUNICIPIO': 'NO_MUNICIPIO',
            'CNES': 'CO_CNES',
        }

        if nivel_selecionado in filtros_aplicaveis:
            coluna_filtro = filtros_aplicaveis[nivel_selecionado]
            if coluna_filtro in filtros:
                valor_filtro = filtros[coluna_filtro]
                coluna_real = mapa_colunas.get(coluna_filtro)
                if coluna_real and coluna_real in df_filtrado.columns:
                    # Aplica o filtro de igualdade
                    df_filtrado = df_filtrado[
                        df_filtrado[coluna_real] == valor_filtro
                    ]
                    print(
                        f'📊 Após filtro {nivel_selecionado}: {len(df_filtrado)} registros'
                    )

        print(f'📊 Registros após TODOS os filtros: {len(df_filtrado)}')

        if df_filtrado.empty:
            print('⚠️ Nenhum dado encontrado para os critérios selecionados')
            return []

        # 7. DEFINIÇÃO DE COLUNAS (Para evitar erros de chave inexistente)
        coluna_hab = mapa_colunas['DS_HABILITACAO']
        coluna_uf = mapa_colunas['NO_UF']
        coluna_macro = mapa_colunas.get('NO_MACRO_REG_SAUDE')
        coluna_regiao_saude = mapa_colunas.get('NO_REGIAO_SAUDE')
        coluna_municipio = mapa_colunas.get('NO_MUNICIPIO')
        coluna_tipo_unidade = mapa_colunas.get('DS_TIPO_UNIDADE')
        coluna_cnes = mapa_colunas.get('CO_CNES')
        coluna_fantasia = mapa_colunas.get('NO_FANTASIA')

        # 8. PROCESSAMENTO POR TIPO DE HABILITAÇÃO
        tabelas = []
        tipos_hab = df_filtrado[coluna_hab].unique()
        print(f'🎯 Tipos de habilitação encontrados: {len(tipos_hab)}')

        for tipo_hab in tipos_hab:
            # df_tipo: DataFrame APENAS com a habilitação atual (aplicado filtro)
            df_tipo = df_filtrado[df_filtrado[coluna_hab] == tipo_hab]
            # df_total: DataFrame TOTAL (sem filtro geográfico) para a habilitação
            df_total_hab = df[df[coluna_hab] == tipo_hab]

            print(f'📋 Processando: {tipo_hab}')
            print(f'   - Registros filtrados: {len(df_tipo)}')

            if df_tipo.empty:
                print(f'   ⚠️ Nenhum registro para {tipo_hab}, pulando...')
                continue

            tabela = []
            tabela.append(['NIVEL', 'DESCRIÇÃO', 'QUANT'])

            # --- Regras de Hierarquia de Exibição ---

            # REGRA 1: NACIONAL - SEMPRE MOSTRA O TOTAL BRASIL
            quant_nacional = len(df_total_hab)
            tabela.append(
                ['NACIONAL', 'BRASIL', formatar_populacao(quant_nacional)]
            )

            # REGRA 2: NACIONAL - mostra todas REGIÕES e UFs
            if nivel_selecionado == 'NACIONAL':
                # Lógica para construir a hierarquia Região -> UF a partir do df_total_hab
                regioes_ufs = {}
                for _, row in df_total_hab.iterrows():
                    # Mapeamento e contagem por Região/UF
                    uf = row[coluna_uf].upper().strip()
                    regiao = UF_TO_REGIAO.get(uf, 'NÃO IDENTIFICADA')
                    if regiao not in regioes_ufs:
                        regioes_ufs[regiao] = {}
                    if uf not in regioes_ufs[regiao]:
                        regioes_ufs[regiao][uf] = 0
                    regioes_ufs[regiao][uf] += 1

                # Exibição: Região (Total) -> UFs (Contagem)
                for regiao in sorted(
                    [r for r in regioes_ufs.keys() if r != 'NÃO IDENTIFICADA']
                ):
                    ufs_regiao = regioes_ufs[regiao]
                    total_regiao = sum(ufs_regiao.values())
                    tabela.append(
                        [
                            'REGIÃO',
                            regiao.title(),
                            formatar_populacao(total_regiao),
                        ]
                    )
                    for uf in sorted(ufs_regiao.keys()):
                        quant_uf = ufs_regiao[uf]
                        tabela.append(
                            [' - UF', uf.title(), formatar_populacao(quant_uf)]
                        )

            # REGRA 3: REGIÃO - mostra UFs e MACRORREGIÕES
            elif nivel_selecionado == 'REGIAO' and 'NO_REGIAO' in filtros:
                # Exibe a Região selecionada, suas UFs, e as Macrorregiões dentro das UFs
                regiao_selecionada = filtros['NO_REGIAO']
                ufs_da_regiao = [
                    uf
                    for uf, reg in UF_TO_REGIAO.items()
                    if padronizar_nome_geografico(reg) == regiao_selecionada
                ]

                if ufs_da_regiao:
                    df_regiao = df_tipo[df_tipo[coluna_uf].isin(ufs_da_regiao)]
                    quant_regiao = len(df_regiao)
                    tabela.append(
                        [
                            'REGIÃO',
                            regiao_selecionada.title(),
                            formatar_populacao(quant_regiao),
                        ]
                    )

                    for uf in sorted(ufs_da_regiao):
                        df_uf = df_tipo[df_tipo[coluna_uf] == uf]
                        quant_uf = len(df_uf)
                        if quant_uf > 0:
                            tabela.append(
                                [
                                    'UF',
                                    uf.title(),
                                    formatar_populacao(quant_uf),
                                ]
                            )
                            if coluna_macro:
                                macros_uf = df_uf[coluna_macro].value_counts()
                                for macro, quant_macro in macros_uf.items():
                                    if macro and macro not in ['NAN', '']:
                                        descricao_macro = get_descricao(macro)
                                        tabela.append(
                                            [
                                                ' - MACRORREGIÃO',
                                                descricao_macro,
                                                formatar_populacao(
                                                    quant_macro
                                                ),
                                            ]
                                        )

            # REGRA 4: UF - mostra MACRORREGIÕES e REGIÕES DE SAÚDE
            elif nivel_selecionado == 'UF' and 'NO_UF' in filtros:
                # Exibe a UF selecionada, suas Macrorregiões e as Regiões de Saúde dentro delas
                uf_selecionada = filtros['NO_UF']
                df_uf_total = df_total_hab[
                    df_total_hab[coluna_uf] == uf_selecionada
                ]
                quant_uf = len(df_uf_total)
                tabela.append(
                    [
                        'UF',
                        uf_selecionada.title(),
                        formatar_populacao(quant_uf),
                    ]
                )

                if coluna_macro:
                    macros_uf = df_tipo[coluna_macro].value_counts()
                    for macro, quant_macro in macros_uf.items():
                        if macro and macro not in ['NAN', '']:
                            descricao_macro = get_descricao(macro)
                            tabela.append(
                                [
                                    'MACRORREGIÃO',
                                    descricao_macro,
                                    formatar_populacao(quant_macro),
                                ]
                            )

                            if coluna_regiao_saude:
                                df_macro = df_tipo[
                                    df_tipo[coluna_macro] == macro
                                ]
                                regioes_saude_macro = df_macro[
                                    coluna_regiao_saude
                                ].value_counts()
                                for (
                                    rs,
                                    quant_rs,
                                ) in regioes_saude_macro.items():
                                    if rs and rs not in ['NAN', '']:
                                        descricao_rs = get_descricao(rs)
                                        tabela.append(
                                            [
                                                ' - REGIÃO DE SAUDE',
                                                descricao_rs,
                                                formatar_populacao(quant_rs),
                                            ]
                                        )

            # REGRA 5: MACRORREGIAO - mostra REGIÕES DE SAÚDE
            elif (
                nivel_selecionado == 'MACRORREGIAO'
                and 'NO_MACRO_REG_SAUDE' in filtros
            ):
                # Exibe o contexto geográfico superior (Região e UF) e as Regiões de Saúde
                macro_selecionada = filtros['NO_MACRO_REG_SAUDE']

                if not df_tipo.empty:
                    # Tenta obter o contexto geográfico do primeiro registro filtrado
                    uf_macro = df_tipo[coluna_uf].iloc[0]
                    regiao_macro = UF_TO_REGIAO.get(
                        uf_macro, 'NÃO IDENTIFICADA'
                    )

                    # Contexto geográfico superior (Região, UF, Macro)
                    if regiao_macro != 'NÃO IDENTIFICADA':
                        ufs_regiao = [
                            uf
                            for uf, reg in UF_TO_REGIAO.items()
                            if reg == regiao_macro
                        ]
                        df_regiao = df_total_hab[
                            df_total_hab[coluna_uf].isin(ufs_regiao)
                        ]
                        tabela.append(
                            [
                                'REGIÃO',
                                regiao_macro.title(),
                                formatar_populacao(len(df_regiao)),
                            ]
                        )

                    df_uf = df_total_hab[df_total_hab[coluna_uf] == uf_macro]
                    tabela.append(
                        [
                            'UF',
                            uf_macro.title(),
                            formatar_populacao(len(df_uf)),
                        ]
                    )

                    df_macro = df_total_hab[
                        df_total_hab[coluna_macro] == macro_selecionada
                    ]
                    descricao_macro = get_descricao(macro_selecionada)
                    tabela.append(
                        [
                            'MACRORREGIÃO',
                            descricao_macro,
                            formatar_populacao(len(df_macro)),
                        ]
                    )

                    # Regiões de Saúde (nível abaixo)
                    if coluna_regiao_saude:
                        regioes_saude_macro = df_tipo[
                            coluna_regiao_saude
                        ].value_counts()
                        for rs, quant_rs in regioes_saude_macro.items():
                            if rs and rs not in ['NAN', '']:
                                descricao_rs = get_descricao(rs)
                                tabela.append(
                                    [
                                        ' - REGIÃO DE SAUDE',
                                        descricao_rs,
                                        formatar_populacao(quant_rs),
                                    ]
                                )

            # REGRA 6: REGIÃO DE SAÚDE - mostra MUNICÍPIOS
            elif (
                nivel_selecionado == 'REGIAO_SAUDE'
                and 'NO_REGIAO_SAUDE' in filtros
            ):
                # Exibe o contexto geográfico superior (Região, UF, Macro) e os Municípios
                regiao_saude_selecionada = filtros['NO_REGIAO_SAUDE']

                if not df_tipo.empty:
                    # Tenta obter o contexto geográfico do primeiro registro filtrado
                    uf_rs = df_tipo[coluna_uf].iloc[0]
                    macro_rs = df_tipo[coluna_macro].iloc[0]
                    regiao_rs = UF_TO_REGIAO.get(uf_rs, 'NÃO IDENTIFICADA')

                    # Contexto geográfico superior
                    if regiao_rs != 'NÃO IDENTIFICADA':
                        ufs_regiao = [
                            uf
                            for uf, reg in UF_TO_REGIAO.items()
                            if reg == regiao_rs
                        ]
                        df_regiao = df_total_hab[
                            df_total_hab[coluna_uf].isin(ufs_regiao)
                        ]
                        tabela.append(
                            [
                                'REGIÃO',
                                regiao_rs.title(),
                                formatar_populacao(len(df_regiao)),
                            ]
                        )

                    df_uf = df_total_hab[df_total_hab[coluna_uf] == uf_rs]
                    tabela.append(
                        ['UF', uf_rs.title(), formatar_populacao(len(df_uf))]
                    )

                    df_macro = df_total_hab[
                        df_total_hab[coluna_macro] == macro_rs
                    ]
                    descricao_macro = get_descricao(macro_rs)
                    tabela.append(
                        [
                            'MACRORREGIÃO',
                            descricao_macro,
                            formatar_populacao(len(df_macro)),
                        ]
                    )

                    df_regiao_saude = df_total_hab[
                        df_total_hab[coluna_regiao_saude]
                        == regiao_saude_selecionada
                    ]
                    descricao_rs = get_descricao(regiao_saude_selecionada)
                    tabela.append(
                        [
                            'REGIÃO DE SAUDE',
                            descricao_rs,
                            formatar_populacao(len(df_regiao_saude)),
                        ]
                    )

                    # Municípios (nível abaixo)
                    if coluna_municipio:
                        municipios_rs = df_tipo[
                            coluna_municipio
                        ].value_counts()
                        for municipio, quant in municipios_rs.items():
                            if municipio and municipio not in ['NAN', '']:
                                tabela.append(
                                    [
                                        ' - MUNICIPIO',
                                        municipio.title(),
                                        formatar_populacao(quant),
                                    ]
                                )

            # REGRA 7: MUNICÍPIO - mostra TIPO_UNIDADE e CNES
            elif (
                nivel_selecionado == 'MUNICIPIO' and 'NO_MUNICIPIO' in filtros
            ):
                # Exibe o contexto geográfico superior e Tipos de Unidade/CNES
                municipio_selecionado = filtros['NO_MUNICIPIO']

                if not df_tipo.empty:
                    # Tenta obter o contexto geográfico do primeiro registro filtrado
                    uf_municipio = df_tipo[coluna_uf].iloc[0]
                    macro_municipio = df_tipo[coluna_macro].iloc[0]
                    regiao_saude_municipio = df_tipo[coluna_regiao_saude].iloc[
                        0
                    ]
                    regiao_municipio = UF_TO_REGIAO.get(
                        uf_municipio, 'NÃO IDENTIFICADA'
                    )

                    # Contexto geográfico superior
                    if regiao_municipio != 'NÃO IDENTIFICADA':
                        ufs_regiao = [
                            uf
                            for uf, reg in UF_TO_REGIAO.items()
                            if reg == regiao_municipio
                        ]
                        df_regiao = df_total_hab[
                            df_total_hab[coluna_uf].isin(ufs_regiao)
                        ]
                        tabela.append(
                            [
                                'REGIÃO',
                                regiao_municipio.title(),
                                formatar_populacao(len(df_regiao)),
                            ]
                        )

                    df_uf = df_total_hab[
                        df_total_hab[coluna_uf] == uf_municipio
                    ]
                    tabela.append(
                        [
                            'UF',
                            uf_municipio.title(),
                            formatar_populacao(len(df_uf)),
                        ]
                    )

                    df_macro = df_total_hab[
                        df_total_hab[coluna_macro] == macro_municipio
                    ]
                    descricao_macro = get_descricao(macro_municipio)
                    tabela.append(
                        [
                            'MACRORREGIÃO',
                            descricao_macro,
                            formatar_populacao(len(df_macro)),
                        ]
                    )

                    df_regiao_saude = df_total_hab[
                        df_total_hab[coluna_regiao_saude]
                        == regiao_saude_municipio
                    ]
                    descricao_rs = get_descricao(regiao_saude_municipio)
                    tabela.append(
                        [
                            'REGIÃO DE SAUDE',
                            descricao_rs,
                            formatar_populacao(len(df_regiao_saude)),
                        ]
                    )

                    df_municipio = df_total_hab[
                        df_total_hab[coluna_municipio] == municipio_selecionado
                    ]
                    tabela.append(
                        [
                            'MUNICIPIO',
                            municipio_selecionado.title(),
                            formatar_populacao(len(df_municipio)),
                        ]
                    )

                    # Tipos de Unidade e CNES (nível abaixo)
                    if coluna_tipo_unidade and coluna_cnes and coluna_fantasia:
                        tipos_unidade = df_tipo[
                            coluna_tipo_unidade
                        ].value_counts()
                        for tipo_unidade, quant_tipo in tipos_unidade.items():
                            if tipo_unidade and tipo_unidade not in [
                                'NAN',
                                '',
                            ]:
                                tabela.append(
                                    [
                                        ' - TIPO_UNIDADE',
                                        tipo_unidade.title(),
                                        formatar_populacao(quant_tipo),
                                    ]
                                )

                                df_tipo_especifico = df_tipo[
                                    df_tipo[coluna_tipo_unidade]
                                    == tipo_unidade
                                ]
                                cnes_tipo = df_tipo_especifico[
                                    coluna_cnes
                                ].value_counts()

                                for cnes, quant_cnes in cnes_tipo.items():
                                    if cnes and cnes not in ['NAN', '']:
                                        # Obtém o nome fantasia da unidade
                                        nome_fantasia = df_tipo_especifico[
                                            df_tipo_especifico[coluna_cnes]
                                            == cnes
                                        ][coluna_fantasia].iloc[0]
                                        descricao_unidade = get_descricao(
                                            nome_fantasia
                                        )
                                        tabela.append(
                                            [
                                                ' - - CNES',
                                                f'{descricao_unidade} ({cnes})',
                                                formatar_populacao(quant_cnes),
                                            ]
                                        )

            # REGRA 8: UNIDADE (CNES) - mostra unidade específica
            elif nivel_selecionado == 'CNES' and 'CO_CNES' in filtros:
                # Exibe todo o contexto geográfico superior e a unidade CNES
                cnes_selecionado = filtros['CO_CNES']

                if not df_tipo.empty:
                    # Unidade COM habilitação (usa os dados da unidade filtrada)
                    unidade_info = df_tipo.iloc[0]
                    uf_unidade = unidade_info[coluna_uf]
                    macro_unidade = unidade_info[coluna_macro]
                    regiao_saude_unidade = unidade_info[coluna_regiao_saude]
                    municipio_unidade = unidade_info[coluna_municipio]
                    tipo_unidade_unidade = unidade_info[coluna_tipo_unidade]
                    nome_fantasia_unidade = unidade_info[coluna_fantasia]
                    regiao_unidade = UF_TO_REGIAO.get(
                        uf_unidade, 'NÃO IDENTIFICADA'
                    )

                    # Contexto geográfico superior
                    if regiao_unidade != 'NÃO IDENTIFICADA':
                        ufs_regiao = [
                            uf
                            for uf, reg in UF_TO_REGIAO.items()
                            if reg == regiao_unidade
                        ]
                        df_regiao = df_total_hab[
                            df_total_hab[coluna_uf].isin(ufs_regiao)
                        ]
                        tabela.append(
                            [
                                'REGIÃO',
                                regiao_unidade.title(),
                                formatar_populacao(len(df_regiao)),
                            ]
                        )

                    df_uf = df_total_hab[df_total_hab[coluna_uf] == uf_unidade]
                    tabela.append(
                        [
                            'UF',
                            uf_unidade.title(),
                            formatar_populacao(len(df_uf)),
                        ]
                    )

                    df_macro = df_total_hab[
                        df_total_hab[coluna_macro] == macro_unidade
                    ]
                    descricao_macro = get_descricao(macro_unidade)
                    tabela.append(
                        [
                            'MACRORREGIÃO',
                            descricao_macro,
                            formatar_populacao(len(df_macro)),
                        ]
                    )

                    df_regiao_saude = df_total_hab[
                        df_total_hab[coluna_regiao_saude]
                        == regiao_saude_unidade
                    ]
                    descricao_rs = get_descricao(regiao_saude_unidade)
                    tabela.append(
                        [
                            'REGIÃO DE SAUDE',
                            descricao_rs,
                            formatar_populacao(len(df_regiao_saude)),
                        ]
                    )

                    df_municipio = df_total_hab[
                        df_total_hab[coluna_municipio] == municipio_unidade
                    ]
                    tabela.append(
                        [
                            'MUNICIPIO',
                            municipio_unidade.title(),
                            formatar_populacao(len(df_municipio)),
                        ]
                    )

                    df_tipo_unidade = df_total_hab[
                        df_total_hab[coluna_tipo_unidade]
                        == tipo_unidade_unidade
                    ]
                    tabela.append(
                        [
                            'TIPO_UNIDADE',
                            tipo_unidade_unidade.title(),
                            formatar_populacao(len(df_tipo_unidade)),
                        ]
                    )

                    # Detalhe da Unidade
                    descricao_unidade = get_descricao(nome_fantasia_unidade)
                    tabela.append(
                        [
                            ' - CNES',
                            f'{descricao_unidade} ({cnes_selecionado})',
                            '1',
                        ]
                    )

                else:
                    # Unidade SEM habilitação (busca o contexto geográfico na base total)
                    df_unidade_geral = df[df[coluna_cnes] == cnes_selecionado]
                    if not df_unidade_geral.empty:
                        unidade_info = df_unidade_geral.iloc[0]
                        uf_unidade = unidade_info[coluna_uf]
                        macro_unidade = unidade_info[coluna_macro]
                        regiao_saude_unidade = unidade_info[
                            coluna_regiao_saude
                        ]
                        municipio_unidade = unidade_info[coluna_municipio]
                        regiao_unidade = UF_TO_REGIAO.get(
                            uf_unidade, 'NÃO IDENTIFICADA'
                        )

                        # Contexto geográfico (baseado na unidade total)
                        if regiao_unidade != 'NÃO IDENTIFICADA':
                            ufs_regiao = [
                                uf
                                for uf, reg in UF_TO_REGIAO.items()
                                if reg == regiao_unidade
                            ]
                            df_regiao = df_total_hab[
                                df_total_hab[coluna_uf].isin(ufs_regiao)
                            ]
                            tabela.append(
                                [
                                    'REGIÃO',
                                    regiao_unidade.title(),
                                    formatar_populacao(len(df_regiao)),
                                ]
                            )

                        df_uf = df_total_hab[
                            df_total_hab[coluna_uf] == uf_unidade
                        ]
                        tabela.append(
                            [
                                'UF',
                                uf_unidade.title(),
                                formatar_populacao(len(df_uf)),
                            ]
                        )

                        df_macro = df_total_hab[
                            df_total_hab[coluna_macro] == macro_unidade
                        ]
                        descricao_macro = get_descricao(macro_unidade)
                        tabela.append(
                            [
                                'MACRORREGIÃO',
                                descricao_macro,
                                formatar_populacao(len(df_macro)),
                            ]
                        )

                        df_regiao_saude = df_total_hab[
                            df_total_hab[coluna_regiao_saude]
                            == regiao_saude_unidade
                        ]
                        descricao_rs = get_descricao(regiao_saude_unidade)
                        tabela.append(
                            [
                                'REGIÃO DE SAUDE',
                                descricao_rs,
                                formatar_populacao(len(df_regiao_saude)),
                            ]
                        )

                        df_municipio = df_total_hab[
                            df_total_hab[coluna_municipio] == municipio_unidade
                        ]
                        tabela.append(
                            [
                                'MUNICIPIO',
                                municipio_unidade.title(),
                                formatar_populacao(len(df_municipio)),
                            ]
                        )

                        # Detalhe de Unidade (com zero)
                        tabela.append(['TIPO_UNIDADE', '-', '0'])
                        tabela.append(
                            [
                                ' - CNES',
                                'Unidade Selecionada não possui esta habilitação',
                                '0',
                            ]
                        )

            # 9. ADICIONA TABELA NA LISTA DE RESULTADOS
            if (
                len(tabela) > 2
            ):  # Verifica se tem mais que cabeçalho + nacional
                tabelas.append({'tipo_habilitacao': tipo_hab, 'dados': tabela})

        print(f'✅ Tabelas geradas com sucesso: {len(tabelas)} tabelas')
        return tabelas

    except Exception as e:
        print(f'❌ ERRO CRÍTICO em gerar_tabela_cnes_hab: {str(e)}')
        print(f'🔍 Traceback completo: {traceback.format_exc()}')
        return []
