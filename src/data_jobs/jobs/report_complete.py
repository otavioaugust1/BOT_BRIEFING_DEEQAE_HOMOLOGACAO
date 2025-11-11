# ==============================================================================
#             CONFIGURAÇÃO DO RELATORIO COMPLETO - REGRA (10 PAGINAS)
# ==============================================================================

# report_complete.py
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING
from docx.shared import Pt
# Importa configurações compartilhadas
from report_configuration import (BASE_DIR, adicionar_cabecalho_com_logo,
                                  encontrar_hierarquia_completa,
                                  gerar_nome_governador, gerar_nome_prefeito,
                                  gerar_nome_secretario,
                                  verificar_arquivo_existente)

from src.data_jobs.jobs.processing_geral import (  # ✅ NOVA FUNÇÃO ADICIONADA
    gerar_descricao_demografica, gerar_tabela_cnes_hab)
from src.data_jobs.jobs.processing_geral_2 import (  # ✅ NOVA FUNÇÃO ADICIONADA
    gerar_tabela_cnes_srv, gerar_tabela_sia_oci)


# Informação inicial
def add_info_paragraph_formatado(doc, titulo, valor):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(0)
    p.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE

    run_titulo = p.add_run(f'• {titulo}: ')
    run_titulo.bold = True
    run_valor = p.add_run(valor)


# Espaço antes do rodapé
def add_rodape_paragraph(doc, texto):
    p = doc.add_paragraph(texto)
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(0)


# fonte_configuração
def add_fonte_paragraph(doc, partes):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(0)
    for texto, estilo in partes:
        run = p.add_run(texto)
        run.bold = estilo.get('bold', False)
        run.italic = estilo.get('italic', False)
        run.font.size = Pt(8)
        run.font.name = 'Arial'


# ==============================================================================
#                           RELATORIO COMPLETO
# ==============================================================================


def gerar_documento_briefing_completo(dados_selecao):
    """Gera documento completo com base nos dados de seleção"""

    # Verifica se já existe um arquivo gerado hoje
    caminho_existente, nome_arquivo = verificar_arquivo_existente(
        dados_selecao, 'COMPLETO'
    )
    if caminho_existente:
        return caminho_existente, nome_arquivo

    # Encontra a hierarquia completa baseada na seleção
    dados_completos = encontrar_hierarquia_completa(dados_selecao)

    regiao = dados_completos.get('regiao', 'TODOS')
    uf = dados_completos.get('uf', 'TODOS')
    macro = dados_completos.get('macro', 'TODOS')
    regiao_saude = dados_completos.get('regiaoSaude', 'TODOS')
    municipio = dados_completos.get('municipio', 'TODOS')
    unidade = dados_completos.get('unidade', 'TODOS')

    print(f'🔄 Gerando NOVO briefing COMPLETO com hierarquia completa:')
    print(f'   Região: {regiao}, UF: {uf}, Macro: {macro}')
    print(
        f'   Região Saúde: {regiao_saude}, Município: {municipio}, Unidade: {unidade}'
    )

    # Caminho de saída
    caminho_saida = os.path.join(
        BASE_DIR, '..', '..', '..', 'static', 'downloads', nome_arquivo
    )
    os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)

    # Criação do documento
    doc = Document()
    adicionar_cabecalho_com_logo(doc)

    titulo = doc.add_heading('BRIEFING COMPLETO - SISTEMA COQAE/DEEQAE', 0)
    titulo.alignment = WD_ALIGN_PARAGRAPH.CENTER

    doc.add_heading('DADOS DA SELEÇÃO E CONTEXTUALIZAÇÃO', level=1)

    add_info_paragraph_formatado(doc, 'Região', regiao)
    add_info_paragraph_formatado(doc, 'UF', uf)
    add_info_paragraph_formatado(doc, 'Macrorregião de Saúde', macro)
    add_info_paragraph_formatado(doc, 'Região de Saúde', regiao_saude)
    add_info_paragraph_formatado(doc, 'Município', municipio)
    add_info_paragraph_formatado(doc, 'Unidade', unidade)

    # Informações de gestão
    if municipio != 'TODOS' and uf != 'TODOS':
        add_info_paragraph_formatado(
            doc, 'Prefeito(a)', gerar_nome_prefeito(municipio, uf)
        )
        add_info_paragraph_formatado(
            doc, 'Secretário(a) de Saúde', gerar_nome_secretario(municipio, uf)
        )
    elif municipio == 'TODOS' and uf != 'TODOS':
        add_info_paragraph_formatado(
            doc, 'Governador(a)', gerar_nome_governador(uf)
        )

    # Linha divisória
    linha = doc.add_paragraph('_' * 50)
    linha.alignment = WD_ALIGN_PARAGRAPH.CENTER
    linha.paragraph_format.space_before = Pt(0)
    linha.paragraph_format.space_after = Pt(0)
    linha.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE

    # Resumo Executivo
    doc.add_heading('RESUMO EXECUTIVO EXPANDIDO', level=1)
    doc.add_paragraph(
        'Este briefing, elaborado no contexto do programa "Agora Tem Especialistas", oferece uma visão geral da situação de saúde na região selecionada. A partir da consolidação de dados públicos e oficiais, o documento reúne informações relevantes sobre a estrutura, cobertura e desempenho dos serviços de saúde, contribuindo para o entendimento do cenário local e subsidiando a atuação dos profissionais especializados.'
    )

    # Dados Demográficos
    doc.add_heading('DADOS DEMOGRÁFICOS - IBGE', level=1)
    texto_demografico = gerar_descricao_demografica(dados_selecao)
    doc.add_paragraph(texto_demografico)

    # ✅ NOVA SEÇÃO: HABILITAÇÕES CNES
    doc.add_heading('HABILITAÇÕES CNES - DISTRIBUIÇÃO HIERÁRQUICA', level=1)

    try:
        tabelas_cnes_hab = gerar_tabela_cnes_hab(dados_selecao)

        if tabelas_cnes_hab:
            for tabela_info in tabelas_cnes_hab:
                # Título do tipo de habilitação
                tipo_hab = tabela_info['tipo_habilitacao']
                doc.add_heading(tipo_hab.title(), level=2)

                # Cria a tabela no Word
                dados_tabela = tabela_info['dados']
                tabela = doc.add_table(rows=len(dados_tabela), cols=3)
                tabela.style = 'Table Grid'

                # Preenche a tabela
                for i, linha in enumerate(dados_tabela):
                    for j, valor in enumerate(linha):
                        tabela.cell(i, j).text = str(valor)

                # Formatação da tabela
                for row in tabela.rows:
                    for cell in row.cells:
                        paragraph = cell.paragraphs[0]
                        paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT

                # Alinha a coluna QUANT à direita
                for i in range(len(dados_tabela)):
                    cell = tabela.cell(i, 2)
                    paragraph = cell.paragraphs[0]
                    paragraph.alignment = WD_ALIGN_PARAGRAPH.RIGHT

                doc.add_paragraph()  # Espaço entre tabelas
        else:
            doc.add_paragraph(
                'Não foram encontradas habilitações CNES para os critérios selecionados.'
            )

    except Exception as e:
        print(f'⚠️ Aviso: Erro ao gerar tabela CNES: {e}')
        doc.add_paragraph(
            'Dados de habilitações CNES temporariamente indisponíveis.'
        )

    # ✅ NOVA SEÇÃO: Núcleos de Gestão do Cuidado - NGC - CNES
    doc.add_heading(
        'NÚCLEOS DE GESTÃO DO CUIDADO - DISTRIBUIÇÃO HIERÁRQUICA', level=1
    )

    try:
        tabelas_cnes_hab = gerar_tabela_cnes_srv(dados_selecao)

        if tabelas_cnes_hab:
            for tabela_info in tabelas_cnes_hab:
                # Título do tipo de habilitação
                tipo_hab = tabela_info['tipo_habilitacao']
                doc.add_heading(tipo_hab.title(), level=2)

                # Cria a tabela no Word
                dados_tabela = tabela_info['dados']
                tabela = doc.add_table(rows=len(dados_tabela), cols=3)
                tabela.style = 'Table Grid'

                # Preenche a tabela
                for i, linha in enumerate(dados_tabela):
                    for j, valor in enumerate(linha):
                        tabela.cell(i, j).text = str(valor)

                # Formatação da tabela
                for row in tabela.rows:
                    for cell in row.cells:
                        paragraph = cell.paragraphs[0]
                        paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT

                # Alinha a coluna QUANT à direita
                for i in range(len(dados_tabela)):
                    cell = tabela.cell(i, 2)
                    paragraph = cell.paragraphs[0]
                    paragraph.alignment = WD_ALIGN_PARAGRAPH.RIGHT

                doc.add_paragraph()  # Espaço entre tabelas
        else:
            doc.add_paragraph(
                'Não foram encontrada serviço NGC CNES para os critérios selecionados.'
            )

    except Exception as e:
        print(f'⚠️ Aviso: Erro ao gerar tabela CNES: {e}')
        doc.add_paragraph(
            'Dados de serviço CNES temporariamente indisponíveis.'
        )

    # ✅ NOVA SEÇÃO: Núcleos de Gestão do Cuidado - OCI - SIA
    doc.add_heading('PRODUÇÃO - OCI - DISTRIBUIÇÃO HIERÁRQUICA', level=1)

    try:
        tabelas_oci = gerar_tabela_sia_oci(dados_selecao)

        # Dicionário de subgrupos com descrições
        subgrupos_descricao = {
            '0901': '- Atenção em Oncologia',
            '0902': '- Atenção em Cardiologia',
            '0903': '- Atenção em Ortopedia',
            '0904': '- Atenção em Otorrinolaringologia',
            '0905': '- Atenção em Oftalmologia',
            '0906': '- Atenção em Saude Mulher'
        }

        if tabelas_oci:
            for tabela_info in tabelas_oci:
                # Obtém o código do subgrupo
                subgrupo = tabela_info['subgrupo_procedimento']
                
                # Busca a descrição no dicionário, se existir
                descricao = subgrupos_descricao.get(subgrupo, '')
                
                # Adiciona o título com código e descrição
                doc.add_heading(f'Subgrupo: {subgrupo} {descricao}', level=2)


                # Cria a tabela no Word
                dados_tabela = tabela_info['dados']
                # CORREÇÃO: A tabela OCI tem 4 colunas, não 3
                tabela = doc.add_table(
                    rows=len(dados_tabela), cols=4
                )  # Mudei de 3 para 4 colunas
                tabela.style = 'Table Grid'

                # Preenche a tabela
                for i, linha in enumerate(dados_tabela):
                    for j, valor in enumerate(linha):
                        tabela.cell(i, j).text = str(valor)

                # Formatação da tabela
                for row in tabela.rows:
                    for cell in row.cells:
                        paragraph = cell.paragraphs[0]
                        paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT

                # Alinha as colunas QUANT e VALOR à direita (CORREÇÃO AQUI)
                for i in range(len(dados_tabela)):
                    # Coluna 2: QUANTIDADE APROVADA
                    cell_quant = tabela.cell(i, 2)
                    paragraph_quant = cell_quant.paragraphs[0]
                    paragraph_quant.alignment = WD_ALIGN_PARAGRAPH.RIGHT

                    # Coluna 3: VALOR APROVADO (R$)
                    cell_valor = tabela.cell(i, 3)
                    paragraph_valor = cell_valor.paragraphs[0]
                    paragraph_valor.alignment = WD_ALIGN_PARAGRAPH.RIGHT

                doc.add_paragraph()  # Espaço entre tabelas
        else:
            doc.add_paragraph(
                'Não foram encontradas OCIs para os critérios selecionados.'
            )

    except Exception as e:
        print(f'⚠️ Aviso: Erro ao gerar tabela OCI: {e}')
        doc.add_paragraph('Dados de OCI temporariamente indisponíveis.')

    # Recomendações Estratégicas
    doc.add_heading('RECOMENDAÇÕES ESTRATÉGICAS', level=1)

    recomendacoes = [
        'Ampliar a cobertura de atenção primária em áreas de maior vulnerabilidade',
        'Fortalecer a rede de atenção psicossocial na região',
        'Implementar programa de telemedicina para especialidades de difícil acesso',
        'Capacitar profissionais para gestão de crônicos',
        'Otimizar a distribuição de medicamentos essenciais',
        'Fortalecer vigilância em saúde com foco em doenças emergentes',
    ]

    for i, recomendacao in enumerate(recomendacoes, 1):
        doc.add_paragraph(f'{i}. {recomendacao}')

    # Metadados Expandidos
    doc.add_heading('METADADOS EXPANDIDOS', level=1)
    doc.add_paragraph(
        f'• Data de Geração: {datetime.now().strftime("%d/%m/%Y às %H:%M:%S")}'
    )
    doc.add_paragraph(f'• Tipo: Briefing Completo')
    doc.add_paragraph(f'• Arquivo: {nome_arquivo}')
    doc.add_paragraph(f'• Sistema: COQAE/DEEQAE - Ministério da Saúde')
    doc.add_paragraph(f'• Período de Análise: 2023-2025')

    # Fontes:
    doc.add_heading('FONTE DOS DADOS', level=1)

    add_fonte_paragraph(
        doc,
        [
            ('--- CONASEMS. ', {}),
            ('Rede COSEMS – Dados. ', {'bold': True}),
            (
                'Disponível em: https://portal.conasems.org.br/rede-cosems/dados. Acesso em: ',
                {},
            ),
            (datetime.now().strftime('%d/%m/%Y às %H:%M:%S'), {}),
        ],
    )

    add_fonte_paragraph(
        doc,
        [
            ('--- BRASIL. Ministério da Saúde. ', {}),
            ('Programa Agora Tem Especialistas – InvestSUS. ', {'bold': True}),
            (
                'Disponível em: https://investsuspaineis.saude.gov.br/extensions/CGIN_PMAE/CGIN_PMAE.html#. Acesso em: ',
                {},
            ),
            (datetime.now().strftime('%d/%m/%Y às %H:%M:%S'), {}),
        ],
    )

    add_fonte_paragraph(
        doc,
        [
            ('--- BRASIL. Ministério da Saúde. ', {}),
            ('Painel PNRF – DRAC. ', {'bold': True}),
            (
                'Disponível em: https://controleavaliacao.saude.gov.br/painel/pnrf.',
                {},
            ),
        ],
    )

    add_fonte_paragraph(
        doc,
        [
            ('--- BRASIL. Ministério da Saúde. ', {}),
            (
                'Macrorregiões e Regiões de Saúde – SEIDIGI/DEMAS. ',
                {'bold': True},
            ),
            (
                'Disponível em: https://infoms.saude.gov.br/extensions/SEIDIGI_DEMAS_MACRORREGIOES/SEIDIGI_DEMAS_MACRORREGIOES.html. Acesso em: ',
                {},
            ),
            (datetime.now().strftime('%d/%m/%Y às %H:%M:%S'), {}),
        ],
    )

    add_fonte_paragraph(
        doc,
        [
            ('--- BRASIL. Ministério da Saúde. ', {}),
            ('Rede Nacional de Dados em Saúde – RNDS. ', {'bold': True}),
            ('Disponível em: https://rnds-guia.saude.gov.br/.', {}),
        ],
    )

    add_fonte_paragraph(
        doc,
        [
            ('--- BRASIL. ', {}),
            ('DATASUS. ', {'italic': True}),
            (
                'Cadastro Nacional de Estabelecimentos de Saúde – CNES. ',
                {'bold': True},
            ),
            ('Disponível em: https://cnes.datasus.gov.br/. Acesso em: ', {}),
            (datetime.now().strftime('%d/%m/%Y às %H:%M:%S'), {}),
        ],
    )

    add_fonte_paragraph(
        doc,
        [
            ('--- BRASIL. ', {}),
            ('DATASUS. ', {'italic': True}),
            ('Sistema de Informações Ambulatoriais – SIA. ', {'bold': True}),
            (
                'Disponível em: http://sia.datasus.gov.br/principal/index.php.',
                {},
            ),
        ],
    )

    add_fonte_paragraph(
        doc,
        [
            ('--- BRASIL. ', {}),
            ('DATASUS. ', {'italic': True}),
            ('Sistema de Informações Hospitalares – SIH. ', {'bold': True}),
            ('Disponível em: http://sih.datasus.gov.br/.', {}),
        ],
    )

    add_fonte_paragraph(
        doc,
        [
            ('--- BRASIL. ', {}),
            (
                'Instituto Brasileiro de Geografia e Estatística – IBGE. ',
                {'bold': True},
            ),
            (
                'Portal IBGE. Disponível em: https://www.ibge.gov.br/. Acesso em: ',
                {},
            ),
            (datetime.now().strftime('%d/%m/%Y às %H:%M:%S'), {}),
        ],
    )

    # Espaço antes do rodapé
    doc.add_paragraph('\n' * 3)

    # Linha divisória
    linha = doc.add_paragraph('_' * 50)
    linha.alignment = WD_ALIGN_PARAGRAPH.CENTER
    linha.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
    linha.paragraph_format.space_before = Pt(0)
    linha.paragraph_format.space_after = Pt(0)

    # Rodapé visual com espaçamento ajustado
    add_rodape_paragraph(
        doc, 'Sistema de Geração Automática de Briefing - COQAE/DEEQAE'
    )
    add_rodape_paragraph(doc, 'Ministério da Saúde - Brasil')
    add_rodape_paragraph(
        doc, 'Departamento de Economia da Saúde e Gestão Estratégica'
    )
    add_rodape_paragraph(
        doc,
        f'Documento gerado automaticamente em: {datetime.now().strftime("%d/%m/%Y às %H:%M:%S")}',
    )

    # Salva o documento
    doc.save(caminho_saida)
    print(f'✅ NOVO documento completo salvo em: {caminho_saida}')

    # Retorna caminho relativo para o Flask
    caminho_relativo = os.path.join('static', 'downloads', nome_arquivo)
    return caminho_relativo, nome_arquivo
