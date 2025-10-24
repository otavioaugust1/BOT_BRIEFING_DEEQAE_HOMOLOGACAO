# Sistema de Geração Automática de Briefing (BOT_BRIEFING_DEEQAE)

## ⚠️ Aviso: Acesso Restrito - BOT BRIEFING DEEQAE

Este repositório contém scripts e códigos para o desenvolvimento de um sistema web interno. O uso e o acesso são **restritos ao ambiente local da coordenação**.

### Acesso ao Sistema

Embora o sistema seja restrito ao ambiente de desenvolvimento/coordenação, ele está hospedado para visualização/teste no seguinte endereço:

**URL:** [https://bot-briefing-deeqae.onrender.com/](https://bot-briefing-deeqae.onrender.com/)

## 🎯 Objetivo do Projeto

O objetivo principal do projeto é desenvolver um sistema web que consolide dados de diversas fontes institucionais (como IBGE e outras bases de dados de saúde) para **gerar automaticamente documentos de *briefing***.

A saída será estruturada no mesmo formato do modelo em Word fornecido pelo cliente (ex.: *briefing* do Ceará), incluindo:

* Texto analítico e narrativo.
* Quadros, tabelas e anexos extraídos de bases de dados oficiais.
* Planilhas auxiliares em Excel (levantamentos tabulares).

## ⚙️ Escopo e Funcionalidades

Este sistema visa automatizar e padronizar a geração de *briefings*, oferecendo as seguintes funcionalidades:

### 1. Consolidação e Cruzamento de Dados

* **Níveis Territoriais:** Consolidação de dados em diferentes níveis territoriais: Brasil, UF, Região, Macrorregião, Região de Saúde, Município e Unidade CNES.
* **Fontes de Dados:** Utilização de dados do IBGE para UF, Região, Macrorregião, Região de Saúde e Município.
* **Consultas:** Scripts em Python integrados a bancos de dados (Oracle, Redshift, Postgres), além de APIs e sites oficiais para realizar consultas e cruzamentos de dados.
* **Indicadores:** Consolidação automática de indicadores como produção, valores financeiros, propostas e estabelecimentos habilitados.
* **Atualização:** Consultas que podem ser atualizadas conforme datas de referência.

### 2. Geração de Documentos

* **Documento Principal (Word):** Geração automática de documentos finais em Word, com a mesma estrutura do modelo de *briefing* fornecido.
* **Tabelas de Apoio (Excel):** Exportação de levantamentos em Excel, organizados de forma tabular.
* **Texto Narrativo:** Uso de Inteligência Artificial para gerar automaticamente partes do texto.

## 📂 Bases de Dados e Fontes Previstas

O sistema integrará dados de diversas fontes oficiais, como:

* InvestSUS
* SIA - Sistema de Informação Ambulatorial
* SIH - Sistema de Informações Hospitalares
* CNES - Cadastro Nacional de Estabelecimentos de Saúde
* RNDS - Rede Nacional de Dados em Saúde
* TABWIN/TABNET
* IBGE - Instituto Brasileiro de Geografia e Estatística
* Fontes complementares setoriais ou APIs institucionais.

## 🛠️ Instalação e Uso (Modelo Preliminar)

Este projeto usa Flask para desenvolver o servidor web e depende de scripts Python para orquestrar consultas e gerar documentos.

### Pré-requisitos

* **Python:** Versão 3.x.
* **Acesso a Bancos de Dados:** Credenciais e permissões para acesso garantido aos bancos de dados.
* **Bibliotecas:** Instalação das bibliotecas Python necessárias para o Flask, conexão com bancos de dados, manipulação de dados e geração de documentos.

### Instalação

#### Clone o repositório

```bash
git clone https://github.com/otavioaugust1/BOT_BRIEFING_DEEQAE.git
cd BOT_BRIEFING_DEEQAE
```

#### Crie e ative um ambiente virtual (recomendado)

Se você ainda não tem um ambiente virtual configurado, pode criá-lo com o comando:

```bash
python -m venv venv
```

Para ativá-lo:

* **Windows**:

  ```bash
  venv\Scripts\activate
  ```
* **Linux/Mac**:

  ```bash
  source venv/bin/activate
  ```

#### Instale as dependências

```bash
pip install -r requirements.txt
```

### Configuração e Execução

1. **Configuração de Credenciais:** As chaves de acesso e credenciais para bancos (Oracle, Redshift, Postgres) devem ser configuradas de forma segura no ambiente local.

2. **Rodando o servidor Flask:** Para iniciar o servidor Flask, basta rodar o arquivo `app.py` com o seguinte comando:

```bash
python app.py
```

O Flask iniciará o servidor localmente no endereço:

```
http://127.0.0.1:5000/
```

### Execução do Gerador

O script principal do gerador de briefing pode ser executado via Flask (através de uma rota configurada) ou diretamente pelo terminal.

#### Exemplo de execução do script de geração (via terminal)

Os parâmetros de entrada devem ser definidos conforme a necessidade do *briefing*:

```bash
python run_briefing_generator.py --territorio <UF/Município> --nivel <Nivel> --data_ref <YYYY-MM-DD>
```

#### Exemplo de execução via Flask

A execução via Flask pode ser configurada em uma rota específica, onde o usuário pode enviar os parâmetros diretamente pelo navegador ou por meio de uma chamada API. Para isso, será necessário implementar uma rota no arquivo `app.py` que acione o gerador de briefing.

### Saída

Os documentos gerados serão salvos em uma pasta local:

* Documento de *Briefing* Padronizado (.docx)
* Tabelas em Excel de apoio (.xlsx)

## 🚧 Restrições e Premissas

### Restrições

* **Banco de Dados:** Não será criado banco de dados permanente; será utilizado um banco temporário para processamento e geração do *briefing*.
* **Recursos Visuais:** Não será utilizado imagens, mapas ou gráficos nesta versão.
* **Ambiente de Uso:** Restrito ao ambiente local da coordenação.
* **Recurso Humano:** O projeto contará com a dedicação de 1 técnico em tempo integral.

### Premissas

* Acesso garantido às bases de dados (credenciais e permissões).
* Apoio do cliente para definição dos indicadores prioritários.
* O modelo final do Word seguirá o padrão já disponibilizado (ex.: *briefing* CEARA).
* Dados complementares podem ser incorporados até a coleta de ideias (03/10/2025).

## 🗓️ Cronograma

| Data              | Entregável                                                          |
| ----------------- | ------------------------------------------------------------------- |
| **03/10/2025**    | Apresentação dos requisitos e coleta de ideias com o cliente        |
| **10/10/2025**    | Protótipo preliminar: prévia do briefing em Word + tabelas em Excel |
| **17/10/2025**    | Entrega da primeira versão viável do briefing automático            |
| **17-24/10/2025** | Semana de ajustes e correções de inconformidades                    |
| **24/10/2025**    | Entrega final do briefing automático e apresentação ao departamento |

## 👥 Autores e Colaboradores

* **Responsável Técnico:** Otavio Augusto / COQAE
* **Coordenação / Cliente:** Gabriela Neves / COQAE
* **Equipe de Apoio:** DEEQAE
* **Elaboração e Revisão do Documento:** Otavio Augusto / COQAE e Gabriela Neves / COQAE

## 📧 Contato

Se você deseja colaborar ou tem alguma dúvida sobre o projeto, entre em contato:

* **E-mail:** [otavioaugust@saude.gov.br](mailto:otavioaugust@saude.gov.br) ou [otavio.santos@saude.gov.br](mailto:otavio.santos@saude.gov.br)
* **GitHub:** [https://github.com/otavioaugust1/BOT_BRIEFING_DEEQAE](https://github.com/otavioaugust1/BOT_BRIEFING_DEEQAE)
* **Portfólio:** [https://otavioaugust1.github.io/Meu_portfolio/](https://otavioaugust1.github.io/Meu_portfolio/)

