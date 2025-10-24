// static/js/script.js - VERSÃO OTIMIZADA COM CORREÇÃO DO RESUMO
let dados = [];
let cacheUF = new Map();
let cacheMunicipios = new Map();

// Função para carregar dados otimizada
function loadBriefingData() {
  console.time('CarregamentoCSV');
  
  fetch("/db/cnes/unidade_cnes.csv")
    .then(response => {
      if (!response.ok) throw new Error('Erro ao carregar CSV');
      return response.text();
    })
    .then(csv => {
      dados = parseCSV(csv);
      console.timeEnd('CarregamentoCSV');
      console.log(`✅ CSV carregado: ${dados.length} registros`);
      
      // Pré-processa caches
      preProcessarCaches();
      updateUF();
      setupEventListeners();
    })
    .catch(error => {
      console.error('Erro ao carregar dados:', error);
      alert('Erro ao carregar dados do servidor');
    });
}

// PRÉ-PROCESSAMENTO - deixa tudo rápido
function preProcessarCaches() {
  console.time('PreProcessamento');
  
  // Cache de UFs por região
  cacheUF.clear();
  const regioes = [...new Set(dados.map(d => d.regiao))];
  regioes.forEach(regiao => {
    const ufs = [...new Set(dados
      .filter(d => d.regiao === regiao)
      .map(d => d.uf))].sort();
    cacheUF.set(regiao, ufs);
  });
  
  // Cache de municípios por UF
  cacheMunicipios.clear();
  const ufs = [...new Set(dados.map(d => d.uf))];
  ufs.forEach(uf => {
    const municipios = [...new Set(dados
      .filter(d => d.uf === uf)
      .map(d => d.municipio))].sort();
    cacheMunicipios.set(uf, municipios);
  });
  
  console.timeEnd('PreProcessamento');
  console.log(`🗂️  Caches pré-processados: ${cacheUF.size} regiões, ${cacheMunicipios.size} UFs`);
}

function parseCSV(csv) {
  const lines = csv.trim().split("\n");
  const headers = lines[0].split(";").map(h => h.replace(/"/g, "").trim());

  return lines.slice(1).map(line => {
    const values = line.split(";").map(v => v.replace(/"/g, "").trim());
    const obj = {};
    headers.forEach((h, i) => {
      obj[h] = values[i];
    });
    return {
      regiao: obj["REGIAO"],
      uf: obj["UF_DESC"],
      macro: obj["CO_MACROREGIAO_SAUDE"],
      regiaoSaude: obj["CO_REGIAO_SAUDE"],
      municipio: obj["MUNICIPIO"],
      unidade: `${obj["NOME_FANTASIA"]} - ${obj["CNES"]}`
    };
  });
}

function setupEventListeners() {
  document.getElementById("regiao").addEventListener("change", updateUF);
  document.getElementById("uf").addEventListener("change", function() {
    updateMacro();
    updateMunicipios(); // Atualiza municípios quando UF muda
  });
  document.getElementById("macro").addEventListener("change", updateRegiaoSaude);
  document.getElementById("regiaoSaude").addEventListener("change", updateMunicipios);
  document.getElementById("municipio").addEventListener("change", function() {
    updateUnidade();
    preencherHierarquiaSuperior(); // Preenche automaticamente a hierarquia
  });
}

function preencherHierarquiaSuperior() {
  const municipio = document.getElementById("municipio").value;
  const uf = document.getElementById("uf").value;
  
  if (municipio && municipio !== "TODOS") {
    // Encontra os dados completos do município selecionado
    const municipioData = dados.find(d => d.municipio === municipio && d.uf === uf);
    
    if (municipioData) {
      // Preenche a hierarquia superior automaticamente
      if (municipioData.regiao && document.getElementById("regiao").value === "TODOS") {
        document.getElementById("regiao").value = municipioData.regiao;
      }
      
      if (municipioData.macro && document.getElementById("macro").value === "TODOS") {
        document.getElementById("macro").value = municipioData.macro;
        updateRegiaoSaude(); // Atualiza regiões de saúde
      }
      
      if (municipioData.regiaoSaude && document.getElementById("regiaoSaude").value === "TODOS") {
        document.getElementById("regiaoSaude").value = municipioData.regiaoSaude;
      }
    }
  }
}

// FUNÇÕES OTIMIZADAS COM CACHE
function updateUF() {
  const regiao = document.getElementById("regiao").value;
  const ufSelect = document.getElementById("uf");
  
  ufSelect.innerHTML = '<option value="">TODOS</option>';
  ufSelect.disabled = true;

  // Usa cache em vez de filtrar tudo
  const ufs = cacheUF.get(regiao) || 
    [...new Set(dados.map(d => d.uf))].sort();

  ufs.forEach(uf => {
    const option = document.createElement("option");
    option.value = uf;
    option.textContent = uf;
    ufSelect.appendChild(option);
  });

  ufSelect.disabled = false;
  clearSelect("macro");
  clearSelect("regiaoSaude");
  clearSelect("municipio");
  clearSelect("unidade");
}

function updateMacro() {
  const uf = document.getElementById("uf").value;
  const macroSelect = document.getElementById("macro");
  macroSelect.innerHTML = '<option value="">TODOS</option>';

  const macros = [...new Set(dados
    .filter(d => !uf || d.uf === uf)
    .map(d => d.macro))].sort();

  macros.forEach(macro => {
    const option = document.createElement("option");
    option.value = macro;
    option.textContent = macro;
    macroSelect.appendChild(option);
  });

  clearSelect("regiaoSaude");
  clearSelect("municipio");
  clearSelect("unidade");
}

function updateRegiaoSaude() {
  const uf = document.getElementById("uf").value;
  const macro = document.getElementById("macro").value;
  const regiaoSaudeSelect = document.getElementById("regiaoSaude");
  regiaoSaudeSelect.innerHTML = '<option value="">TODOS</option>';

  const regioes = [...new Set(dados
    .filter(d => (!uf || d.uf === uf) && (!macro || d.macro === macro))
    .map(d => d.regiaoSaude))].sort();

  regioes.forEach(rs => {
    const option = document.createElement("option");
    option.value = rs;
    option.textContent = rs;
    regiaoSaudeSelect.appendChild(option);
  });

  clearSelect("municipio");
  clearSelect("unidade");
}

function updateMunicipios() {
  const uf = document.getElementById("uf").value;
  const macro = document.getElementById("macro").value;
  const regiaoSaude = document.getElementById("regiaoSaude").value;
  const municipioSelect = document.getElementById("municipio");
  municipioSelect.innerHTML = '<option value="">TODOS</option>';

  // Filtra municípios baseado na UF (obrigatório) e opcionalmente macro/região saúde
  const municipios = [...new Set(dados
    .filter(d => {
      // UF é obrigatório para mostrar municípios
      if (!uf) return false;
      
      const matchesUF = d.uf === uf;
      const matchesMacro = !macro || d.macro === macro;
      const matchesRegiaoSaude = !regiaoSaude || d.regiaoSaude === regiaoSaude;
      
      return matchesUF && matchesMacro && matchesRegiaoSaude;
    })
    .map(d => d.municipio))].sort();

  municipios.forEach(mun => {
    const option = document.createElement("option");
    option.value = mun;
    option.textContent = mun;
    municipioSelect.appendChild(option);
  });

  municipioSelect.disabled = !uf; // Só habilita se tiver UF selecionada
  clearSelect("unidade");
}

function updateUnidade() {
  const municipio = document.getElementById("municipio").value;
  const unidadeSelect = document.getElementById("unidade");
  unidadeSelect.innerHTML = '<option value="">TODOS</option>';

  const unidades = [...new Set(dados
    .filter(d => !municipio || d.municipio === municipio)
    .map(d => d.unidade))].sort();

  unidades.forEach(u => {
    const option = document.createElement("option");
    option.value = u;
    option.textContent = u;
    unidadeSelect.appendChild(option);
  });
}

function clearSelect(id) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = '<option value="">TODOS</option>';
}

// ✅ FUNÇÃO CORRIGIDA - Respeita a hierarquia corretamente
function gerarResumo() {
  const regiaoSelect = document.getElementById("regiao");
  const ufSelect = document.getElementById("uf");
  const macroSelect = document.getElementById("macro");
  const regiaoSaudeSelect = document.getElementById("regiaoSaude");
  const municipioSelect = document.getElementById("municipio");
  const unidadeSelect = document.getElementById("unidade");
  
  // Pega os valores atuais dos selects
  let regiao = regiaoSelect.value || "TODOS";
  let uf = ufSelect.value || "TODOS";
  let macro = macroSelect.value || "TODOS";
  let regiaoSaude = regiaoSaudeSelect.value || "TODOS";
  let municipio = municipioSelect.value || "TODOS";
  let unidade = unidadeSelect.value || "TODOS";

  // ✅ CORREÇÃO: Só preenche a hierarquia quando há seleção específica
  // e respeita o que já foi selecionado pelo usuário
  if (unidade && unidade !== "TODOS") {
    // Se tem unidade específica, busca todos os dados dessa unidade
    const unidadeData = dados.find(d => d.unidade === unidade);
    if (unidadeData) {
      regiao = unidadeData.regiao;
      uf = unidadeData.uf;
      macro = unidadeData.macro || "TODOS";
      regiaoSaude = unidadeData.regiaoSaude || "TODOS";
      municipio = unidadeData.municipio;
    }
  } else if (municipio && municipio !== "TODOS") {
    // Se tem município específico, busca os dados desse município
    const municipioData = dados.find(d => d.municipio === municipio && d.uf === uf);
    if (municipioData) {
      regiao = municipioData.regiao;
      uf = municipioData.uf;
      macro = municipioData.macro || "TODOS";
      regiaoSaude = municipioData.regiaoSaude || "TODOS";
    }
  } else if (regiaoSaude && regiaoSaude !== "TODOS") {
    // Se tem região de saúde específica, busca os dados dessa região
    const regiaoSaudeData = dados.find(d => d.regiaoSaude === regiaoSaude && d.uf === uf);
    if (regiaoSaudeData) {
      regiao = regiaoSaudeData.regiao;
      uf = regiaoSaudeData.uf;
      macro = regiaoSaudeData.macro || "TODOS";
    }
  } else if (macro && macro !== "TODOS") {
    // Se tem macrorregião específica, busca os dados dessa macrorregião
    const macroData = dados.find(d => d.macro === macro && d.uf === uf);
    if (macroData) {
      regiao = macroData.regiao;
      uf = macroData.uf;
    }
  } else if (uf && uf !== "TODOS") {
    // Se tem UF específica, busca a região correspondente
    const ufData = dados.find(d => d.uf === uf);
    if (ufData) {
      regiao = ufData.regiao;
    }
  }

  // ✅ GARANTE que nenhum campo fique undefined
  macro = macro || "TODOS";
  regiaoSaude = regiaoSaude || "TODOS";

  return `
    <strong>Briefing Gerado:</strong><br>
    Região: ${regiao}<br>
    UF: ${uf}<br>
    Macrorregião: ${macro}<br>
    Região de Saúde: ${regiaoSaude}<br>
    Município: ${municipio}<br>
    Unidade: ${unidade}<br>
  `;
}

// ✅ FUNÇÃO REMOVIDA - não é mais necessária
// function encontrarDadosCompletos(uf, municipio) {
//   if (municipio !== "TODOS") {
//     // Prioriza o município específico
//     return dados.find(d => d.municipio === municipio && d.uf === uf);
//   } else if (uf !== "TODOS") {
//     // Se só tem UF, pega o primeiro registro dessa UF para obter a região
//     return dados.find(d => d.uf === uf);
//   }
//   return null;
// }

function renderFilesCompleto(caminhoArquivo, nomeArquivo) {
  const resumo = gerarResumo();
  document.getElementById("filesSection_c").innerHTML = `
    <h2>Arquivos Gerados - COMPLETO</h2>
    <p>${resumo}</p>
    <ul>
      <li>
        <a href="/download/${caminhoArquivo}" download="${nomeArquivo}">
          <img src="/static/img/doc.png" alt="Documento" class="file-icon" />
          ${nomeArquivo}
        </a>
      </li>
      <li>
        <a href="#" onclick="gerarZipCompleto()">
          <img src="/static/img/zip.png" alt="Arquivo ZIP" class="file-icon" />
          Dados_C.zip
        </a>
      </li>
    </ul>
  `;
  document.getElementById("filesSection_c").style.display = "block";
}

function gerarZipCompleto() {
  alert("Funcionalidade de ZIP em desenvolvimento");
}

function renderFilesSimplificado(caminhoArquivo, nomeArquivo) {
  const resumo = gerarResumo();
  document.getElementById("filesSection_s").innerHTML = `
    <h2>Arquivos Gerados - SIMPLIFICADO</h2>
    <p>${resumo}</p>
    <ul>
      <li>
        <a href="/download/${caminhoArquivo}" download="${nomeArquivo}">
          <img src="/static/img/doc.png" alt="Documento" class="file-icon" />
          ${nomeArquivo}
        </a>
      </li>
      <li>
        <a href="#" onclick="gerarZipSimplificado()">
          <img src="/static/img/zip.png" alt="Arquivo ZIP" class="file-icon" />
          Dados_S.zip
        </a>
      </li>
    </ul>
  `;
  document.getElementById("filesSection_s").style.display = "block";
}

function gerarZipSimplificado() {
  alert("Funcionalidade de ZIP em desenvolvimento");
}

function animateProgressBar() {
  return new Promise((resolve) => {
    const fill = document.getElementById("progressFill");
    let progress = 0;
    const duration = 15000; // 15 segundos EXATOS
    const interval = 100;
    const step = 100 / (duration / interval);

    // RESETA a barra para 0%
    fill.style.width = '0%';
    fill.textContent = '0%';

    const timer = setInterval(() => {
      progress += step;
      if (progress >= 100) {
        progress = 100;
        clearInterval(timer);
        fill.style.width = `${progress}%`;
        fill.textContent = `${Math.round(progress)}%`;
        resolve();
      }
      fill.style.width = `${progress}%`;
      fill.textContent = `${Math.round(progress)}%`;
    }, interval);
  });
}

function startLoadingCompleto() {
  // Mostra a barra de progresso
  document.getElementById("progressSection").style.display = "block";
  
  // Coleta dados do formulário
  const dadosSelecao = {
    regiao: document.getElementById("regiao").value || "TODOS",
    uf: document.getElementById("uf").value || "TODOS",
    macro: document.getElementById("macro").value || "TODOS",
    regiaoSaude: document.getElementById("regiaoSaude").value || "TODOS",
    municipio: document.getElementById("municipio").value || "TODOS",
    unidade: document.getElementById("unidade").value || "TODOS"
  };
  
  console.log("Iniciando briefing COMPLETO - Barra de 10 segundos");

  // DESABILITA os botões durante o processamento
  document.querySelectorAll('.completo-btn, .simplif-btn, .clear-btn').forEach(btn => {
    btn.disabled = true;
    btn.style.opacity = '0.6';
  });

  // Inicia a animação da barra de progresso (10s FIXOS)
  const progressPromise = animateProgressBar();

  // Envia requisição para o backend (roda em paralelo)
  const fetchPromise = fetch('/gerar-briefing-completo', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(dadosSelecao)
  })
  .then(response => {
    if (!response.ok) {
      throw new Error('Erro na resposta do servidor');
    }
    return response.json();
  })
  .catch(error => {
    console.error('Erro no fetch:', error);
    return { success: false, error: error.message };
  });

  // AGUARDA EXATAMENTE 10 SEGUNDOS (barra completa) antes de mostrar resultados
  progressPromise.then(() => {
    // Após 10 segundos, verifica o resultado do fetch
    fetchPromise.then(data => {
      if (data.success) {
        renderFilesCompleto(data.arquivo, data.nomeArquivo);
      } else {
        alert('Erro ao gerar briefing: ' + data.error);
      }
    }).finally(() => {
      // Reabilita os botões (sempre após 10s)
      document.querySelectorAll('.completo-btn, .simplif-btn, .clear-btn').forEach(btn => {
        btn.disabled = false;
        btn.style.opacity = '1';
      });
      
      // Esconde a barra de progresso
      document.getElementById("progressSection").style.display = "none";
    });
  });
}

function startLoadingSimplificado() {
  console.log("🎬 INICIANDO briefing simplificado...");
  
  // ✅ PRIMEIRO: Esconde resultados anteriores
  document.getElementById("filesSection_c").style.display = "none";
  document.getElementById("filesSection_s").style.display = "none";
  
  // Mostra a barra de progresso
  document.getElementById("progressSection").style.display = "block";
  
  // Coleta dados do formulário
  const dadosSelecao = {
    regiao: document.getElementById("regiao").value || "TODOS",
    uf: document.getElementById("uf").value || "TODOS",
    macro: document.getElementById("macro").value || "TODOS",
    regiaoSaude: document.getElementById("regiaoSaude").value || "TODOS",
    municipio: document.getElementById("municipio").value || "TODOS",
    unidade: document.getElementById("unidade").value || "TODOS"
  };
  
  console.log("📤 Enviando dados:", dadosSelecao);

  // DESABILITA os botões durante o processamento
  document.querySelectorAll('.completo-btn, .simplif-btn, .clear-btn').forEach(btn => {
    btn.disabled = true;
    btn.style.opacity = '0.6';
  });

  // Inicia a animação da barra de progresso (15s FIXOS)
  const progressPromise = animateProgressBar();

  // Envia requisição para o backend (roda em paralelo)
  const fetchPromise = fetch('/gerar-briefing-simplificado', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(dadosSelecao)
  })
  .then(response => {
    console.log("📥 Resposta recebida do servidor");
    if (!response.ok) {
      throw new Error('Erro na resposta do servidor');
    }
    return response.json();
  })
  .catch(error => {
    console.error('❌ Erro no fetch:', error);
    return { success: false, error: error.message };
  });

  // ✅ ESTRATÉGIA MELHOR: Aguarda a barra E o fetch
  Promise.all([progressPromise, fetchPromise])
    .then(([_, data]) => {
      console.log("✅ Ambos completos: barra (15s) + fetch");
      
      if (data.success) {
        console.log("🎉 Sucesso! Renderizando resultados...");
        renderFilesSimplificado(data.arquivo, data.nomeArquivo);
      } else {
        console.error("❌ Erro do backend:", data.error);
        alert('Erro ao gerar briefing: ' + data.error);
      }
    })
    .catch(error => {
      console.error('❌ Erro geral:', error);
      alert('Erro ao conectar com o servidor: ' + error.message);
    })
    .finally(() => {
      // ✅ SEMPRE reabilita e esconde a barra (mesmo em caso de erro)
      document.querySelectorAll('.completo-btn, .simplif-btn, .clear-btn').forEach(btn => {
        btn.disabled = false;
        btn.style.opacity = '1';
      });
      
      document.getElementById("progressSection").style.display = "none";
      console.log("🔚 Processamento finalizado");
    });
}

function clearForm() {
  document.querySelectorAll("select").forEach(select => select.selectedIndex = 0);
  document.getElementById("progressSection").style.display = "none";
  document.getElementById("filesSection_c").style.display = "none";
  document.getElementById("filesSection_s").style.display = "none";
  updateUF();
}

function logout() {
  window.location.href = '/';
}

// Inicialização específica para cada página
if (window.location.pathname === '/briefing' || window.location.pathname.includes('briefing_app.html')) {
  document.addEventListener("DOMContentLoaded", loadBriefingData);
}

// Lógica de login (apenas para index.html)
if (window.location.pathname === '/') {
  document.addEventListener("DOMContentLoaded", function() {
    const loginForm = document.getElementById('loginForm');
    if (loginForm) {
      loginForm.addEventListener('submit', function(event) {
        event.preventDefault();

        const CORRECT_USER = 'COQAE';
        const CORRECT_PASS = '123456';

        const usernameInput = document.getElementById('username').value.trim();
        const passwordInput = document.getElementById('password').value.trim();
        const errorMessage = document.getElementById('errorMessage');

        if (usernameInput === CORRECT_USER && passwordInput === CORRECT_PASS) {
          errorMessage.textContent = '';
          errorMessage.style.display = 'none';
          window.location.href = '/briefing';
        } else {
          errorMessage.textContent = 'Usuário ou senha de demonstração incorretos.';
          errorMessage.style.display = 'block';
        }
      });
    }

    // Lógica do modal "esqueceu sua senha"
    window.openForgotModal = function() {
      document.getElementById('forgotModal').style.display = 'flex';
    }

    window.closeForgotModal = function() {
      document.getElementById('forgotModal').style.display = 'none';
      document.getElementById('forgotForm').reset();
      document.getElementById('forgotMessage').style.display = 'none';
      document.getElementById('forgotForm').style.display = 'block';
    }

    window.submitForgotForm = function() {
      const emailInput = document.getElementById('forgotEmail');
      const forgotMessage = document.getElementById('forgotMessage');
      const forgotForm = document.getElementById('forgotForm');

      if (emailInput.value.trim() && forgotForm.checkValidity()) {
        forgotForm.style.display = 'none';
        forgotMessage.textContent = '✅ Seu e-mail foi enviado com sucesso. Entraremos em contato em breve.';
        forgotMessage.style.display = 'block';

        setTimeout(function() {
          closeForgotModal();
        }, 5000);
      } else {
        alert("Por favor, preencha o campo de e-mail corretamente.");
      }
    }

    // Fechar modal clicando fora
    window.onclick = function(event) {
      const modal = document.getElementById('forgotModal');
      if (event.target === modal) {
        closeForgotModal();
      }
    }
  });
}