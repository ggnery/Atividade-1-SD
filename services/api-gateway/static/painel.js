/* Painel de Leitos — cliente SSE (design 8.7.3, 8.7.4, R11).
 *
 * O navegador NAO contem regra clinica: desenha `severidade` e `score_news2` como vieram do evento
 * (design 8.7.7). O `EventSource` reconecta sozinho (campo `retry` do servidor); um vigia de 30 s cobre
 * a conexao "meio aberta" (proxy que nao fecha o socket). Reconciliacao por `snapshot`, nao por replay.
 */
"use strict";

(function () {
  var URL_STREAM = "/painel/stream";
  var LIMITE_ALERTAS = 50;
  var TIMEOUT_VIGIA_MS = 30000;

  // Estado local — espelho da projecao do servidor.
  var leitos = new Map();          // leito_id -> card
  var elementosCard = new Map();   // leito_id -> <article>
  var alertas = [];                // mais recente primeiro
  var fonte = null;
  var estadoConexao = "";
  var ultimaMensagem = Date.now();

  // Elementos fixos.
  var elMural = document.getElementById("mural");
  var elMuralVazio = document.getElementById("mural-vazio");
  var elListaAlertas = document.getElementById("lista-alertas");
  var elAlertasVazio = document.getElementById("alertas-vazio");
  var elNLeitos = document.getElementById("n-leitos");
  var elNAlertas = document.getElementById("n-alertas");
  var elRelogio = document.getElementById("relogio");
  var elConexao = document.getElementById("conexao");
  var elUltimoEvento = document.getElementById("ultimo-evento");
  var elAviso = document.getElementById("aviso-hidratacao");

  // ------------------------------------------------------------- utilitarios

  function escapar(txt) {
    if (txt === null || txt === undefined) return "";
    return String(txt)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  function doisDig(n) { return (n < 10 ? "0" : "") + n; }

  function horaLocal(iso) {
    var d = iso ? new Date(iso) : new Date();
    if (isNaN(d.getTime())) d = new Date();
    return doisDig(d.getHours()) + ":" + doisDig(d.getMinutes()) + ":" + doisDig(d.getSeconds());
  }

  function idadeTexto(iso) {
    if (!iso) return "sem leitura";
    var ms = Date.now() - new Date(iso).getTime();
    if (isNaN(ms) || ms < 0) return "agora";
    var s = Math.floor(ms / 1000);
    if (s < 60) return "há " + s + " s";
    var m = Math.floor(s / 60);
    if (m < 60) return "há " + m + " min";
    return "há " + Math.floor(m / 60) + " h";
  }

  var ROTULO_SEV = { normal: "NORMAL", baixa: "BAIXA", media: "MEDIA", alta: "ALTA" };
  function classeSev(sev) { return "sev-" + (ROTULO_SEV[sev] ? sev : "normal"); }
  function rotuloSev(sev) { return ROTULO_SEV[sev] || "NORMAL"; }

  /**
   * Estado visual do card, separando AUSENCIA DE DADO de risco clinico.
   *
   * O dominio (services/comum/news2.py) so conhece baixa, media e alta. O valor "normal" que a
   * projecao usa como padrao significa "ainda nao ha leitura", e nao "paciente avaliado e sem
   * risco" -- desenhar a palavra NORMAL num leito que ninguem mediu faz um monitor mudo parecer um
   * paciente estavel, que e o pior erro possivel num painel clinico. `score_news2 === null` e o
   * discriminador confiavel: so existe escore depois que o triage-service avaliou uma leitura.
   */
  function estadoCard(card) {
    if (card.estado === "livre") return { classe: "sev-livre", rotulo: "LIVRE" };
    if (card.score_news2 === null || card.score_news2 === undefined) {
      return { classe: "sev-sem-leitura", rotulo: "SEM LEITURA" };
    }
    return { classe: classeSev(card.severidade), rotulo: rotuloSev(card.severidade) };
  }

  // ---------------------------------------------------------------- conexao

  function marcarConexao(estado) {
    if (estado === estadoConexao) return;
    estadoConexao = estado;
    elConexao.className = "conexao conexao--" + estado;
    var rot = { conectado: "conectado", reconectando: "reconectando…", offline: "offline" }[estado] || estado;
    elConexao.querySelector(".rotulo").textContent = rot;
  }

  // Token de painel (enfermeiro/medico/admin) guardado pelo Console para o fallback `?token=` de
  // 8.7.5: se a demonstracao trocar para `auditor`, o cookie `hmq_session` passa a nao servir ao
  // stream (403) e o mural morreria na proxima reconexao. Com o token guardado, o mural sobrevive.
  var tokenStream = null;

  function conectar() {
    try { if (fonte) fonte.close(); } catch (e) { /* ignora */ }
    var url = tokenStream ? URL_STREAM + "?token=" + encodeURIComponent(tokenStream) : URL_STREAM;
    fonte = new EventSource(url, { withCredentials: true });

    fonte.onopen = function () { ultimaMensagem = Date.now(); marcarConexao("conectado"); };
    fonte.onerror = function () {
      marcarConexao(fonte && fonte.readyState === EventSource.CLOSED ? "offline" : "reconectando");
    };
    fonte.addEventListener("snapshot", function (e) { tocar(); redesenharTudo(ler(e)); });
    fonte.addEventListener("leito", function (e) { tocar(); aplicarLeito(ler(e)); });
    fonte.addEventListener("alerta", function (e) { tocar(); aplicarAlerta(ler(e)); });
    fonte.addEventListener("ping", function () { tocar(); marcarConexao("conectado"); });
  }

  function tocar() { ultimaMensagem = Date.now(); }

  function ler(e) {
    try { return JSON.parse(e.data); } catch (err) { return null; }
  }

  // --------------------------------------------------------------- render

  function redesenharTudo(snap) {
    if (!snap) return;
    leitos.clear();
    elementosCard.clear();
    elMural.querySelectorAll(".card").forEach(function (n) { n.remove(); });
    (snap.leitos || []).forEach(function (c) { leitos.set(c.leito_id, c); upsertCard(c); });
    alertas = (snap.alertas || []).slice(0, LIMITE_ALERTAS);
    renderAlertas(null);
    atualizarContadores();
    sincronizarSelects();
    verificarMonitores();
    marcarConexao("conectado");
    elAviso.hidden = snap.hidratada !== false;
    if (snap.hidratada === false) {
      elAviso.textContent = "projeção não reconciliada no boot — aguardando eventos do Broker";
    }
  }

  function aplicarLeito(card) {
    if (!card || !card.leito_id) return;
    var antes = leitos.get(card.leito_id);
    leitos.set(card.leito_id, card);
    upsertCard(card);
    atualizarContadores();
    if (!antes || antes.estado !== card.estado ||
        antes.paciente_nome !== card.paciente_nome ||
        antes.internacao_id !== card.internacao_id) {
      sincronizarSelects();
    }
    // Alta/liberacao do leito derruba o monitoramento continuo; o resto so reflete o novo escore.
    verificarMonitores();
    ultimoEvento("leito.atualizado", card.correlation_id);
  }

  function aplicarAlerta(al) {
    if (!al || !al.alerta_id) return;
    var idx = -1;
    for (var i = 0; i < alertas.length; i++) {
      if (alertas[i].alerta_id === al.alerta_id) { idx = i; break; }
    }
    var novo = idx < 0;
    if (novo) {
      alertas.unshift(al);
      if (alertas.length > LIMITE_ALERTAS) alertas.pop();
    } else {
      alertas[idx] = al;
    }
    renderAlertas(novo ? al.alerta_id : null);
    atualizarContadores();
    ultimoEvento("alerta." + (al.estado || "gerado"), al.correlation_id);
  }

  function upsertCard(card) {
    var el = elementosCard.get(card.leito_id);
    var novo = !el;
    if (novo) {
      el = document.createElement("article");
      el.dataset.leito = card.leito_id;
      elementosCard.set(card.leito_id, el);
    }
    el.className = "card " + estadoCard(card).classe + (card.estado === "livre" ? " card--livre" : "");
    el.dataset.atualizado = card.atualizado_em || "";
    el.innerHTML = htmlCard(card);
    if (novo) inserirOrdenado(el, card.leito_id);
    if (elMuralVazio) elMuralVazio.hidden = leitos.size > 0;
  }

  function inserirOrdenado(el, leitoId) {
    var existentes = elMural.querySelectorAll(".card");
    for (var i = 0; i < existentes.length; i++) {
      if ((existentes[i].dataset.leito || "") > leitoId) {
        elMural.insertBefore(el, existentes[i]);
        return;
      }
    }
    elMural.appendChild(el);
  }

  function parSinal(k, valor, sufixo) {
    var v = (valor === null || valor === undefined || valor === "") ? "—" : escapar(valor) + (sufixo || "");
    return '<div class="par"><span class="k">' + k + '</span><span class="v">' + v + "</span></div>";
  }

  function htmlCard(card) {
    var s = card.sinais || {};
    var ec = estadoCard(card);
    var alta = ec.rotulo === "ALTA";
    var badge = '<span class="badge">' + (alta ? "⚠ " : "") + ec.rotulo + "</span>";

    var topo =
      '<div class="card__topo"><div><span class="card__leito">' + escapar(card.leito_id) + "</span>" +
      (card.setor ? ' <span class="card__setor">' + escapar(card.setor) + "</span>" : "") +
      "</div>" + badge + "</div>";

    if (card.estado === "livre") {
      return topo + '<div class="card__paciente"><span>(livre)</span></div>';
    }

    var paciente =
      '<div class="card__paciente"><span class="card__nome">' +
      escapar(card.paciente_nome || "paciente") + "</span>" +
      (card.internacao_id ? '<span class="int">int. ' + escapar(String(card.internacao_id).slice(0, 8)) + "</span>" : "") +
      "</div>";

    var escore =
      '<div class="card__escore"><span class="num">' +
      (card.score_news2 === null || card.score_news2 === undefined ? "—" : escapar(card.score_news2)) +
      '</span><span class="rot">NEWS2</span></div>' +
      (card.componente_critico ? '<div class="card__critico">[!] componente crítico = 3</div>' : "");

    var sinais =
      '<div class="sinais">' +
      parSinal("FR", s.frequencia_respiratoria) +
      parSinal("SpO₂", s.saturacao_o2, "%") +
      parSinal("T", s.temperatura) +
      parSinal("PAS", s.pressao_sistolica) +
      parSinal("FC", s.frequencia_cardiaca) +
      parSinal("AVPU", s.nivel_consciencia) +
      "</div>" +
      '<div class="card__o2">O₂ suplementar: <b>' +
      (s.oxigenio_suplementar === undefined ? "—" : (s.oxigenio_suplementar ? "sim" : "não")) + "</b></div>";

    var tarja = "";
    if (card.ultima_rejeicao) {
      var r = card.ultima_rejeicao;
      tarja = '<div class="tarja-rejeicao">última leitura rejeitada: ' +
        escapar(r.campo || "campo") + " — " + escapar(r.motivo || "fora da faixa") + "</div>";
    }

    var despacho = "";
    if (card.ultimo_alerta_estado && card.ultimo_alerta_estado !== "nenhum") {
      despacho = '<span class="despacho despacho--' + escapar(card.ultimo_alerta_estado) + '">' +
        escapar(card.ultimo_alerta_estado) + "</span>";
    }

    var rodape =
      '<div class="card__rodape"><span class="idade">' + idadeTexto(card.atualizado_em) + "</span>" +
      despacho + "</div>";

    return topo + paciente + escore + (card.sinais ? sinais : "") + tarja + rodape;
  }

  function renderAlertas(idRealce) {
    elListaAlertas.innerHTML = "";
    alertas.forEach(function (a) {
      var li = document.createElement("li");
      li.className = "alerta " + classeSev(a.severidade) + (a.alerta_id === idRealce ? " alerta--realce" : "");
      var estado = a.estado || "gerado";
      li.innerHTML =
        '<div class="alerta__linha1"><span>' + escapar(a.leito_id) + "</span>" +
        '<span class="alerta__hora">' + horaLocal(a.gerado_em) + "</span></div>" +
        '<div class="alerta__linha2">NEWS2 <b>' + escapar(a.score_news2) + "</b> · " +
        rotuloSev(a.severidade) + " · <span class=\"despacho despacho--" + escapar(estado) + '">' +
        escapar(estado) + "</span></div>" +
        '<div class="alerta__paciente">' + escapar(a.paciente_nome || "—") + "</div>";
      elListaAlertas.appendChild(li);
    });
    if (elAlertasVazio) elAlertasVazio.hidden = alertas.length > 0;
  }

  function atualizarContadores() {
    elNLeitos.textContent = leitos.size;
    elNAlertas.textContent = alertas.length;
  }

  function ultimoEvento(tipo, correlationId) {
    elUltimoEvento.innerHTML = "último evento: <b>" + escapar(tipo) +
      "</b> · correlation_id <b>" + escapar(correlationId || "—") + "</b>";
  }

  // ------------------------------------------------------- relogio + vigia

  function tickSegundo() {
    elRelogio.textContent = horaLocal();
    elMural.querySelectorAll(".card[data-atualizado]").forEach(function (el) {
      var span = el.querySelector(".idade");
      if (span) span.textContent = idadeTexto(el.dataset.atualizado);
    });
    // Vigia: sem mensagem ha 30 s, forca reconexao (cobre conexao "meio aberta", design 8.7.4).
    if (fonte && Date.now() - ultimaMensagem > TIMEOUT_VIGIA_MS) {
      marcarConexao("reconectando");
      conectar();
      ultimaMensagem = Date.now();
    }
  }

  /* ======================================================================= *
   *  CONSOLE DE OPERACAO — gaveta retratil (mesma pagina, sem framework).   *
   *                                                                        *
   *  Emite as MESMAS chamadas HTTP que o roteiro de curl da apresentacao:   *
   *  POST /auth/token, POST /pacientes, POST /internacoes, POST /sinais e   *
   *  POST /internacoes/{id}/alta. O token JWT vive apenas em memoria (nunca *
   *  em localStorage/sessionStorage) e some ao recarregar a pagina.         *
   *  Nada aqui usa innerHTML com dado vindo da API (createElement/          *
   *  textContent apenas).                                                   *
   * ======================================================================= */

  var LIMITE_LOG = 20;

  // Papeis que o /painel/stream aceita (design 8.7.5) — define quando guardar `tokenStream`.
  var PAPEIS_PAINEL = ["enfermeiro", "medico", "admin"];

  var NOMES_FICTICIOS = [
    "Ana Paula Ribeiro", "Bruno Carvalho", "Carla Menezes", "Diego Antunes",
    "Elisa Fontenele", "Fabio Moraes", "Gabriela Nunes", "Heitor Vasques",
    "Isadora Prado", "Joana Beltrao", "Kleber Amancio", "Larissa Tavares",
    "Marcelo Quinta", "Natalia Bastos", "Otavio Serrano", "Priscila Damasco"
  ];

  // Presets de sinais vitais. Os escores seguem services/comum/news2.py:
  // alta = total >= 5 ou componente isolado 3; media = 3..4; baixa <= 2.
  var PRESETS = {
    "estavel": {
      rotulo: "Estável", frequencia_respiratoria: 16, saturacao_o2: 98,
      oxigenio_suplementar: false, temperatura: 36.6, pressao_sistolica: 120,
      frequencia_cardiaca: 72, nivel_consciencia: "A"
    },
    "atencao": {
      rotulo: "Atenção", frequencia_respiratoria: 21, saturacao_o2: 95,
      oxigenio_suplementar: false, temperatura: 36.8, pressao_sistolica: 120,
      frequencia_cardiaca: 80, nivel_consciencia: "A"
    },
    "critico": {
      rotulo: "Crítico", frequencia_respiratoria: 28, saturacao_o2: 88,
      oxigenio_suplementar: true, temperatura: 39.2, pressao_sistolica: 88,
      frequencia_cardiaca: 131, nivel_consciencia: "V"
    },
    "fora-de-faixa": {
      rotulo: "Fora de faixa", frequencia_respiratoria: 16, saturacao_o2: 20,
      oxigenio_suplementar: false, temperatura: 36.6, pressao_sistolica: 120,
      frequencia_cardiaca: 72, nivel_consciencia: "A"
    }
  };

  // Oito leituras que pioram progressivamente: baixa -> media -> alta (com componente critico).
  var DETERIORACAO = [
    { frequencia_respiratoria: 16, saturacao_o2: 98, oxigenio_suplementar: false, temperatura: 36.6, pressao_sistolica: 122, frequencia_cardiaca: 74, nivel_consciencia: "A" },
    { frequencia_respiratoria: 18, saturacao_o2: 97, oxigenio_suplementar: false, temperatura: 37.0, pressao_sistolica: 118, frequencia_cardiaca: 80, nivel_consciencia: "A" },
    { frequencia_respiratoria: 20, saturacao_o2: 96, oxigenio_suplementar: false, temperatura: 37.5, pressao_sistolica: 116, frequencia_cardiaca: 86, nivel_consciencia: "A" },
    { frequencia_respiratoria: 21, saturacao_o2: 95, oxigenio_suplementar: false, temperatura: 37.8, pressao_sistolica: 114, frequencia_cardiaca: 88, nivel_consciencia: "A" },
    { frequencia_respiratoria: 22, saturacao_o2: 94, oxigenio_suplementar: false, temperatura: 38.2, pressao_sistolica: 112, frequencia_cardiaca: 90, nivel_consciencia: "A" },
    { frequencia_respiratoria: 23, saturacao_o2: 93, oxigenio_suplementar: true, temperatura: 38.5, pressao_sistolica: 105, frequencia_cardiaca: 98, nivel_consciencia: "A" },
    { frequencia_respiratoria: 25, saturacao_o2: 91, oxigenio_suplementar: true, temperatura: 38.9, pressao_sistolica: 96, frequencia_cardiaca: 114, nivel_consciencia: "A" },
    { frequencia_respiratoria: 28, saturacao_o2: 88, oxigenio_suplementar: true, temperatura: 39.2, pressao_sistolica: 88, frequencia_cardiaca: 131, nivel_consciencia: "V" }
  ];

  var INTERVALO_SEQUENCIA_MS = 1500;

  // Estado do console (token em memoria — design 8.3.1; nada persistido no navegador).
  var sessao = { token: null, usuario: null, papel: null };
  var registros = [];       // ultimas chamadas, mais recente primeiro
  var sequenciaAtiva = null;

  var el = {};
  ["btn-operar", "console", "op-fechar", "op-usuario", "op-senha", "op-entrar", "op-sair",
   "op-recarregar", "op-sessao-estado", "op-apikey", "op-nome", "op-documento", "op-nascimento",
   "op-sexo", "op-leito-livre", "op-admitir", "op-novo-nome", "op-leito-ocupado", "op-sequencia",
   "op-cancelar-sequencia", "op-progresso", "op-intervalo", "op-escalada", "op-continuo",
   "op-parar-todos", "op-continuo-estado", "op-leito-alta", "op-motivo-alta", "op-alta",
   "op-log", "op-log-vazio", "op-limpar-log", "op-copiado"
  ].forEach(function (id) { el[id] = document.getElementById(id); });

  // ------------------------------------------------------------- utilidades

  function texto(tag, classe, conteudo) {
    var n = document.createElement(tag);
    if (classe) n.className = classe;
    if (conteudo !== undefined && conteudo !== null) n.textContent = String(conteudo);
    return n;
  }

  function sortear(lista) { return lista[Math.floor(Math.random() * lista.length)]; }

  function agoraISO() { return new Date().toISOString(); }

  function estadoSessao(mensagem, tipo) {
    el["op-sessao-estado"].textContent = mensagem;
    el["op-sessao-estado"].className = "op-estado op-estado--" + (tipo || "neutro");
  }

  function aviso(mensagem) {
    el["op-copiado"].textContent = mensagem || "";
    if (mensagem) {
      window.setTimeout(function () {
        if (el["op-copiado"].textContent === mensagem) el["op-copiado"].textContent = "";
      }, 4000);
    }
  }

  function exigirSessao() {
    if (sessao.token) return true;
    estadoSessao("Entre com um usuário antes de operar.", "erro");
    el["op-usuario"].focus();
    return false;
  }

  // ------------------------------------------------------------------- log

  function copiarTexto(valor) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(valor);
    }
    return new Promise(function (resolve, reject) {
      var area = document.createElement("textarea");
      area.value = valor;
      area.setAttribute("readonly", "");
      area.style.position = "fixed";
      area.style.opacity = "0";
      document.body.appendChild(area);
      area.select();
      var ok = false;
      try { ok = document.execCommand("copy"); } catch (e) { ok = false; }
      document.body.removeChild(area);
      ok ? resolve() : reject(new Error("copia indisponivel"));
    });
  }

  function blocoProblema(problema) {
    // RFC 7807 (design 8.4) exibido como esta no fio — o contrato de erro e parte da demonstracao.
    var dl = texto("dl", "op-problema");
    var campos = [
      ["type", problema.type], ["title", problema.title],
      ["status", problema.status], ["detail", problema.detail]
    ];
    campos.forEach(function (par) {
      if (par[1] === undefined || par[1] === null || par[1] === "") return;
      dl.appendChild(texto("dt", null, par[0]));
      dl.appendChild(texto("dd", null, par[1]));
    });
    if (Array.isArray(problema.errors) && problema.errors.length) {
      dl.appendChild(texto("dt", null, "errors[]"));
      problema.errors.forEach(function (e) {
        dl.appendChild(texto("dd", null, (e.campo || "?") + " — " + (e.mensagem || "")));
      });
    }
    dl.appendChild(texto("p", "op-problema-rfc", "corpo application/problem+json (RFC 7807)"));
    return dl;
  }

  function itemLog(reg) {
    var li = texto("li", "op-log-item op-log-item--" + reg.classe);

    var linha1 = texto("div", "op-log-linha1");
    linha1.appendChild(texto("span", "op-rota", reg.metodo + " " + reg.rota));
    var st = texto("span", "op-status op-status--" + reg.classe, reg.rotuloStatus);
    st.setAttribute("title", reg.rotuloStatus + " · " + reg.hora);
    linha1.appendChild(st);
    li.appendChild(linha1);

    if (reg.cid) {
      var botao = texto("button", "op-cid", "correlation_id " + reg.cid);
      botao.type = "button";
      botao.setAttribute("title", "Clique para copiar o correlation_id");
      botao.addEventListener("click", function () {
        // O retorno visual e SINCRONO: `navigator.clipboard.writeText` pode ficar pendente quando
        // o documento perde o foco, e na apresentacao ninguem pode ficar sem resposta na tela.
        reg.trace = "./scripts/trace.sh " + reg.cid;
        mostrarTrace(li, reg.trace);
        aviso("correlation_id copiado — cole o comando no terminal");
        copiarTexto(reg.cid).catch(function () {
          aviso("copie manualmente: " + reg.cid);
        });
      });
      li.appendChild(botao);
    }

    if (reg.problema) li.appendChild(blocoProblema(reg.problema));
    if (reg.trace) mostrarTrace(li, reg.trace);
    return li;
  }

  function mostrarTrace(li, comando) {
    var antigo = li.querySelector(".op-trace");
    if (antigo) antigo.remove();
    li.appendChild(texto("code", "op-trace", comando));
  }

  function renderLog() {
    el["op-log"].textContent = "";
    registros.forEach(function (reg) { el["op-log"].appendChild(itemLog(reg)); });
    el["op-log-vazio"].hidden = registros.length > 0;
  }

  function registrar(metodo, rota, status, cid, corpo, erroRede) {
    var classe = erroRede ? "rede" : (status >= 200 && status < 300 ? "ok" : "erro");
    var reg = {
      metodo: metodo,
      rota: rota,
      classe: classe,
      rotuloStatus: erroRede ? "rede" : String(status),
      hora: horaLocal(),
      cid: cid || "",
      problema: null,
      trace: null
    };
    if (erroRede) {
      reg.problema = { type: "(sem resposta)", title: "Falha de rede", detail: String(erroRede) };
    } else if (classe === "erro" && corpo && typeof corpo === "object") {
      reg.problema = corpo;
    }
    registros.unshift(reg);
    if (registros.length > LIMITE_LOG) registros.pop();
    renderLog();
    return reg;
  }

  // ------------------------------------------------------------------ HTTP

  function chamar(metodo, rota, opcoes) {
    opcoes = opcoes || {};
    var cabecalhos = {};
    if (opcoes.corpo !== undefined) cabecalhos["Content-Type"] = "application/json";
    if (opcoes.apiKey) {
      cabecalhos["X-API-Key"] = (el["op-apikey"].value || "").trim();
    } else if (sessao.token) {
      cabecalhos["Authorization"] = "Bearer " + sessao.token;
    }

    var init = { method: metodo, headers: cabecalhos, credentials: "same-origin" };
    if (opcoes.corpo !== undefined) init.body = JSON.stringify(opcoes.corpo);

    return fetch(rota, init).then(function (resp) {
      return resp.text().then(function (bruto) {
        var dados = null;
        try { dados = bruto ? JSON.parse(bruto) : null; } catch (e) { dados = null; }
        var cid = resp.headers.get("X-Correlation-ID") ||
          (dados && dados.correlation_id) || "";
        registrar(metodo, rota, resp.status, cid, dados, null);
        return { ok: resp.ok, status: resp.status, dados: dados, cid: cid };
      });
    }, function (erro) {
      registrar(metodo, rota, 0, "", null, erro && erro.message ? erro.message : erro);
      return { ok: false, status: 0, dados: null, cid: "", rede: true };
    });
  }

  function resumoErro(res, prefixo) {
    var d = res.dados || {};
    var titulo = d.title || (res.rede ? "falha de rede" : "erro " + res.status);
    var detalhe = d.detail ? " — " + d.detail : "";
    return prefixo + ": " + res.status + " " + titulo + detalhe;
  }

  // ------------------------------------------------------- selects de leito

  function opcao(valor, rotulo) {
    var o = document.createElement("option");
    o.value = valor;
    o.textContent = rotulo;
    return o;
  }

  function preencherSelect(elemento, itens, vazio) {
    if (!elemento) return;
    var anterior = elemento.value;
    elemento.textContent = "";
    if (!itens.length) {
      elemento.appendChild(opcao("", vazio));
      return;
    }
    itens.forEach(function (i) { elemento.appendChild(opcao(i.valor, i.rotulo)); });
    for (var k = 0; k < elemento.options.length; k++) {
      if (elemento.options[k].value === anterior) { elemento.value = anterior; return; }
    }
  }

  /** Repopula os selects a partir da projecao local (mesma verdade dos cards do mural). */
  function sincronizarSelects() {
    if (!el["op-leito-livre"]) return;
    var livres = [];
    var ocupados = [];
    Array.from(leitos.values()).sort(function (a, b) {
      return a.leito_id < b.leito_id ? -1 : (a.leito_id > b.leito_id ? 1 : 0);
    }).forEach(function (c) {
      if (c.estado === "livre") {
        livres.push({ valor: c.leito_id, rotulo: c.leito_id + " · " + (c.setor || "—") });
      } else {
        ocupados.push({
          valor: c.leito_id,
          rotulo: c.leito_id + " · " + (c.paciente_nome || "paciente")
        });
      }
    });
    preencherSelect(el["op-leito-livre"], livres, "— sem leitos livres —");
    preencherSelect(el["op-leito-ocupado"], ocupados, "— sem leitos ocupados —");
    preencherSelect(el["op-leito-alta"], ocupados, "— sem leitos ocupados —");
    // O rotulo do botao de monitoramento depende de qual leito esta selecionado agora.
    atualizarBotaoContinuo();
  }

  /** GET /leitos (R11.7) — registra a chamada no log e reconcilia os selects. */
  function recarregarLeitos() {
    return chamar("GET", "/leitos", {}).then(function (res) {
      if (!res.ok) {
        estadoSessao(resumoErro(res, "GET /leitos recusado"), "erro");
        return res;
      }
      var itens = (res.dados && res.dados.itens) || [];
      itens.forEach(function (c) { if (c && c.leito_id) leitos.set(c.leito_id, c); });
      sincronizarSelects();
      return res;
    });
  }

  // -------------------------------------------------------------- 1. sessao

  function entrar(usuario, senha) {
    return chamar("POST", "/auth/token", {
      corpo: { usuario: usuario, senha: senha }
    }).then(function (res) {
      if (!res.ok) {
        sessao = { token: null, usuario: null, papel: null };
        estadoSessao(resumoErro(res, "Login recusado"), "erro");
        marcarPapelAtivo();
        return res;
      }
      sessao = {
        token: res.dados.access_token,
        usuario: usuario,
        papel: res.dados.role
      };
      estadoSessao("Sessão ativa: " + usuario + " · papel " + res.dados.role +
        " · token em memória (expira em " + res.dados.expires_in + " s)", "ok");
      marcarPapelAtivo();

      // Mantem o mural vivo mesmo com papel `auditor` (que o /painel/stream recusa).
      if (PAPEIS_PAINEL.indexOf(res.dados.role) >= 0) {
        tokenStream = res.dados.access_token;
        conectar();
        recarregarLeitos();
      } else {
        aviso("papel " + res.dados.role + " não enxerga /leitos nem o stream — mural preservado");
      }
      return res;
    });
  }

  function marcarPapelAtivo() {
    document.querySelectorAll(".btn--chip[data-usuario]").forEach(function (b) {
      b.setAttribute("aria-pressed", b.dataset.usuario === sessao.usuario ? "true" : "false");
    });
  }

  function sair() {
    sessao = { token: null, usuario: null, papel: null };
    marcarPapelAtivo();
    estadoSessao("Sessão encerrada — token descartado da memória.", "neutro");
  }

  // ------------------------------------------------------------- 2. admitir

  function novoPaciente() {
    el["op-nome"].value = sortear(NOMES_FICTICIOS);
    el["op-documento"].value = "DOC-" + Date.now();
  }

  function admitir() {
    if (!exigirSessao()) return Promise.resolve();
    var leito = el["op-leito-livre"].value;
    if (!leito) { estadoSessao("Escolha um leito livre.", "erro"); return Promise.resolve(); }

    var corpoPaciente = {
      nome: (el["op-nome"].value || "").trim(),
      documento: (el["op-documento"].value || "").trim(),
      data_nascimento: el["op-nascimento"].value,
      sexo: el["op-sexo"].value
    };

    el["op-admitir"].disabled = true;
    return chamar("POST", "/pacientes", { corpo: corpoPaciente }).then(function (res1) {
      if (!res1.ok) {
        estadoSessao(resumoErro(res1, "POST /pacientes"), "erro");
        return null;
      }
      return chamar("POST", "/internacoes", {
        corpo: {
          paciente_id: res1.dados.paciente_id,
          leito_id: leito,
          equipe_responsavel: "Equipe A",
          motivo: "Admissão pela demonstração do Console"
        }
      }).then(function (res2) {
        if (!res2.ok) {
          estadoSessao(resumoErro(res2, "POST /internacoes"), "erro");
          return null;
        }
        estadoSessao("Admitido: " + corpoPaciente.nome + " em " + res2.dados.leito_id +
          " (" + res2.dados.setor + ")", "ok");
        novoPaciente();
        return res2;
      });
    }).then(function (r) {
      el["op-admitir"].disabled = false;
      return r;
    }, function (e) {
      el["op-admitir"].disabled = false;
      throw e;
    });
  }

  // --------------------------------------------------------------- 3. sinais

  function corpoSinais(leitoId, leitura) {
    return {
      leito_id: leitoId,
      frequencia_respiratoria: leitura.frequencia_respiratoria,
      saturacao_o2: leitura.saturacao_o2,
      oxigenio_suplementar: leitura.oxigenio_suplementar,
      // O contrato aceita no maximo 1 casa decimal (SinaisVitaisRequest.temperatura).
      temperatura: Number(Number(leitura.temperatura).toFixed(1)),
      pressao_sistolica: leitura.pressao_sistolica,
      frequencia_cardiaca: leitura.frequencia_cardiaca,
      nivel_consciencia: leitura.nivel_consciencia,
      coletado_em: agoraISO()
    };
  }

  function publicarSinais(leitoId, leitura) {
    // POST /sinais e autenticado por X-API-Key (papel `dispositivo`, R4.5) — nunca por JWT.
    return chamar("POST", "/sinais", { corpo: corpoSinais(leitoId, leitura), apiKey: true });
  }

  function enviarPreset(chave) {
    var preset = PRESETS[chave];
    var leito = el["op-leito-ocupado"].value;
    if (!preset) return Promise.resolve();
    if (!leito) { estadoSessao("Escolha um leito ocupado.", "erro"); return Promise.resolve(); }
    return publicarSinais(leito, preset).then(function (res) {
      if (res.ok) {
        var extra = chave === "fora-de-faixa"
          ? " — a borda aceitou; o vitals-service vai recusar e mandar para a DLQ (R6.6)"
          : "";
        estadoSessao("202 " + preset.rotulo + " em " + leito + extra, "ok");
      } else {
        estadoSessao(resumoErro(res, "POST /sinais"), "erro");
      }
      return res;
    });
  }

  function pararSequencia(mensagem) {
    if (sequenciaAtiva && sequenciaAtiva.timer) window.clearTimeout(sequenciaAtiva.timer);
    sequenciaAtiva = null;
    el["op-cancelar-sequencia"].disabled = true;
    el["op-sequencia"].disabled = false;
    if (mensagem) el["op-progresso"].textContent = mensagem;
  }

  function iniciarSequencia() {
    if (sequenciaAtiva) return;
    var leito = el["op-leito-ocupado"].value;
    if (!leito) { estadoSessao("Escolha um leito ocupado.", "erro"); return; }
    if (monitores.has(leito)) {
      pararMonitor(leito, null);
      aviso("monitoramento contínuo de " + leito + " parado: a sequência assumiu o leito");
    }

    sequenciaAtiva = { leito: leito, indice: 0, timer: null };
    el["op-sequencia"].disabled = true;
    el["op-cancelar-sequencia"].disabled = false;
    estadoSessao("Deterioração em " + leito + ": o card muda de cor a cada leitura.", "ok");

    function passo() {
      if (!sequenciaAtiva) return;
      var atual = sequenciaAtiva;
      var i = atual.indice;
      el["op-progresso"].textContent = (i + 1) + "/" + DETERIORACAO.length;
      publicarSinais(atual.leito, DETERIORACAO[i]).then(function () {
        if (sequenciaAtiva !== atual) return;   // cancelada durante o voo
        atual.indice += 1;
        if (atual.indice >= DETERIORACAO.length) {
          pararSequencia(DETERIORACAO.length + "/" + DETERIORACAO.length + " concluída");
          return;
        }
        atual.timer = window.setTimeout(passo, INTERVALO_SEQUENCIA_MS);
      });
    }
    passo();
  }

  /* ===================================================================== *
   *  3b. MONITORAMENTO CONTINUO — o leito que "vive".                     *
   *                                                                      *
   *  Diferente da "Sequência de deterioração" (8 leituras fixas e para),  *
   *  este modo publica POST /sinais em laco indefinido: os valores mudam  *
   *  a cada leitura (ruido) e a GRAVIDADE sobe devagar com o tempo, ate   *
   *  o card atravessar baixa -> media -> alta sozinho.                    *
   *                                                                      *
   *  O console continua SEM regra clinica (design 8.7.7): a trajetoria    *
   *  abaixo foi calibrada offline contra a tabela NEWS2 de                *
   *  services/comum/news2.py, mas o escore e a severidade exibidos vem    *
   *  sempre da projecao do servidor (evento `leito`), nunca de calculo    *
   *  no navegador.                                                        *
   * ===================================================================== */

  // Faixas fisiologicas de aceitacao (espelho de FAIXAS_FISIOLOGICAS, services/comum/news2.py).
  // Todo valor gerado aqui e grampeado nestes limites: o modo continuo NUNCA produz dado invalido
  // — gerar leitura impossivel e trabalho exclusivo do preset "Fora de faixa" (R6.6).
  var FAIXAS_ACEITACAO = {
    frequencia_respiratoria: [0, 80],
    saturacao_o2: [50, 100],
    temperatura: [25, 45],
    pressao_sistolica: [40, 300],
    frequencia_cardiaca: [20, 250]
  };

  /* Ancoras da trajetoria continua: `g` e a gravidade normalizada (0 = admissao tranquila,
   * 1 = quadro critico). Entre duas ancoras os valores sao interpolados linearmente, entao a
   * evolucao e continua — nao ha "passo" visivel como na tabela de 8 leituras.
   *
   * Escore NEWS2 esperado nas ancoras (tabela de 7.5.1, conferida por curl na verificacao):
   *   g=0.00 -> 0   baixa
   *   g=0.20 -> 1   baixa
   *   g=0.35 -> 2   baixa
   *   g=0.50 -> 3   MEDIA   (SpO2 94 = 1, T 38.3 = 1, FC 100 = 1)
   *   g=0.68 -> 4   media
   *   g=0.82 -> 6   ALTA    (FR 22 = 2, SpO2 92 = 2, T = 1, FC = 1)
   *   g=1.00 -> 17  alta + componente isolado 3 (FR >= 25, SpO2 <= 91, AVPU V)
   */
  var TRAJETORIA_CONTINUA = [
    { g: 0.00, fr: 15, spo2: 98, temp: 36.6, pas: 122, fc: 70 },
    { g: 0.20, fr: 18, spo2: 95, temp: 37.4, pas: 120, fc: 86 },
    { g: 0.35, fr: 19, spo2: 95, temp: 37.8, pas: 118, fc: 95 },
    { g: 0.50, fr: 19, spo2: 94, temp: 38.3, pas: 117, fc: 100 },
    { g: 0.68, fr: 19, spo2: 93, temp: 38.6, pas: 116, fc: 105 },
    { g: 0.82, fr: 22, spo2: 92, temp: 38.8, pas: 115, fc: 105 },
    { g: 1.00, fr: 26, spo2: 90, temp: 39.3, pas: 96, fc: 124 }
  ];

  /* Ruido pseudoaleatorio: SO em pressao sistolica e frequencia cardiaca.
   *
   * Nao e economia de codigo, e a mesma decisao de projeto de `perfis.py` ("ruido mantido DENTRO
   * da mesma faixa de pontuacao"): PAS e FC tem folga larga na tabela de 7.5.1 (111-219 e 91-110),
   * entao +-4 mexe no numero sem mudar o ponto. FR, SpO2 e temperatura tem faixas estreitas (2 rpm,
   * 2 %, 1,0 C) e sao justamente os parametros que empurram o escore para a proxima banda: com
   * ruido neles, o card ficaria PISCANDO verde/amarelo nas fronteiras em vez de subir de degrau.
   * Eles evoluem pela interpolacao (mudam sozinhos ao longo da escalada), sem sorteio.
   */
  var RUIDO_MAX = { pas: 4, fc: 4 };

  // Gravidade a partir da qual o quadro ganha O2 suplementar e rebaixa o AVPU (fim da escalada).
  var G_OXIGENIO = 0.90;
  var G_AVPU_V = 0.96;

  var MONITOR_INTERVALO_PADRAO_S = 2;
  var MONITOR_INTERVALO_MIN_S = 1;
  var MONITOR_INTERVALO_MAX_S = 60;
  var MONITOR_ESCALADA_PADRAO_S = 90;

  // Semente fixa: a mesma configuracao produz a mesma evolucao, como o simulador Python (R10.6).
  // Ela e combinada com o leito_id para que dois leitos monitorados juntos nao oscilem em unissono
  // no mural -- e ainda assim cada leito repita a propria sequencia em toda apresentacao.
  var SEMENTE_MONITOR = 42;

  // leito_id -> monitor ativo. Map permite varios leitos evoluindo ao mesmo tempo no mural.
  var monitores = new Map();

  /** Gerador pseudoaleatorio proprio (mulberry32) — nunca `Math.random`, para ser reprodutivel. */
  function criarRng(semente) {
    var estado = semente >>> 0;
    return function () {
      estado = (estado + 0x6d2b79f5) >>> 0;
      var t = estado;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  /** Ruido uniforme em [-amplitude, +amplitude]. */
  function ruido(rng, amplitude) { return (rng() * 2 - 1) * amplitude; }

  /** Semente estavel derivada do leito (djb2 simplificado): "UTI-03" sempre da a mesma sequencia. */
  function sementeDoLeito(leito) {
    var h = 5381;
    for (var i = 0; i < leito.length; i++) h = ((h * 33) ^ leito.charCodeAt(i)) >>> 0;
    return (SEMENTE_MONITOR + h) >>> 0;
  }

  function grampear(valor, campo) {
    var faixa = FAIXAS_ACEITACAO[campo];
    return Math.min(faixa[1], Math.max(faixa[0], valor));
  }

  /** Valor base da chave `k` na posicao `t` entre as ancoras `a` e `b` (interpolacao linear). */
  function entre(a, b, t, k) { return a[k] + (b[k] - a[k]) * t; }

  /** Interpola `k` e devolve inteiro na faixa fisiologica — sem ruido (faixa NEWS2 estreita). */
  function inteiroLimpo(a, b, t, k, campo) {
    return grampear(Math.round(entre(a, b, t, k)), campo);
  }

  /** Interpola `k`, soma ruido de `amplitude` e devolve inteiro na faixa fisiologica. */
  function inteiroComRuido(a, b, t, k, rng, amplitude, campo) {
    return grampear(Math.round(entre(a, b, t, k) + ruido(rng, amplitude)), campo);
  }

  /**
   * Monta a leitura correspondente a uma gravidade (0..1): trajetoria interpolada + ruido.
   *
   * @param {number} gravidade Posicao na trajetoria; 1 = quadro critico (a partir dai so oscila).
   * @param {function(): number} rng Gerador pseudoaleatorio semeado do monitor.
   * @returns {{frequencia_respiratoria: number, saturacao_o2: number,
   *            oxigenio_suplementar: boolean, temperatura: number, pressao_sistolica: number,
   *            frequencia_cardiaca: number, nivel_consciencia: string}}
   *          Os sete parametros do `POST /sinais`, todos dentro das faixas fisiologicas.
   */
  function leituraContinua(gravidade, rng) {
    var g = Math.min(1, Math.max(0, gravidade));
    var i = 0;
    while (i < TRAJETORIA_CONTINUA.length - 2 && g > TRAJETORIA_CONTINUA[i + 1].g) i += 1;
    var a = TRAJETORIA_CONTINUA[i];
    var b = TRAJETORIA_CONTINUA[i + 1];
    var t = b.g === a.g ? 0 : (g - a.g) / (b.g - a.g);
    t = Math.min(1, Math.max(0, t));

    // Uma casa decimal na temperatura: e a resolucao da coluna NUMERIC(4,1) e da tabela de 7.5.1.
    var temperatura = Math.round(entre(a, b, t, "temp") * 10) / 10;
    return {
      frequencia_respiratoria: inteiroLimpo(a, b, t, "fr", "frequencia_respiratoria"),
      saturacao_o2: inteiroLimpo(a, b, t, "spo2", "saturacao_o2"),
      oxigenio_suplementar: g >= G_OXIGENIO,
      temperatura: grampear(temperatura, "temperatura"),
      pressao_sistolica: inteiroComRuido(a, b, t, "pas", rng, RUIDO_MAX.pas, "pressao_sistolica"),
      frequencia_cardiaca: inteiroComRuido(a, b, t, "fc", rng, RUIDO_MAX.fc, "frequencia_cardiaca"),
      nivel_consciencia: g >= G_AVPU_V ? "V" : "A"
    };
  }

  // -- leitura dos controles ------------------------------------------------ //

  function intervaloEscolhidoS() {
    var v = parseFloat((el["op-intervalo"] && el["op-intervalo"].value) || "");
    if (!isFinite(v)) v = MONITOR_INTERVALO_PADRAO_S;
    v = Math.min(MONITOR_INTERVALO_MAX_S, Math.max(MONITOR_INTERVALO_MIN_S, Math.round(v)));
    if (el["op-intervalo"]) el["op-intervalo"].value = String(v);
    return v;
  }

  function escaladaEscolhidaS() {
    var v = parseFloat((el["op-escalada"] && el["op-escalada"].value) || "");
    return isFinite(v) && v > 0 ? v : MONITOR_ESCALADA_PADRAO_S;
  }

  // -- estado na tela ------------------------------------------------------- //

  /** Descreve um monitor usando o escore/severidade da PROJECAO (servidor), nunca calculo local. */
  function descreverMonitor(m) {
    var card = leitos.get(m.leito);
    var escore = (card && card.score_news2 !== null && card.score_news2 !== undefined)
      ? String(card.score_news2) : "—";
    var sev = (card && card.severidade) ? rotuloSev(card.severidade) : "—";
    return m.leito + " · leitura " + m.leitura +
      " · gravidade " + Math.round(m.gravidade * 100) + "%" +
      " · NEWS2 " + escore + " (" + sev + ")";
  }

  function atualizarEstadoContinuo() {
    var alvo = el["op-continuo-estado"];
    if (!alvo) return;
    if (!monitores.size) {
      alvo.textContent = "parado";
      alvo.classList.remove("op-continuo--ativo");
      return;
    }
    var linhas = [];
    monitores.forEach(function (m) { linhas.push(descreverMonitor(m)); });
    alvo.textContent = linhas.join("\n");
    alvo.classList.add("op-continuo--ativo");
  }

  function atualizarBotaoContinuo() {
    if (!el["op-continuo"]) return;
    var leito = el["op-leito-ocupado"] ? el["op-leito-ocupado"].value : "";
    var ativo = !!leito && monitores.has(leito);
    el["op-continuo"].textContent = ativo ? "Parar " + leito : "Iniciar monitoramento";
    el["op-continuo"].setAttribute("aria-pressed", ativo ? "true" : "false");
    el["op-continuo"].classList.toggle("btn--parar", ativo);
    if (el["op-parar-todos"]) el["op-parar-todos"].disabled = monitores.size === 0;
  }

  // -- ciclo de vida do monitor -------------------------------------------- //

  /** Interrompe de verdade o monitor de um leito (timer cancelado; resposta em voo ignorada). */
  function pararMonitor(leito, mensagem) {
    var m = monitores.get(leito);
    if (!m) return;
    m.vivo = false;
    if (m.timer) window.clearTimeout(m.timer);
    monitores.delete(leito);
    atualizarBotaoContinuo();
    atualizarEstadoContinuo();
    if (mensagem) estadoSessao(mensagem, "neutro");
  }

  function pararTodosMonitores(mensagem) {
    Array.from(monitores.keys()).forEach(function (leito) { pararMonitor(leito, null); });
    if (mensagem) estadoSessao(mensagem, "neutro");
  }

  function passoMonitor(m) {
    if (!m.vivo || monitores.get(m.leito) !== m) return;
    var decorridoMs = Date.now() - m.inicio;
    m.gravidade = Math.min(1, decorridoMs / m.escaladaMs);
    m.leitura += 1;

    // Em intervalos curtos o sorteio pode repetir a leitura anterior; um unico resorteio garante
    // que a tela nunca mostre dois numeros identicos seguidos (a sensacao de monitor ligado).
    var leitura = leituraContinua(m.gravidade, m.rng);
    var chave = JSON.stringify(leitura);
    if (chave === m.ultimaChave) {
      leitura = leituraContinua(m.gravidade, m.rng);
      chave = JSON.stringify(leitura);
    }
    m.ultimaChave = chave;

    publicarSinais(m.leito, leitura).then(function (res) {
      if (!m.vivo || monitores.get(m.leito) !== m) return;   // parado durante o voo
      if (!res.ok) {
        // 409/422/503 (ou falha de rede) param o modo continuo; o corpo RFC 7807 ja foi ao log.
        pararMonitor(m.leito, null);
        estadoSessao(resumoErro(res, "POST /sinais interrompeu o monitoramento de " + m.leito),
          "erro");
        return;
      }
      atualizarEstadoContinuo();
      m.timer = window.setTimeout(function () { passoMonitor(m); }, m.intervaloMs);
    });
  }

  function iniciarMonitor(leito) {
    if (monitores.has(leito)) return;
    if (sequenciaAtiva && sequenciaAtiva.leito === leito) {
      pararSequencia("substituída pelo monitoramento contínuo");
    }
    var intervaloS = intervaloEscolhidoS();
    var escaladaS = escaladaEscolhidaS();
    var m = {
      leito: leito,
      inicio: Date.now(),
      intervaloMs: intervaloS * 1000,
      escaladaMs: escaladaS * 1000,
      leitura: 0,
      gravidade: 0,
      rng: criarRng(sementeDoLeito(leito)),
      ultimaChave: "",
      timer: null,
      vivo: true
    };
    monitores.set(leito, m);
    atualizarBotaoContinuo();
    atualizarEstadoContinuo();
    estadoSessao("Monitoramento contínuo em " + leito + ": uma leitura a cada " + intervaloS +
      " s; a gravidade sobe de baixa a crítica em " + escaladaS + " s e não para sozinha.", "ok");
    passoMonitor(m);
  }

  function alternarMonitor() {
    var leito = el["op-leito-ocupado"].value;
    if (!leito) { estadoSessao("Escolha um leito ocupado.", "erro"); return; }
    if (monitores.has(leito)) {
      pararMonitor(leito, "Monitoramento contínuo de " + leito + " parado a pedido.");
      return;
    }
    iniciarMonitor(leito);
  }

  /** Alta ou desaparecimento do leito na projecao para o monitor com aviso explicito. */
  function verificarMonitores() {
    if (!monitores || !monitores.size) return;
    Array.from(monitores.keys()).forEach(function (leito) {
      var card = leitos.get(leito);
      if (!card) {
        pararMonitor(leito, "Leito " + leito +
          " saiu da projeção — monitoramento contínuo parado.");
      } else if (card.estado === "livre") {
        pararMonitor(leito, "Alta em " + leito +
          " — monitoramento contínuo parado (leito livre não aceita sinais).");
      }
    });
    atualizarEstadoContinuo();
  }

  // ----------------------------------------------------------------- 4. alta

  function darAlta() {
    if (!exigirSessao()) return Promise.resolve();
    var leito = el["op-leito-alta"].value;
    if (!leito) { estadoSessao("Escolha um leito ocupado.", "erro"); return Promise.resolve(); }

    var card = leitos.get(leito);
    var internacaoId = card && card.internacao_id;
    if (!internacaoId) {
      estadoSessao("Leito " + leito + " sem internacao_id na projeção — recarregue os leitos.", "erro");
      return Promise.resolve();
    }

    var motivo = (el["op-motivo-alta"].value || "").trim() || "Alta médica";
    el["op-alta"].disabled = true;
    return chamar("POST", "/internacoes/" + internacaoId + "/alta", {
      corpo: { motivo: motivo, observacoes: "Alta registrada pelo Console de operação" }
    }).then(function (res) {
      el["op-alta"].disabled = false;
      if (res.ok) {
        estadoSessao("Alta registrada em " + res.dados.leito_id +
          (res.dados.leito_liberado ? " — leito liberado" : ""), "ok");
      } else if (res.status === 403) {
        estadoSessao("403 esperado: papel " + (sessao.papel || "?") +
          " não pode dar alta (R4.4). Troque para med.silva.", "erro");
      } else {
        estadoSessao(resumoErro(res, "POST /internacoes/{id}/alta"), "erro");
      }
      return res;
    });
  }

  // -------------------------------------------------------------- gaveta

  /** Abre/fecha a gaveta. `mexerFoco` fica falso no boot para nao roubar o foco da pagina. */
  function abrirConsole(abrir, mexerFoco) {
    document.body.classList.toggle("console-aberto", abrir);
    el["btn-operar"].setAttribute("aria-expanded", abrir ? "true" : "false");
    // `inert` tira a gaveta fechada da ordem de tabulacao e do leitor de tela; sem suporte, cai
    // para aria-hidden.
    if ("inert" in el["console"]) {
      el["console"].inert = !abrir;
    } else if (abrir) {
      el["console"].removeAttribute("aria-hidden");
    } else {
      el["console"].setAttribute("aria-hidden", "true");
    }
    if (!mexerFoco) return;
    if (abrir) el["op-usuario"].focus();
    else el["btn-operar"].focus();
  }

  // ---------------------------------------------------------------- ligacao

  function iniciarConsole() {
    if (!el["console"] || !el["btn-operar"]) return;   // pagina sem o console

    abrirConsole(false, false);
    novoPaciente();
    marcarPapelAtivo();
    renderLog();
    sincronizarSelects();

    el["btn-operar"].addEventListener("click", function () {
      abrirConsole(!document.body.classList.contains("console-aberto"), true);
    });
    el["op-fechar"].addEventListener("click", function () { abrirConsole(false, true); });
    document.addEventListener("keydown", function (ev) {
      if (ev.key === "Escape" && document.body.classList.contains("console-aberto")) {
        abrirConsole(false, true);
      }
    });

    el["op-entrar"].addEventListener("click", function () {
      entrar((el["op-usuario"].value || "").trim(), el["op-senha"].value);
    });
    el["op-senha"].addEventListener("keydown", function (ev) {
      if (ev.key === "Enter") el["op-entrar"].click();
    });
    el["op-sair"].addEventListener("click", sair);
    el["op-recarregar"].addEventListener("click", function () {
      if (exigirSessao()) recarregarLeitos();
    });
    document.querySelectorAll(".btn--chip[data-usuario]").forEach(function (b) {
      b.addEventListener("click", function () {
        el["op-usuario"].value = b.dataset.usuario;
        el["op-senha"].value = "demo123";
        entrar(b.dataset.usuario, "demo123");
      });
    });

    el["op-novo-nome"].addEventListener("click", novoPaciente);
    el["op-admitir"].addEventListener("click", admitir);

    document.querySelectorAll(".btn--preset[data-preset]").forEach(function (b) {
      b.addEventListener("click", function () { enviarPreset(b.dataset.preset); });
    });
    el["op-sequencia"].addEventListener("click", iniciarSequencia);
    el["op-cancelar-sequencia"].addEventListener("click", function () {
      pararSequencia("cancelada");
    });

    el["op-continuo"].addEventListener("click", alternarMonitor);
    el["op-parar-todos"].addEventListener("click", function () {
      if (!monitores.size) return;
      pararTodosMonitores("Monitoramento contínuo parado em todos os leitos.");
    });
    // Trocar o leito selecionado NAO derruba quem esta rodando (vários leitos sao permitidos):
    // apenas troca o alvo do botao, e o aviso diz quem continua vivo.
    el["op-leito-ocupado"].addEventListener("change", function () {
      atualizarBotaoContinuo();
      if (monitores.size && !monitores.has(el["op-leito-ocupado"].value)) {
        aviso("monitoramento contínuo segue ativo em " +
          Array.from(monitores.keys()).join(", "));
      }
    });
    atualizarEstadoContinuo();
    atualizarBotaoContinuo();

    el["op-alta"].addEventListener("click", darAlta);
    el["op-limpar-log"].addEventListener("click", function () {
      registros = [];
      renderLog();
      aviso("log limpo");
    });
  }

  // ------------------------------------------------------------------ boot

  marcarConexao("reconectando");
  conectar();
  iniciarConsole();
  setInterval(tickSegundo, 1000);
  tickSegundo();
})();

