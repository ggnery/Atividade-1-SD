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
    el.className = "card " + classeSev(card.severidade) + (card.estado === "livre" ? " card--livre" : "");
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
    var alta = card.severidade === "alta";
    var badge = '<span class="badge">' + (alta ? "⚠ " : "") + rotuloSev(card.severidade) + "</span>";

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
   "op-cancelar-sequencia", "op-progresso", "op-leito-alta", "op-motivo-alta", "op-alta",
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

