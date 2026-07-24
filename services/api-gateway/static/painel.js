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

  function conectar() {
    try { if (fonte) fonte.close(); } catch (e) { /* ignora */ }
    fonte = new EventSource(URL_STREAM, { withCredentials: true });

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
    marcarConexao("conectado");
    elAviso.hidden = snap.hidratada !== false;
    if (snap.hidratada === false) {
      elAviso.textContent = "projeção não reconciliada no boot — aguardando eventos do Broker";
    }
  }

  function aplicarLeito(card) {
    if (!card || !card.leito_id) return;
    leitos.set(card.leito_id, card);
    upsertCard(card);
    atualizarContadores();
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

  // ------------------------------------------------------------------ boot

  marcarConexao("reconectando");
  conectar();
  setInterval(tickSegundo, 1000);
  tickSegundo();
})();
