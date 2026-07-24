# Slides da apresentação

Deck de 17 slides para os **15 minutos** da apresentação (entregável da seção 9 do enunciado).

## Como abrir

```bash
open slides/index.html          # macOS
xdg-open slides/index.html      # Linux
```

Não precisa de servidor, build, internet ou dependência — é um único arquivo HTML, no mesmo
princípio do painel (R11.6).

## Controles

| Tecla | Ação |
|---|---|
| `→` `espaço` `PageDown` | próximo slide |
| `←` `PageUp` | slide anterior |
| `Home` / `End` | primeiro / último |
| **`P`** | mostra/esconde as **notas do apresentador** |
| `Ctrl+P` / `Cmd+P` | imprime em PDF (um slide por página) |

Clique na metade direita da tela avança; na esquerda, volta. O endereço guarda o slide atual
(`#7`), então dá para abrir direto num ponto durante o ensaio.

## Antes de apresentar

1. **Preencher os nomes do G3** — dois slides trazem o marcador âmbar `PREENCHER` (capa e
   encerramento). São os únicos lugares.
2. Ensaiar com `P` ligado: cada slide tem uma nota com o **tempo alvo** e o que não pode ser
   esquecido ali.
3. Subir a stack **antes** da apresentação, nunca na frente da banca:
   ```bash
   docker compose down -v && docker compose up -d --wait
   ```
4. Abrir as abas: `/painel`, `/docs` e `localhost:15672`, com a fonte já ampliada.

## Estrutura e tempo

| # | Slide | Bloco | Alvo |
|---|---|---|---|
| 1 | Capa | — | 10 s |
| 2 | O problema é de acoplamento | Contexto | 40 s |
| 3 | Três desacoplamentos | Teoria | 40 s |
| 4 | *Exactly-once* não existe | Teoria | 40 s |
| 5 | Cliente → Middleware → Servidor → Banco | Arquitetura | 60 s |
| 6 | Topologia AMQP | Arquitetura | 60 s |
| 7 | **O middleware é nosso** | Middleware | 60 s |
| 8 | O Envelope | Middleware | 60 s |
| 9 | Pipeline do Consumer | Middleware | 60 s |
| 10 | RPC sobre fila | Middleware | 60 s |
| 11 | **Demonstração** | Demo | 5 min |
| 12 | O que a demo prova | Demo | rede de segurança |
| 13 | Mapeamento AWS | AWS | 40 s |
| 14 | Diferenças semânticas do SQS | AWS | 20 s |
| 15 | Qualidade e limitações | Fechamento | 30 s |
| 16 | Perguntas esperadas | Q&A | cola |
| 17 | Encerramento | — | — |

O **slide 7** é o que sustenta os 30% de "implementação do middleware": ele responde à pergunta
"vocês só usaram o RabbitMQ?". Não passe rápido por ele.

O **slide 12** é rede de segurança: se a demonstração falhar ao vivo, ele conta o que ela mostraria,
requisito por requisito.

O **slide 16** não deve ser apresentado em sequência — pule para ele se a banca perguntar.

## Roteiro da demonstração (slide 11)

Detalhado em `docs/arquitetura.md` e no `README.md` (§6.A). Ordem sugerida:

1. `/docs` aberto → `401` sem token em `application/problem+json`
2. Trocar para `aud.paula` → tentar admitir → **403** com o corpo RFC 7807 na tela
3. Admitir 3 pacientes → **Modo enfermaria** → falar sobre arquitetura enquanto o mural ganha vida
4. Um card fica vermelho sozinho; o alerta entra no topo
5. Preset **Fora de faixa** → DLQ na 1ª tentativa, sem retentativa
6. Leito sabotado → `1s · 2s · 4s` → DLQ na 4ª
7. Clicar num `correlation_id` do log → colar `./scripts/trace.sh` no terminal
