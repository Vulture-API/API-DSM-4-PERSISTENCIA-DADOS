# Persistência de telemetria — base (SCRUM-529)

Serviço Python 3.12+ independente. Nesta etapa, **apenas inspeciona** o Redis e
verifica a conexão PostgreSQL com `SELECT 1`. Não grava leituras, cria cadastros,
confirma mensagens ou altera grupos de consumidores. Não está implantado no INFRA.

[Contrato oficial no Confluence](https://vultureapi.atlassian.net/wiki/spaces/V/pages/46333954).

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[test,lint]'
cp .env.example .env
station-persist inspect
# equivalente: python -m station_persist
```

Configuração em `.env.example`, prefixo `PERSIST_`. DATABASE_URL é obrigatória.
As URLs são segredos e não aparecem nos logs. A inspeção faz PING/SELECT 1 no
início e XREAD de `0-0` em diante, com cursor apenas em memória. Reiniciar relê
o histórico. São registrados ID e quantidade de campos, sem payloads completos.
SIGINT/SIGTERM fecham leitura, pool e Redis. Falha de inicialização encerra o
processo com erro; queda de Redis durante inspeção aplica backoff.

```bash
docker build -t station-persist:local .
docker run --rm --env-file .env station-persist:local
pytest --cov --cov-report=term-missing
ruff check .
ruff format --check .
```

## Consumidor por grupo (biblioteca, sem comando de produção)

`GroupConsumer` exige um processador assíncrono injetado. `ensure_group` cria o
grupo em `0-0`; `process_once` lê entradas novas ou pendências do consumidor
configurado. Cada entrada conserva o ID e todos os campos originais, sem
normalizar timestamp, identificar estação ou interpretar sensores.

Somente `ProcessingResult.COMPLETED` permite XACK. O processador futuro só pode
retornar esse resultado depois de commit e destino seguro de todas as medições.
`INCOMPLETE`, exceções e cancelamento deixam a entrada pendente. Não há handler
padrão, escrita PostgreSQL, recuperação de outros consumidores ou descarte de
entradas expurgadas. A inspeção não usa esse componente e não modifica a PEL.

Validação/associação: SCRUM-448/450; classificação: SCRUM-449; transações e
idempotência: SCRUM-451; recuperação/quarentena/replay: SCRUM-530; substituição
operacional: SCRUM-531/532. O processo legado não deve ser removido nesta etapa.

## Integração descartável

```bash
RUN_INTEGRATION=1 \
TEST_REDIS_URL=redis://localhost:6379/15 \
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/test \
pytest --cov --cov-report=term-missing
```

Use Redis e PostgreSQL descartáveis. Os testes usam
Stream/grupos aleatórios e os removem ao final. O CI roda integrações obrigatórias,
ruff, cobertura mínima de 80%, wheel e build Docker, sem deploy ou publicação.
