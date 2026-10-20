# Persistência de telemetria — inspeção e relações

Serviço Python 3.12+ independente. Nesta etapa, **apenas inspeciona** o Redis e
verifica a conexão PostgreSQL com `SELECT 1`; `inspect --resolve` também consulta
o catálogo para diagnosticar associações (SCRUM-450). Não grava leituras, cria cadastros,
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

Validação: SCRUM-448; associação disponível na biblioteca da SCRUM-450; classificação: SCRUM-449; transações e
idempotência: SCRUM-451; recuperação/quarentena/replay: SCRUM-530; substituição
operacional: SCRUM-531/532. O processo legado não deve ser removido nesta etapa.

## Resolução de relações (SCRUM-450)

```bash
station-persist inspect --resolve
# equivalente:
python -m station_persist inspect --resolve
```

A opção mantém XREAD desde `0-0` e resolve cada lote em transação PostgreSQL
`REPEATABLE READ READ ONLY`. Não cria grupos, altera pendências, executa XACK,
insere leituras ou grava quarentena. Logs incluem IDs, `local_identifier`,
associações e códigos de rejeição, sem valores das medições ou payload. Sem a
opção, `inspect` continua registrando apenas metadados do Stream.

Falhas transitórias de conexão/consulta aplicam o backoff configurado e repetem
o lote sem avançar o cursor; resultados desse lote podem reaparecer nos logs.
Erros de schema, autenticação ou configuração encerram com erro explícito.
O snapshot vale por lote, e mudanças do catálogo são vistas nos lotes seguintes.
Não há cache persistente de cadastros.

A biblioteca `station_persist.relationships.resolve_entries(entries, connection)`
recebe uma sequência de `StreamEntry` e uma conexão asyncpg. **O chamador controla
a conexão e a transação**, inclusive na futura persistência da SCRUM-451:

```python
from station_persist.relationships import resolve_entries

async with pool.acquire() as connection:
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        results = await resolve_entries(entries, connection)
for event in results:
    # event.source conserva o ID e todos os campos originais do Stream.
    # field.associated indica apenas associação, nunca validação/persistência.
    for field in event.fields:
        if field.associated:
            pass  # Validação de negócio será implementada na SCRUM-448.
```

`EventResolution` traz `source`, `station_id`, `property_id`, `fields` e `reasons`.
Cada `FieldResolution` preserva `local_identifier` e `value`, com os quatro IDs
(`station_id`, `property_id`, `sensor_id`, `sensor_type_id`) e motivos próprios.
Números JSON fracionários e inteiros com milhares de dígitos usam `Decimal`,
sem arredondamento para float. Strings,
booleanos, null, listas, objetos e valores fora dos limites permanecem intactos.
O JSON original continua disponível em `source.fields['payload']`.

Regras de associação:

- `estacao_id` do Stream é a fonte de identidade; o tópico não é usado para inferir estação.
- Uma sequência ASCII de dígitos é ID legado (0 a 2147483647), com zeros iniciais aceitos. Não há tentativa posterior como MAC, inclusive para 12 dígitos.
- MAC aceita caixa mista, dois-pontos, hífens consistentes ou nenhum separador. Espaços externos são removidos para comparação, sem alterar a origem.
- `estacao_id` presente no payload deve ser string e representar a mesma identidade normalizada. ID e MAC não são intercambiados silenciosamente, mesmo que cadastrem a mesma estação.
- Cada chave do payload, exceto `estacao_id` e `unix_time`, é candidata a `local_identifier`, incluindo campos adicionais. Sem trimming ou filtros por tipo de valor.
- A consulta SQL parametrizada compara `lower(local_identifier)` dentro da estação, respeitando a collation PostgreSQL. Identificadores iguais em estações distintas não se confundem.
- Mais de um cadastro compatível, ou duas chaves do evento associadas ao mesmo sensor, rejeitam as referências afetadas. Demais campos continuam disponíveis.
- A propriedade vem da estação; `sensors.station_id` é conferido. Nenhum cadastro é criado ou alterado.

| Código estável | Significado |
| --- | --- |
| `invalid_payload` | JSON ausente, inválido, não objeto ou com chaves repetidas. |
| `invalid_identification` | Identificação ausente ou fora do formato aceito, no Stream ou payload. |
| `identification_out_of_range` | ID legado não cabe em integer PostgreSQL. |
| `identification_mismatch` | Identidades normalizadas do Stream e payload divergem. |
| `invalid_sensor_identifier` | Chave contém NUL ou surrogate Unicode e não pode ser comparada em PostgreSQL; valor e origem continuam preservados. |
| `unknown_station` / `unknown_sensor` | Referência inexistente no escopo consultado. Um identificador que existe só em outra estação continua desconhecido nesta estação. |
| `ambiguous_station` / `ambiguous_sensor` | Mais de um cadastro corresponde à mesma referência normalizada. |
| `duplicate_sensor` | Duas chaves de um evento apontam ao mesmo sensor; ambas são rejeitadas. |
| `incompatible_reference` | Propriedade/tipo ausente ou vínculo de sensor incompatível; não se adota outra estação. |

Motivos no evento impedem a associação de seus campos; motivos num campo não
invalidam os demais. Exceções PostgreSQL propagam como erros técnicos, sem serem
convertidas em ausência de cadastro. O resultado não é `ProcessingResult`, não
pode liberar XACK e não é instalado como processador de produção. As rejeições
estruturadas serão consumidas pela quarentena das SCRUM-449/530; nenhum destino
persistente de rejeições é implementado aqui. Timestamps, limites, valores
numéricos e calibração continuam fora desta etapa.

## Integração descartável

```bash
RUN_INTEGRATION=1 \
TEST_REDIS_URL=redis://localhost:6379/15 \
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/test \
pytest --cov --cov-report=term-missing
```

Use Redis e PostgreSQL descartáveis. Os testes usam
Streams/grupos e schemas de catálogo aleatórios e os removem ao final.
As fixtures reproduzem o subconjunto de colunas e vínculos do BANCO usado pela
resolução, sem migrations ou alterações no ambiente principal. O CI roda integrações obrigatórias,
ruff, cobertura mínima de 80%, wheel e build Docker, sem deploy ou publicação.
