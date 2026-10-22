# Persistência de telemetria — inspeção e relações

Serviço Python 3.12+ independente. Nesta etapa, **apenas inspeciona** o Redis e
verifica a conexão PostgreSQL com `SELECT 1`. `inspect --resolve` diagnostica
associações (SCRUM-450); `inspect --validate` também valida medições e timestamps
(SCRUM-448), sem escrita. Não grava leituras, cria cadastros,
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

Validação disponível na SCRUM-448; associação disponível na biblioteca da SCRUM-450; classificação: SCRUM-449; transações e
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

## Validação de medições e timestamps (SCRUM-448)

```bash
station-persist inspect --validate
python -m station_persist inspect --validate
```

`--validate` inclui resolução de relações, inclusive se combinado com `--resolve`.
Mantém XREAD desde `0-0`, snapshot `REPEATABLE READ READ ONLY` por lote e os
mesmos recursos de encerramento/backoff. Erros transitórios repetem o lote sem
avançar o cursor. Schema/configuração inválidos encerram explicitamente.
Nenhum grupo, pendência, XACK, ocorrência, leitura ou quarentena é alterado.

A biblioteca `station_persist.validation.validate_entries(entries, connection,
legacy_max_id=None)` recebe as entradas originais. `legacy_max_id` é argumento
nomeado; o chamador controla conexão/transação. Use a mesma transação para
resolver relações e consultar limites. Não há cache persistente.

`EventValidation` conserva `resolution`, `timestamp` e `fields`. O timestamp
informa segundos efetivos, received_at original em ms, origem, motivos e avisos.
Cada `MeasurementValidation` conserva a resolução original, valor decimal exato,
limites consultados, motivos e status `valid`, `out_of_range` ou `rejected`.
**Validação não é persistência nem autoriza confirmação.** Valores fora da faixa
ficam disponíveis para as SCRUM-449/451; rejeições alimentam as SCRUM-449/530.

- Aceita inteiros JSON e decimais como `Decimal`, sem conversão para float,
  arredondamento ou calibração. Booleanos, textos, null, listas/objetos e valores
  não finitos são rejeitados como medições. Tokens com expoente maior que a
  capacidade de Decimal são preservados como `UnrepresentableNumber` e rejeitados
  individualmente, sem invalidar outras medições.
- `NUMERIC` PostgreSQL sem escala fixa comporta até 131072 dígitos integrais e
  16383 fracionários. A validação calcula a representabilidade sem expandir
  expoentes nem aplicar o arredondamento do contexto Decimal. Zeros finais podem
  ser removidos do valor preparado sem perda numérica; o token original permanece
  em `resolution.source.fields['payload']`.
- Limites inclusivos: `below_min`/`above_max` indicam `out_of_range`; igualdade é
  válida e um lado NULL não restringe. Tipos indevidos geram
  `invalid_numeric_type`; não finitos, `non_finite_value`; excesso de capacidade,
  `numeric_not_representable`. Motivos de associação da SCRUM-450 são preservados.
- O schema deve conter `readings.value` e `sensors.min_value/max_value` como
  NUMERIC sem escala fixa: **aplicar a migration 016 do BANCO antes de usar o
  diagnóstico**. Não é aplicada pelo serviço. Colunas ausentes, precisão antiga,
  limites invertidos/não finitos são erros de configuração, nunca limites ausentes.
  Catálogo alterado entre consultas sem snapshot gera erro técnico recuperável.

### Timestamps e corte legado

A fonte de `unix_time` é o payload, não o campo extraído do Stream. Exige token
inteiro JSON não negativo; textos, booleanos, null, `1.0` e `1e0` não são inteiros
JSON aceitos. O intervalo UTC suportado pelo serviço é de 0 a 253402300799 segundos
(1970 até o fim de 9999), dentro de BIGINT. Não existe janela de atraso/futuro.
`received_at` é obrigatório, inteiro textual não negativo em ms, até
253402300799999. Ausência/invalidez gera `invalid_received_at` ou
`received_at_not_representable`; timestamp informado incorretamente gera
`invalid_timestamp_type` ou `timestamp_not_representable`.

Timestamp ausente usa `received_at // 1000`, com origem `received_at`. Timestamp
válido usa origem `payload`. Nunca se consulta o relógio do processamento.

`PERSIST_LEGACY_MAX_ID` é opcional e vazio por padrão. É um ID Redis completo,
com dois inteiros uint64, comparados numericamente e com limite inclusivo. Vale
somente para o `PERSIST_STREAM` configurado. O operador deve registrar o último ID
gerado pelo receptor antigo durante uma transição com ele parado, antes de ativar
o receptor novo. Não deduzir esse corte por payload nem copiar entre Streams.

Dentro do corte, payload `unix_time: null` junto do campo extraído `unix_time`
igual ao texto `null` permite usar received_at com origem `legacy_received_at` e
aviso `legacy_timestamp_original_lost`. O valor original não pode ser reconstruído.
Fora do corte, ou sem configuração, null explícito é rejeitado. Timestamp inválido
compartilhado rejeita as medições do evento; valor inválido em um sensor não afeta
os demais. O resultado preserva todas as entradas para seu destino futuro.

Logs de validação mostram IDs, status, timestamp efetivo, origem e motivos/avisos;
não incluem valores das medições, limites ou payload completo. A inspeção padrão
e `--resolve` continuam disponíveis.

### Limite da garantia de precisão

A precisão é preservada no tratamento e no armazenamento NUMERIC. A migration
não recupera casas já arredondadas no histórico. As APIs existentes continuam
usando números JavaScript e conversões float8, incluindo estatísticas arredondadas:
não oferecem precisão decimal exata nem suporte equivalente a valores extremos.
Este PR não muda seu contrato HTTP. Validar essa compatibilidade antes de habilitar
a persistência definitiva em produção (SCRUM-451/531).

## Destinos das medições — SCRUM-449

`build_processing_plan(EventValidation)` é uma função sem escrita. Conserva a
validação/origem e classifica cada campo como `reading`, `inconsistent` ou
`quarantine`. A posição do campo identifica o resultado; a posição zero representa
uma rejeição do evento inteiro, inclusive payload sem campos associáveis.
Uma rejeição individual não impede o destino dos outros campos. O plano não é
`ProcessingResult.COMPLETED` e nunca autoriza ACK.

A SCRUM-451 integra esses destinos em uma transação PostgreSQL: leituras fora dos
limites mantêm o valor exato e `data_consistent=false`, com ocorrência
`OUT_OF_RANGE` (no máximo uma aberta por sensor). Não depender do monitor do
BACKEND: ele ignora leituras já inconsistentes. Rejeições preservam a origem em
quarentena, e valores conflitantes para o mesmo sensor/instante não sobrescrevem
a primeira leitura. Recuperação e replay operacional são descritos abaixo, na SCRUM-530.

## Consumo transacional optativo — SCRUM-451

```bash
# Apply BANCO schema/migrations explicitly first, only in the intended database.
# Both historical 016 migrations, 017 and 018 are required.
export PERSIST_SOURCE_ID=vulture-local-telemetry
station-persist consume
# equivalent:
python -m station_persist consume
```

`consume` writes; the default remains `inspect` (read-only). Do not run the new
consumer beside `recepcao-persist` against the same database. Deployment and the
operational replacement belong to SCRUM-531. No migration is run by this package.
Startup verifies storage columns, exact NUMERIC types and required unique indexes
before creating a consumer group. `--resolve`/`--validate` remain inspection flags.

`process_entry(entry, pool, *, source_id, stream, legacy_max_id=None)` owns one
`REPEATABLE READ` transaction. It registers the source, calls the SCRUM-448
validator, locks associated station/sensor rows, writes all destinations, updates
communication and marks completion. `ProcessingResult.COMPLETED` is returned only
after the transaction context has committed; only then may GroupConsumer XACK.
Technical failures roll back all destinations; ambiguous commit and cancellation
leave the entry unacknowledged. Transient errors retry the entire transaction.

The BANCO migration adds `telemetry_events`, `telemetry_results` and
`telemetry_quarantine`. Events store **all original Redis string fields** as
ASCII-escaped JSON bytes in BYTEA. This is an envelope representation, not a
parse/rewrite of the payload: `json.loads(original_fields)` reconstructs the
original field strings, including arbitrary numeric tokens, malformed payloads,
NUL and lone surrogates. Quarantine joins the event for its topic, received_at,
payload and identity; it stores ordinal (zero = event rejection), reasons, OPEN
state and registration time. Individual field ordinals follow payload order.
No automatic cleanup or deletion of these audit records is implemented.

Idempotency has two independent levels:

- `(PERSIST_SOURCE_ID, PERSIST_STREAM, Redis entry ID)` is unique. Keep SOURCE_ID
  stable through restarts; consumer names are not part of this identity. A
  completed entry with identical original fields is a no-op. Reused identity
  with changed content raises SourceIdentityError and receives no ACK. Changing
  SOURCE_ID is an explicit operator decision when replacing/restoring a source
  history that can reuse Redis IDs; it is not a workaround for normal replay.
- `(sensor_id, unix_time)` is unique in readings, including partitions. The same
  exact Decimal value reuses the reading; a different value creates quarantine
  reason `conflicting_measurement`. The first committed reading retains value
  and consistency; later limit changes do not silently reclassify duplicates.

MQTT redelivery with different Redis IDs is deduplicated when the effective sensor
and measurement second are the same. Without a publisher timestamp, a new receipt
in another second can produce another measurement time: the current protocol has
no durable publisher event ID to identify those messages as the same measurement.

New out-of-range readings preserve exact value and `data_consistent=false` and
insert OUT_OF_RANGE directly, respecting the existing partial unique index for
one OPEN occurrence per sensor. Repeating a reading never reopens a resolved
occurrence. Occurrence values use unconstrained NUMERIC; descriptions avoid
expanding huge numbers. API float8/JavaScript limitations remain unchanged.

Communication uses original received_at milliseconds in UTC and GREATEST, so
late deliveries cannot regress stations.last_communication_at. A safely resolved
station with valid receipt can update communication even if its measurements
are rejected. Measurement time, missing timestamp fallback and the configured
inclusive legacy cutoff retain SCRUM-448 rules. Empty Redis entries (e.g. trimmed
pending entries) geram incidente durável e permanecem pendentes sem prova de conclusão, conforme a SCRUM-530 abaixo.

The consumer starts its group at 0-0. SCRUM-530 interleaves bounded batches of
own pending entries, recovery from abandoned identities and new entries. Keep
PERSIST_CONSUMER unique among live instances; leases prevent duplicate identities.
On SIGINT/SIGTERM it stops admitting work and drains the active transaction up to
PERSIST_SHUTDOWN_TIMEOUT_SECONDS, then cancels and closes resources. Logs report
IDs, counts, duplicate/conflict quarantine totals and commit latency, without
measurement values or complete payloads.

### Integration tests with the real BANCO contract

The persistence tests read the versioned schema and migrations from the sibling
BANCO checkout (override with BANCO_SCHEMA_DIR). This is a **test-only dependency**;
the installed package/image never imports or reads another repository at runtime.
CI checks out a pinned BANCO revision with migration 018 and fails if any required
integration is skipped. Use RUN_INTEGRATION=1, TEST_DATABASE_URL and TEST_REDIS_URL
against disposable services only.


## Recuperação e operação — SCRUM-530

A migration **018** deve ser aplicada explicitamente antes de iniciar o consumidor.
O runtime valida colunas e índices; não executa migrations. Não ativar o novo
consumidor junto ao `recepcao-persist`, que não participa do protocolo de leases.
Integração operacional e retirada do legado continuam nas SCRUM-531/532.

### Lease, retomada e falhas técnicas

Cada identidade `(Stream, grupo, consumidor)` recebe uma chave Redis com token
aleatório (`SET NX PX`). Renovação e liberação usam Lua e conferem o token. Outra
instância com a mesma identidade é recusada; perda de lease ou falha de renovação
cancela a transação ativa e encerra o processo. O tempo máximo do processamento é
30 s; TTL 60 s, renovação 10 s. A configuração exige TTL >= timeout + 2 renovações,
e o idle mínimo para recuperação >= TTL. Durante SIGTERM/SIGINT a lease continua
sendo renovada enquanto a transação é drenada dentro do prazo de shutdown.

A recuperação usa `XPENDING` paginado (100 por lote), varre a cada 10 s quando
chega ao fim e considera idle >= 120000 ms por padrão. Lua revalida owner/idle,
protege qualquer owner com lease ativa, confere o conteúdo e só então executa
`XCLAIM`. Identificadores são preservados; o destino transacional da SCRUM-451
mantém ACK após commit e idempotência. Cursores avançam por item tentado, incluindo
falhas, para permitir progresso das próximas entradas. Retentativas transitórias
repetem a transação completa com backoff exponencial e jitter; schema e identidade
incompatíveis continuam erros explícitos, sem serem convertidos em rejeição.
Esta implementação usa Redis 7 standalone, sem presumir scripts multikey em Cluster.

Não usamos `XAUTOCLAIM`: no Redis 7 ele pode limpar da PEL referências a entradas
já removidas. Conteúdo ausente gera `telemetry_recovery_incidents` por origem,
grupo e ID, com primeiro/último registro e contagem. Se o ledger **da mesma origem**
provar evento concluído, o incidente é commitado e depois pode ocorrer XACK. Sem
essa prova, mantém-se a PEL e emite-se erro; não se inventam medições. As demais
entradas continuam progredindo. Isso detecta perda anterior, sem reconstruí-la.
Um erro no registro do incidente impede ACK. Nunca remover PEL manualmente para
ocultar esse caso; investigar backups/origem e registrar decisão operacional.

### Quarentena e replay original

Os comandos abaixo usam somente PostgreSQL, com escopo por `PERSIST_SOURCE_ID` e
`PERSIST_STREAM`. `status` também lê Redis. Os modos `inspect`, `--resolve` e
`--validate` continuam sem escrita, grupos ou ACKs.

```bash
station-persist status
station-persist quarantine list --limit 100 --after-id 0
station-persist quarantine show --id 42
station-persist quarantine replay --id 42 \
  --request-id 870203e8-52f6-4273-8409-0d71e513a08a \
  --operator operador-identificavel --reason 'Cadastro corrigido' --dry-run
# Retire --dry-run para executar; pode repetir o MESMO UUID/operador/motivo
# após resposta perdida. Uma nova decisão/tentativa após correção usa novo UUID.
python -m station_persist quarantine replay --id 42 \
  --request-id 870203e8-52f6-4273-8409-0d71e513a08a \
  --operator operador-identificavel --reason 'Cadastro corrigido'
```

O operador é uma identificação obrigatória informada pela CLI, não autenticação.
Controlar acesso por credenciais PostgreSQL e ambiente autorizado. Listagem paginada
e detalhes exibem metadados/motivos/histórico, sem despejar medições ou payloads.
Em show, --limit também limita os históricos de tentativas e resultados, dos mais
recentes para os antigos.
UUID válido, operador e justificativa são obrigatórios, inclusive para dry-run.
Dry-run usa snapshot somente de leitura, sem registrar solicitação ou tentativa;
avalia destinos sem reservas e pode mudar se o catálogo for alterado antes da execução.

Replay decodifica **os campos string BYTEA originais**, revalida cadastro/limites
atuais e preserva medição/recepção originais, inclusive origem do timestamp,
avisos e corte legado usado na primeira execução. A 018 armazena `legacy_max_id`
nos novos eventos. Para anteriores à 018, timestamp já efetivado no ledger é
reutilizado; se rejeitados antes disso e sem corte registrado, aplica-se a regra
estrita (não inferir corte nem reconstruir timestamp perdido).

Solicitação e tentativa RUNNING são duráveis antes da transação de efeitos. Ela
bloqueia evento/quarentena e cadastros, registra resultados e fecha a quarentena
somente se não restarem rejeições. Não altera conteúdo, motivos, limites ou
resultados originais: novos resultados guardam seus próprios limites e origem. Se a estação passar a ser
associável, sua comunicação usa somente received_at original, com GREATEST; o
relógio do replay não simula uma nova comunicação.
Uma chave de evento/campo impede repetir sucesso em replays parciais. Campos já
aceitos não são reprocessados, mesmo após mudança/remoção do sensor. Valor
conflitante continua rejeitado, sem substituir leitura ou permitir que exclusão
da leitura original contorne o conflito. Não há editor de payload nem HTTP novo.

A mesma request-id com os mesmos parâmetros retorna o resultado terminal já
commitado. Parâmetros divergentes causam erro; requisição simultânea com o mesmo
UUID retorna BUSY. Novos UUIDs para o mesmo evento são serializados. Estados:
RUNNING, SUCCEEDED, PARTIAL, REJECTED, TECHNICAL_FAILURE e INTERRUPTED. Falhas técnicas
fazem rollback de todos os destinos, mas mantêm tentativa auditada. Até 3 tentativas
transitórias usam backoff/jitter; depois corrigir a falha e repetir o mesmo comando.
Uma tentativa RUNNING abandonada vira INTERRUPTED na retomada. Se o banco ficar
inacessível durante a auditoria, RUNNING permanece rastreável até essa retomada.
Resposta de commit perdida não sobrescreve resultado terminal nem repete efeitos.
BUSY/PARTIAL/REJECTED são resultados explícitos no JSON; consultar o campo status.

### Métricas, alertas e capacidade

A cada `PERSIST_METRICS_INTERVAL_SECONDS` (30 s), `persist_status` publica JSON com
lag Redis (null se desconhecido), PEL, idade do menor ID pendente (idade desde o ID
Redis, não tempo exato de idle), quarentenas abertas/idade, incidentes sem cópia
durável, tentativas por estado e contagens de destinos commitados. `station-persist
status` consulta o mesmo snapshot somente de leitura; falhas retornam `degraded`,
valores desconhecidos null e exit code 1, sem mascarar indisponibilidade como zero.

`event_committed` registra latência, novas leituras, inconsistentes, quarentena e
duplicatas. `quarantine_replay` separa campos aceitos/rejeitados; aceitos podem ser
duplicatas e **não** são contador de novas leituras. Nos totais duráveis,
`reading`/`inconsistent` representam inserções e `duplicate` reaproveitamento.
`consumption_retry`, `replay_retry`, `pending_recovered`, `missing_stream_content`,
`consumption_failed` e `lease_release_failed` identificam recuperação e falhas.
Logs omitem valores/payloads; uma tentativa não conta como nova leitura.
Não há servidor Prometheus nesta etapa; o ambiente deve coletar os logs.

Procedimento de operação:

1. Consultar `status`. Alertar sobre aumento sustentado de lag/PEL/idade, falhas
   técnicas, perda de lease, incidentes e quarentena crescente. Limiares e capacidade
   são decisões do ambiente, não metas presumidas nesta entrega.
2. Para consumidor encerrado, iniciar instância com identidade única e mesma
   SOURCE_ID/Stream/grupo, aguardar idle/lease; conferir redução da PEL e commits.
   Não usar `DELCONSUMER`, XACK manual ou trocar SOURCE_ID como retentativa.
3. Para rejeição, consultar ID/motivo, corrigir cadastro/regra pelo fluxo existente,
   executar dry-run e replay com UUID/operador/justificativa; guardar resultado e
   verificar histórico. Dado imutável inválido pode continuar rejeitado.
4. Em indisponibilidade longa, preservar Redis (persistência e noeviction), alertar
   memória/disco e aumentar capacidade ou pausar origem conforme runbook. A recepção
   controla retenção atômica por idade **e progresso de todos os grupos**; eventos
   não processados bloqueiam o corte. O tratamento nunca executa XTRIM.

Nenhuma recuperação garante dados já perdidos por XDEL, evicção ou perda de disco.
A recuperação e replay não mudam o limite MQTT QoS 1/at-least-once nem a limitação
de precisão float8/JavaScript das APIs atuais. Não há merge ou ativação automática.
