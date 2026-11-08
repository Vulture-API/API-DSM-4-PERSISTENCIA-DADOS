## Work Item

<!-- Ex: SCRUM-530 -->

Closes SCRUM-

## O que foi feito

-

## Como testar

1.

## Testes

<!-- Card de teste vinculado (ex.: SCRUM-588) ou "testes neste PR" quando a regra for pequena -->

Card de teste: SCRUM-

- [ ] Regra nova ou alterada tem teste
- [ ] Commit da feature separado do commit de teste
- [ ] Card de teste com o mesmo responsável da feature (ou testes neste PR)
- [ ] Regra de negócio em função pura, fora da camada de Redis/PostgreSQL
- [ ] Teste unitário não acessa Redis, PostgreSQL nem rede
- [ ] Nome do teste descreve o comportamento esperado
- [ ] Casos de quina cobertos (vazio, limite, inválido, ausente)
- [ ] Testes não dependem de ordem, da hora atual nem de dados persistidos
- [ ] Teste de integração marcado com `@pytest.mark.integration`
- [ ] Nenhum warning novo na saída dos testes
- [ ] Cobertura não caiu

## Checklist (DoD)

- [ ] A branch contém a chave `SCRUM-XX`
- [ ] Commits com a chave do Jira na mensagem
- [ ] Pipeline de CI verde (lint, testes unitários, integração, cobertura ≥ 80% e build)
- [ ] Documentação atualizada, se aplicável
- [ ] Code Review solicitado a outro Developer
