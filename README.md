# TP1 - Information Retrieval - Web Crawler

Este projeto implementa um crawler web para a disciplina de Information Retrieval (UFMG), com:

- fronteira de URLs baseada em seeds;
- paralelismo com múltiplas threads;
- respeito a `robots.txt` e atraso mínimo por host;
- armazenamento em arquivos WARC compactados (`.warc.gz`);
- retomada automática após interrupção/crash.

## Requisitos

- Python 3.14 (conforme especificação do trabalho)
- Ambiente virtual com dependências do `requirements.txt`

## Executando o crawler

Comando principal:

```bash
python crawler.py -s <ARQUIVO_SEEDS> -n <LIMITE> [-d]
```

Exemplo:

```bash
python crawler.py -s seeds/all-seeds.txt -n 100000
```

Parâmetros:

- `-s`: caminho para arquivo de seeds (uma URL por linha)
- `-n`: número alvo de páginas únicas
- `-d`: modo debug (imprime JSON por página crawleada)

## Progresso no terminal

Durante o crawl, o programa exibe:

- progresso por arquivo WARC em passos de 10% (10, 20, ..., 100);
- mensagem de retomada quando continua de execução anterior;
- mensagem de finalização quando a frontier esgota.

## Interromper e retomar

- Para parar: `Ctrl + C`
- Para retomar: execute o mesmo comando novamente

O crawler lê os WARC já existentes na pasta `corpus/` e continua de onde parou.

## Saídas geradas

- Corpus em `corpus/` (`warc-00001.warc.gz`, `warc-00002.warc.gz`, ...)
- Cada arquivo WARC armazena até 1000 páginas

## Utilitários

### Estatísticas rápidas de corpus

```bash
python corpus_stats.py corpus --json
```

### Contagem de documentos por arquivo WARC

```bash
python warc_docs_per_file.py corpus
```

### Média e máximo de tokens por página

```bash
python token_stats.py corpus
```

## Observações importantes

- Para uma execução final limpa, use uma pasta `corpus/` sem resíduos de testes anteriores.
- Se houver WARC parcial/corrompido por interrupções, os scripts tentam ignorar trechos inválidos quando possível.
- Garanta execução final em Python 3.14 antes da entrega.
