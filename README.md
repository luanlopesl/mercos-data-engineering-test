# Sobre

Esse projeto foi desenvolvido de acordo com o teste técnico fornecido pela empresa Mercos. O objetivo principal é processar os arquivos CSV fornecidos e gerar os resultados solicitados.

# Estrutura do Projeto

```
. (mercos-app)
├── data/                   <-- Arquivos fonte vão na pasta data/
│   ├── authors.csv
│   ├── categories.csv
│   ├── dataset.csv
│   └── format.csv
├── src/
│   ├── data_processing.py  # Funções relacionadas a transformações e limpeza dos dados
│   ├── data_results.py     # Funções relacionadas aos resultados das perguntas propostas
│   ├── main.py             # Script principal (executar esse!)
│   ├── schema.py           # Arquivo auxiliar com a definição do schema da tabela
│   └── gcp.py              # Operações relacionadas ao GCP
└── requirements.txt
```

# Como executar

1. (Opcional) Criar um ambiente virtual para isolar as bibliotecas

```
$ python -m venv .venv
$ source .venv/bin/activate
```

2. Instalar as bibliotecas necessárias

```
$ pip install -r requirements.txt
```

3. Executar o arquivo principal `main.py`

```
$ python main.py
```

Os resultados devem aparecer no prompt :)