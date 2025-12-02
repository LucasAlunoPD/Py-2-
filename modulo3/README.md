# Projeto: SQLAlchemy + SQLite com salaries.csv

Este projeto implementa todos os passos pedidos:

1. **Conhecendo os dados**
   - Carrega o `salaries.csv` com `pandas`.
   - Mostra `head()`, `info()` e `describe()`.

2. **Modelando os dados (ORM com SQLAlchemy)**
   - Cria um modelo `SalaryRecord` com:
     - `id` (PK, autoincremento).
     - Colunas de texto, inteiros, floats e datas conforme descrição.
     - Campos `SEX`, `DESIGNATION` e `UNIT` como Enums dinâmicos, gerados a partir dos valores únicos do CSV.

3. **Conexão**
   - Usa `create_engine` com SQLite: `sqlite:///salarios.db`.

4. **Criando as tabelas**
   - Usa `Base.metadata.create_all(engine)` para criar as tabelas no banco `salarios.db`.

5. **Populando**
   - Usa `pandas.DataFrame.to_sql` com `if_exists='append'` e `index=False` para inserir os dados na tabela já criada.

6. **Consultas SQL vs ORM**
   - Agrupa os dados por `DESIGNATION` e calcula:
     - `MIN(SALARY/12)`, `MAX(SALARY/12)` e `AVG(SALARY/12)`.
   - Faz a consulta de 3 formas:
     1. SQL puro via `engine.connect()` e `conn.execute(text(...))`.
     2. SQL via `pandas.read_sql_query(...)`.
     3. ORM com `select()` + `Session(engine)`.

## Como usar

1. Crie e ative um ambiente virtual (opcional, mas recomendado).
2. Instale as dependências:

   ```bash
   pip install pandas sqlalchemy
   ```

3. Baixe o arquivo `salaries.csv` do GitHub e coloque na mesma pasta do `main.py`:

   - Link: https://github.com/camilalaranjeira/python-intermediario/blob/main/salaries.csv  
     (no GitHub, clique em **Raw** e depois **Salvar como**).

4. Execute o script:

   ```bash
   python main.py
   ```

5. Ao final, você terá:
   - O arquivo de banco `salarios.db` na pasta.
   - As saídas das consultas impressas no terminal.
