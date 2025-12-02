"""
Projeto: Análise e persistência de salários com SQLAlchemy + SQLite

Pré‑requisitos:
    pip install pandas sqlalchemy

Passos para executar:
1) Baixe o arquivo salaries.csv do link:
   https://github.com/camilalaranjeira/python-intermediario/blob/main/salaries.csv
2) Salve o salaries.csv na mesma pasta deste script.
3) Execute:
   python main.py

O script irá:
- Carregar e mostrar uma visão inicial dos dados (Q1).
- Modelar o ORM com SQLAlchemy usando Enums dinâmicos (Q2).
- Criar o banco SQLite salarios.db (Q3, Q4).
- Popular o banco com pandas.to_sql(if_exists='append') (Q5).
- Executar a consulta pedida de três formas (Q6).
"""

import re
from enum import Enum as PyEnum

import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    String,
    Date,
    Enum as SAEnum,
    func,
    text,
)
from sqlalchemy.orm import declarative_base, Session
from datetime import datetime

# =========================
# 1) CARREGANDO OS DADOS
# =========================

CSV_PATH = "salaries.csv"

print("#" * 80)
print("Carregando dados do CSV...")
df = pd.read_csv(CSV_PATH)

print("\nPrimeiras linhas do DataFrame:")
print(df.head())

print("\nInformações gerais do DataFrame:")
print(df.info())

print("\nDescrição estatística (colunas numéricas):")
print(df.describe())

# =========================
# 2) MODELANDO OS DADOS (ORM)
# =========================

def criar_enum_dinamico(nome_enum: str, valores):
    """
    Cria uma classe Enum do Python dinamicamente a partir de uma lista de valores,
    garantindo que os nomes dos membros sejam identificadores válidos.
    """
    valores_unicos = sorted({str(v) for v in valores if pd.notna(v)})
    membros = {}
    for valor in valores_unicos:
        # Cria um nome de membro válido para Enum (sem espaços, sem caracteres estranhos, não começando com número)
        chave = re.sub(r"\W|^(?=\d)", "_", valor).upper()
        membros[chave] = valor
    return PyEnum(nome_enum, membros)


# Criando Enums dinâmicos com base nos valores ÚNICOS das colunas
SexEnum = criar_enum_dinamico("SexEnum", df["SEX"].unique())
DesignationEnum = criar_enum_dinamico("DesignationEnum", df["DESIGNATION"].unique())
UnitEnum = criar_enum_dinamico("UnitEnum", df["UNIT"].unique())

Base = declarative_base()


def parse_date(date_str: str):
    """
    Converte datas no formato MM/DD/AAAA para datetime.date.
    Se a leitura falhar ou o valor for nulo, retorna None.
    """
    if pd.isna(date_str):
        return None
    if isinstance(date_str, datetime):
        return date_str.date()
    try:
        return datetime.strptime(str(date_str), "%m/%d/%Y").date()
    except Exception:
        return None


class SalaryRecord(Base):
    """
    Modelo ORM que representa a tabela de salários.
    As colunas do banco usam exatamente os nomes do CSV, para que
    o pandas.to_sql consiga inserir com if_exists='append'.
    """

    __tablename__ = "salaries"

    id = Column(Integer, primary_key=True, autoincrement=True)

    first_name = Column("FIRST NAME", String, nullable=False)
    last_name = Column("LAST NAME", String, nullable=False)

    sex = Column("SEX", SAEnum(SexEnum, name="sex_enum"), nullable=False)

    doj = Column("DOJ", Date, nullable=True)  # Date of Joining
    current_date = Column("CURRENT DATE", Date, nullable=True)

    designation = Column(
        "DESIGNATION", SAEnum(DesignationEnum, name="designation_enum"), nullable=False
    )

    age = Column("AGE", Integer, nullable=True)
    salary = Column("SALARY", Float, nullable=False)
    unit = Column(
        "UNIT", SAEnum(UnitEnum, name="unit_enum"), nullable=False
    )
    leaves_used = Column("LEAVES USED", Integer, nullable=True)
    leaves_remaining = Column("LEAVES REMAINING", Integer, nullable=True)
    ratings = Column("RATINGS", Float, nullable=True)
    past_exp = Column("PAST EXP", Float, nullable=True)

    def __repr__(self):
        return (
            f"<SalaryRecord(id={self.id}, name={self.first_name} {self.last_name}, "
            f"designation={self.designation}, salary={self.salary})>"
        )


# =========================
# 3) CONEXÃO COM SQLITE
# =========================

DB_URL = "sqlite:///salarios.db"
print("\n" + "#" * 80)
print(f"Criando engine para o banco: {DB_URL}")
engine = create_engine(DB_URL, echo=False, future=True)

# =========================
# 4) CRIANDO AS TABELAS
# =========================

print("\nCriando as tabelas no banco (se ainda não existirem)...")
Base.metadata.create_all(engine)
print("Tabelas criadas com sucesso.")

# =========================
# 5) POPULANDO O BANCO
# =========================

print("\nConvertendo colunas de data do DataFrame para datetime.date...")

# Se as colunas de data existirem, convertemos
if "DOJ" in df.columns:
    df["DOJ"] = df["DOJ"].apply(parse_date)

if "CURRENT DATE" in df.columns:
    df["CURRENT DATE"] = df["CURRENT DATE"].apply(parse_date)

print("Colunas de data convertidas.")

print("\nPopulando o banco com pandas.to_sql(if_exists='append')...")

df.to_sql(
    name=SalaryRecord.__tablename__,
    con=engine,
    if_exists="append",
    index=False,  # Não queremos uma coluna de índice extra
)

print("Banco populado com sucesso.")

# =========================
# 6) CONSULTAS: SQL vs ORM
# =========================

print("\n" + "#" * 80)
print("Consultas agrupando por DESIGNATION e calculando salário MENSAL:")
print("Mínimo, máximo e média de SALARY/12.\n")

# A) Query SQL “pura” com engine.connect()

query_sql = """
SELECT
    "DESIGNATION" AS designation,
    MIN("SALARY" / 12.0) AS min_monthly_salary,
    MAX("SALARY" / 12.0) AS max_monthly_salary,
    AVG("SALARY" / 12.0) AS avg_monthly_salary
FROM salaries
GROUP BY "DESIGNATION"
ORDER BY avg_monthly_salary DESC;
"""

print("A) Usando engine.connect() e SQL manual:")
with engine.connect() as conn:
    result = conn.execute(text(query_sql))
    for row in result:
        print(
            f"Designation={row.designation:25s} | "
            f"Min={row.min_monthly_salary:10.2f} | "
            f"Max={row.max_monthly_salary:10.2f} | "
            f"Avg={row.avg_monthly_salary:10.2f}"
        )

# B) Query SQL com pandas.read_sql_query

print("\nB) Usando pandas.read_sql_query (mesma SQL):")
with engine.connect() as conn:
    df_group = pd.read_sql_query(query_sql, conn)

print(df_group)

# C) Query com ORM (select + Session)

print("\nC) Usando ORM com select() + Session:")

from sqlalchemy import select

stmt = (
    select(
        SalaryRecord.designation.label("designation"),
        (func.min(SalaryRecord.salary / 12.0)).label("min_monthly_salary"),
        (func.max(SalaryRecord.salary / 12.0)).label("max_monthly_salary"),
        (func.avg(SalaryRecord.salary / 12.0)).label("avg_monthly_salary"),
    )
    .group_by(SalaryRecord.designation)
    .order_by(func.avg(SalaryRecord.salary / 12.0).desc())
)

with Session(engine) as session:
    result = session.execute(stmt).all()

for row in result:
    # row é um Row com os campos definidos no select
    print(
        f"Designation={row.designation:25s} | "
        f"Min={row.min_monthly_salary:10.2f} | "
        f"Max={row.max_monthly_salary:10.2f} | "
        f"Avg={row.avg_monthly_salary:10.2f}"
    )

print("\nFim do script. Veja o banco salarios.db e os resultados acima.")
