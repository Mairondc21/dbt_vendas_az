import duckdb
from dotenv import load_dotenv
from pathlib import Path
import os

def load_settings():
    dotenv_path = Path.cwd() / '.env'
    load_dotenv(dotenv_path=dotenv_path)

    settings = {
        "db_host_prod": os.getenv("DB_HOST_PROD"),
        "db_name_prod": os.getenv("DB_NAME_PROD"),
        "db_user_prod": os.getenv("DB_USER_PROD"),
        "db_pass_prod": os.getenv("DB_PASS_PROD"),
        "db_port_prod": os.getenv("DB_PORT_PROD"),
    }
    return settings

def connect_postgres():
    settings = load_settings()
    conn = duckdb.connect()

    conn.execute("INSTALL postgres_scanner;")
    conn.execute("LOAD postgres_scanner;")


    postgres_connection = f"postgres://{settings['db_user_prod']}:{settings['db_pass_prod']}@{settings['db_host_prod']}:{settings['db_port_prod']}/{settings['db_name_prod']}"

    conn.execute(f"ATTACH '{postgres_connection}' AS postgres_db (TYPE POSTGRES);")

    return conn

def create_table_dl():
    conn_pg = connect_postgres()


    create_table_query = """
     CREATE OR REPLACE TABLE postgres_db.raw.tb_localidade (
        id_localidade INTEGER,
        nome_cidade VARCHAR,
        nome_estado VARCHAR,
        nome_regiao VARCHAR,
        PRIMARY KEY (id_localidade)
    );

     CREATE OR REPLACE TABLE postgres_db.raw.tb_marca (
        id_marca INTEGER,
        nome_marca VARCHAR,
        PRIMARY KEY (id_marca)
    );

    CREATE OR REPLACE TABLE postgres_db.raw.tb_usuarios (
        id_usuario INTEGER,
        nome_usuario VARCHAR,
        email VARCHAR,
        navegador VARCHAR,
        data_nascimento DATE,
        data_cadastro DATE,
        fk_usuarios_localidade INTEGER,
        PRIMARY KEY (id_usuario)
    );
    CREATE OR REPLACE TABLE postgres_db.raw.tb_produtos (
        id_produto INTEGER,
        nome_produto VARCHAR,
        fk_produtos_marca INTEGER,
        preco FLOAT,
        categoria VARCHAR,
        PRIMARY KEY (id_produto)
    );
    CREATE OR REPLACE TABLE postgres_db.raw.tb_lojas (
        id_loja INTEGER,
        nome_loja VARCHAR,
        fk_lojas_localidade INTEGER,
        data_abertura DATE,
        PRIMARY KEY (id_loja)
    );
   
       CREATE OR REPLACE TABLE postgres_db.raw.tb_vendas (
        id_venda INTEGER,
        fk_venda_usuario INTEGER,
        fk_venda_loja INTEGER,
        data_venda DATE,
        valor_total FLOAT,
        PRIMARY KEY (id_venda)
    );
    CREATE OR REPLACE TABLE postgres_db.raw.tb_item_venda (
        id_item_venda INTEGER,
        fk_item_venda_vendas INTEGER,
        fk_item_venda_produto INTEGER,
        quantidade INTEGER,
        preco_unitario FLOAT,
        preco_total FLOAT,
        PRIMARY KEY (id_item_venda)
    );
    """
    conn_pg.execute(create_table_query)
    conn_pg.close()

def insert_table_dl(id: int, id_usuarios: str, nome_usuario: str, sistema_origem: str, navegador: str, acao: str):
    conn_pg = connect_postgres()

    insert_query = """
    INSERT INTO postgres_db.az_bucket
    (id, id_usuario, nome_usuario, sistema_origem, navegador, acao_realizada, created_at)
    VALUES
    (?, ?, ?, ?, ?, ?, NOW());
    """
    data = (id, id_usuarios, nome_usuario, sistema_origem, navegador, acao)

    conn_pg.execute(insert_query,data)


    result = conn_pg.execute("SELECT * FROM postgres_db.az_bucket").fetchall()


    print("Dados inseridos na tabela:")
    for row in result:
        print(row)

                                        
    conn_pg.close()

create_table_dl()


