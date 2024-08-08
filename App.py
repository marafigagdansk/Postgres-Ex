import psycopg2
import pandas as pd
from time import sleep

def conectDBD():
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            dbname="postgres",
            user="postgres",
            password="senha:vini1612H",
            port=5432
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE Passageiros(
                PassengerId SERIAL PRIMARY KEY,
                Survived    VARCHAR(1)
            );
        """)
    except Exception as e:
            print(f"Erro ao criar a tabela: {e}")

    df = pd.read_csv('C:\\Users\\vinic\\OneDrive\\Documentos\\Projetos\\Postgres-Ex\\titanic (1).csv')
    df['Survived'] = df['Survived'].astype(str)
    try:
        for i, row in df.iterrows():
            cur.execute("""INSERT INTO Passageiros (Survived)
                            VALUES (%s)""", (row['Survived'],))
        conn.commit()
        print("Dados inseridos com sucesso")     

    except Exception as e:
            print(f"Erro ao inserir informações: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
           conn.close()       
        sleep(3)  