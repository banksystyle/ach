import psycopg2
from psycopg2 import OperationalError
import xml.etree.ElementTree as ET
import glob

def create_connection(db_name, db_user, db_password, db_host, db_port):
    """Функция для подключения к PostreSQL"""
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )    
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    """Функция для выполнения запросов"""
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
#         print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

def execute_read_query(connection, query):
    """Функция для чтения данных"""
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")

connection = create_connection("postgres", "postgres", "12345", "127.0.0.1", "5432")

#Создание базы данных rsmppp
create_database_query = "CREATE DATABASE rsmppp"
execute_query(connection, create_database_query)
#Подключение к базе данных rsmppp
connection = create_connection("rsmppp", "postgres", "12345", "127.0.0.1", "5432")

#Описание структуры таблиц базы данных
create_individual_table = """
CREATE TABLE IF NOT EXISTS individual (
  ИННФЛ CHAR(12) PRIMARY KEY,
  Фамилия VARCHAR(60) NOT NULL,
  Имя VARCHAR(60) NOT NULL,
  Отчество VARCHAR(60)
)
"""

create_business_table = """
CREATE TABLE IF NOT EXISTS business (
  ИННЮЛ CHAR(10) PRIMARY KEY,
  НаимОрг VARCHAR(1000) NOT NULL
)
"""

create_supply_table = """
CREATE TABLE IF NOT EXISTS supply (
  ИННЮЛ CHAR(10) PRIMARY KEY,
  НаимОрг VARCHAR(1000) NOT NULL
)
"""

create_form_table = """
CREATE TABLE IF NOT EXISTS form (
  КодФорм CHAR(4) PRIMARY KEY,
  НаимФорм VARCHAR(100) NOT NULL
)
"""

create_type_table = """
CREATE TABLE IF NOT EXISTS type (
  КодВид CHAR(4) PRIMARY KEY,
  НаимВид VARCHAR(200) NOT NULL
)
"""
#Создание таблиц
execute_query(connection, create_individual_table)
execute_query(connection, create_business_table)
execute_query(connection, create_supply_table)
execute_query(connection, create_form_table)
execute_query(connection, create_type_table)

#Описание основной таблицы
create_main_table = """
CREATE TABLE IF NOT EXISTS main (
  ИД SERIAL PRIMARY KEY,
  ИдДок VARCHAR(36) NOT NULL,
  ДатаСост CHAR(10) NOT NULL,
  ИННПолПод VARCHAR(12),
  ИННПредПод CHAR(10) REFERENCES supply (ИННЮЛ),
  КатСуб CHAR(1) NOT NULL,
  ВидПП CHAR(1) NOT NULL,
  ДатаПрин CHAR(10) NOT NULL,
  СрокПод CHAR(10) NOT NULL,
  ДатаПрекр VARCHAR(10) NULL,
  КодФорм CHAR(4) REFERENCES form (КодФорм),
  КодВид CHAR(4) REFERENCES type (КодВид),
  ЕдПод CHAR(1) NOT NULL,
  РазмПод NUMERIC(13,2) NOT NULL,
  ИнфНецел CHAR(1) NOT NULL,
  ИнфНаруш CHAR(1) NOT NULL
)
"""
#Создание основной таблицы
execute_query(connection, create_main_table)



#Парсинг файлов и наполение базы данных
path = 'db\*'  # Путь к файлам xml
files = glob.glob(path)
for file in files:
    tree = ET.parse(file)
    root = tree.getroot()
    for doc in root.findall('Документ'):      
        for pp in doc.findall('СвПредПод'):
            doc_id = doc.get('ИдДок')
            doc_date = doc.get('ДатаСост')
            org_inn = pp.get('ИННЮЛ')
            accept_date = pp.get('ДатаПрин')
            valid_date = pp.get('СрокПод')
            end_date = pp.get('ДатаПрекр')
            category = pp.get('КатСуб')
            type_pp = pp.get('ВидПП')
            form_cod = pp.find('ФормПод').get('КодФорм')
            type_cod = pp.find('ВидПод').get('КодВид')
            sup_unit = pp.find('РазмПод').get('ЕдПод')
            sup_vol = pp.find('РазмПод').get('РазмПод')
            inf_appr = pp.find('ИнфНаруш').get('ИнфНецел')
            inf_offe = pp.find('ИнфНаруш').get('ИнфНаруш')

            if doc.find('СвФЛ'):
                inn = doc.find('СвФЛ').get('ИННФЛ')
                if not execute_read_query(connection, f"SELECT * FROM individual WHERE ИННФЛ = '{inn}'"):
                    first_name = doc.find('СвФЛ').find('ФИО').get('Имя')
                    middle_name = doc.find('СвФЛ').find('ФИО').get('Отчество')
                    last_name = doc.find('СвФЛ').find('ФИО').get('Фамилия')

                    insert_individual_table = f"""INSERT INTO individual (ИННФЛ, Фамилия, Имя, Отчество)\
                            VALUES ('{inn}', '{last_name}', '{first_name}', '{middle_name}')"""
                    execute_query(connection, insert_individual_table)       
            else:
                inn = doc.find('СвЮЛ').get('ИННЮЛ')
                if not execute_read_query(connection, f"SELECT * FROM business WHERE ИННЮЛ = '{inn}'"):
                    ul_name = doc.find('СвЮЛ').get('НаимОрг')

                    insert_business_table = f"""INSERT INTO business (ИННЮЛ, НаимОрг) \
                            VALUES ('{inn}', '{ul_name}')"""
                    execute_query(connection, insert_business_table)       

            if not execute_read_query(connection, f"SELECT * FROM supply WHERE ИННЮЛ = '{org_inn}'"):
                org_name = pp.get('НаимОрг')            
                insert_supply_table = f"""INSERT INTO supply (ИННЮЛ, НаимОрг) \
                            VALUES ('{org_inn}', '{org_name}')"""
                execute_query(connection, insert_supply_table)

            if not execute_read_query(connection, f"SELECT * FROM form WHERE КодФорм = '{form_cod}'"):
                form_name = pp.find('ФормПод').get('НаимФорм')            
                insert_form_table = f"""INSERT INTO form (КодФорм, НаимФорм) \
                            VALUES ('{form_cod}', '{form_name}')"""
                execute_query(connection, insert_form_table)

            if not execute_read_query(connection, f"SELECT * FROM type WHERE КодВид = '{type_cod}'"):
                type_name = pp.find('ВидПод').get('НаимВид')
                insert_type_table = f"""INSERT INTO type (КодВид, НаимВид) \
                            VALUES ('{type_cod}', '{type_name}')"""
                execute_query(connection, insert_type_table)

            insert_main_table = f"""INSERT INTO main (ИдДок, ДатаСост, ИННПолПод, ИННПредПод, \
КатСуб, ВидПП, ДатаПрин, СрокПод, ДатаПрекр, КодФорм, КодВид, ЕдПод, РазмПод, ИнфНецел, ИнфНаруш)
VALUES ('{doc_id}', '{doc_date}', '{inn}', '{org_inn}', '{category}', '{type_pp}', '{accept_date}',\
'{valid_date}', '{end_date}', '{form_cod}', '{type_cod}', '{sup_unit}', {sup_vol}, '{inf_appr}', '{inf_offe}')"""

            execute_query(connection, insert_main_table)

execute_query(connection, "UPDATE main SET ДатаПрекр = NULL WHERE ДатаПрекр = 'None'")
execute_query(connection, "UPDATE individual SET Отчество = NULL WHERE Отчество = 'None'")

connection.close()
