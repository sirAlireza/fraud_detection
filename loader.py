import logging
from os.path import join
from sys import stdout

import pandas as pd
from neo4j import GraphDatabase

import settings
from timing import timeit

DATA_FOLDER = settings.DATA_FOLDER_50_MB


class Loader:

    def __init__(self, uri, user, passwd, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, passwd), database=database)

    def close(self):
        self.driver.close()

    @staticmethod
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getLogger("neo4j").addHandler(handler)
        logging.getLogger("neo4j").setLevel(level)

    @timeit
    def create_db(self, c_df, t_df, tx_df):
        with self.driver.session() as session:
            session.write_transaction(self._create_customers, c_df)
            session.write_transaction(self._create_terminals, t_df)
            self._create_indexes()
            # create transactions by chunks due to large size of transactions
            total_transactions = tx_df.count()[0]
            chunk_size = settings.WRITE_TX_CHUNK_SIZE
            print('Total transactions: ', total_transactions)
            index = 0
            finish_flag = True
            while finish_flag:
                start = index * chunk_size
                end = chunk_size + (index * chunk_size)
                if end > total_transactions - 1:
                    index -= 1
                    finish_flag = False
                    start = chunk_size + (index * chunk_size)
                    end = total_transactions - 1
                print(f'Loading chunk {index} -- from {start} to {end}')
                chunk_df = tx_df.iloc[start:end]
                session.write_transaction(self._create_transactions, chunk_df)
                index += 1

    @timeit
    def _create_customers(self, tx, c_df):
        query = (
            "CREATE (c:Customer { id: $c_id }) "
            "RETURN c"
        )
        for index, row in c_df.iterrows():
            tx.run(query, c_id=row['CUSTOMER_ID'])

    @timeit
    def _create_terminals(self, tx, t_df):
        query = (
            "CREATE (t:Terminal { id: $t_id }) "
            "RETURN t"
        )
        for index, row in t_df.iterrows():
            tx.run(query, t_id=row['TERMINAL_ID'])

    @timeit
    def _create_indexes(self):
        query1 = (
            "CREATE INDEX FOR (c:Customer)"
            "ON (c.id)"
        )
        query2 = (
            "CREATE INDEX FOR (t:Terminal)"
            "ON (t.id)"
        )
        with self.driver.session() as session:
            session.run(query1)
            session.run(query2)

    @timeit
    def _create_transactions(self, db_tx, tx_df):
        query3 = ("MATCH "
                  "(c:Customer), "
                  "(t:Terminal) "
                  "WHERE c.id = $c_id AND t.id = $t_id "
                  "CREATE (tx:Transaction { id: $tx_id, amount: $tx_amount, datetime: $tx_datetime, "
                  "is_fraud: $tx_fraud }) "
                  "CREATE (c)-[r1:HAS_TX]->(tx)-[r2:PAYED_TO]->(t) "
                  "RETURN c,tx,t")
        for index, row in tx_df.iterrows():
            db_tx.run(query3,
                      c_id=row["CUSTOMER_ID"], t_id=row["TERMINAL_ID"],
                      tx_id=row['TRANSACTION_ID'], tx_amount=row['TX_AMOUNT'],
                      tx_datetime=row["TX_DATETIME"], tx_fraud=row['TX_FRAUD'])

    @timeit
    def clear_db(self):
        query1 = (
            "match (a) -[r] -> () delete a, r"
        )
        query2 = (
            "match (a) delete a"
        )
        with self.driver.session() as session:
            session.run(query1)
            session.run(query2)


def main():
    """This function loads all generated data by "generator.py" into NoSql database."""

    customers_df = pd.read_csv(join(DATA_FOLDER, 'customers.csv'))
    terminals_df = pd.read_csv(join(DATA_FOLDER, 'terminals.csv'))
    transactions_df = pd.read_csv(join(DATA_FOLDER, 'transactions.csv'))

    Loader.enable_log(logging.INFO, stdout)
    loader = Loader(settings.DB_URL, settings.USERNAME, settings.PASSWORD, settings.DB)
    # clears the database before start
    # app.clear_db()
    loader.create_db(customers_df, terminals_df, transactions_df)
    loader.close()


if __name__ == "__main__":
    main()
