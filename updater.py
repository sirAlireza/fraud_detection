import logging
import random
import time
from sys import stdout

from neo4j import GraphDatabase

import settings
from timing import timeit

PRODUCTS = ['high_tech', 'consumable', 'food', 'clothing', 'other']
PERIODS = ['morning', 'afternoon', 'evening', 'night']


class Updater:

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
    def update_db(self):
        self.add_products()
        self.add_periods()
        self.add_buying_friends_relations()

    @timeit
    def add_products(self):
        random.seed(int(time.time()))
        query = (
            "MATCH (tx:Transaction) "
            "WHERE tx.tx_product IS NULL "
            "WITH tx LIMIT $limit "
            "SET tx.tx_product = $tx_product "
            "RETURN count(tx)"
        )
        with self.driver.session() as session:
            chunk_size = settings.UPDATE_TX_CHUNK_SIZE
            count_rows = chunk_size
            total_updated_rows = 0
            while count_rows == chunk_size:
                updated_rows = session.run(query,
                                           tx_product=random.choice(PRODUCTS),
                                           # tx_product=None, # to remove products
                                           limit=chunk_size)
                count_rows = int(updated_rows.single()[0])
                total_updated_rows += count_rows
                print(f'{total_updated_rows} rows updated...', end='\r')
        print('adding products done.')

    @timeit
    def add_periods(self):
        random.seed(int(time.time()))
        query = (
            "MATCH (tx:Transaction) "
            "WHERE tx.tx_period IS NULL "
            "WITH tx LIMIT $limit "
            "SET tx.tx_period = $tx_period "
            "RETURN count(tx)"
        )
        with self.driver.session() as session:
            chunk_size = settings.UPDATE_TX_CHUNK_SIZE
            count_rows = chunk_size
            total_updated_rows = 0
            while count_rows == chunk_size:
                updated_rows = session.run(query,
                                           tx_period=random.choice(PERIODS),
                                           # tx_period=None, # to remove products
                                           limit=chunk_size)
                count_rows = int(updated_rows.single()[0])
                total_updated_rows += count_rows
                print(f'{total_updated_rows} rows updated...', end='\r')
        print('adding periods done.')

    @timeit
    def add_buying_friends_relations(self):
        query = (
            "MATCH (c:Customer)-[:HAS_TX]->(tx:Transaction {tx_product:$product})-[:PAYED_TO]->(t:Terminal) "
            "with t as t, count(tx) as c_tx, c as c limit $limit "
            "where c_tx > 3 "
            "MERGE (c)-[r:BUYING_FRIENDS]-(t) "
            "SET r.products = [$product] + coalesce(r.products, []) "
            "return count(c)"
        )
        with self.driver.session() as session:
            chunk_size = settings.UPDATE_TX_CHUNK_SIZE
            for p in PRODUCTS:
                count_rows = chunk_size
                total_updated_rows = 0
                while count_rows == chunk_size:
                    updated_rows = session.run(query,
                                               product=p,
                                               limit=chunk_size)
                    count_rows = int(updated_rows.single()[0])
                    total_updated_rows += count_rows
                    print(f'{total_updated_rows} rows updated...', end='\r')
                print(f'adding relationships for product "{p}" is done.')
        print('adding "buying_friends" relationship is done.')


def main():
    """This function updates all generated data by adding products, periods and buying friends relationship."""

    Updater.enable_log(logging.INFO, stdout)
    updater = Updater(settings.DB_URL, settings.USERNAME, settings.PASSWORD, settings.WORKING_DB)
    updater.update_db()
    updater.close()


if __name__ == "__main__":
    main()
