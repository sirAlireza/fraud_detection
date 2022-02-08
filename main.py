import logging
from sys import stdout

from neo4j import GraphDatabase

import settings
from timing import timeit


class Handler:

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
    def query_customer_payments(self, tx, start_date="2018-06-01", end_date="2018-07-01", limit=10):
        """Handles first query of the project proposal"""
        query = (
            "MATCH (c:Customer)-[r:HAS_TX]->(tx:Transaction) "
            'where tx.date >= $start_date and tx.date <= $end_date '
            "RETURN c, sum(tx.amount), tx.date order by c, tx.date limit $limit "
        )
        result = tx.run(query,
                        start_date=start_date,
                        end_date=end_date,
                        limit=limit)

        self.show_result(result, 'Result of customer daily payments: ')
        return result

    @timeit
    def query_terminal_fraud_transactions(self, tx, start_date="2018-06-01", end_date="2018-07-01", limit=10):
        """Handles second query of the project proposal:"""
        query = (
            'MATCH (t:Terminal)<-[:PAYED_TO]-(tx:Transaction) '
            'WHERE date(tx.date) > date($start_date) - Duration({days: 30}) '
            'AND date(tx.date) < date($start_date) '
            'WITH t, avg(tx.amount) AS avg_tx '
            'MATCH (t:Terminal)<-[:PAYED_TO]-(tx:Transaction) '
            'WHERE date(tx.date) > date($start_date) AND date(tx.date) < date($end_date) '
            'AND (avg_tx+(avg_tx/2)) < tx.amount '
            'RETURN t, count(tx), avg_tx LIMIT $limit'
        )
        result = tx.run(query,
                        start_date=start_date,
                        end_date=end_date,
                        limit=limit)
        self.show_result(result, 'Result of terminal fraud transactions: ')
        return result

    @timeit
    def query_cc_relationship_with_degree(self, tx, k=4, limit=10):
        """Handles third query of the project proposal:"""
        n = (k * 4) + 4
        query = (
            f'MATCH path=(c1:Customer)-[*{n}]-(c2:Customer) RETURN c1,c2 LIMIT $limit'
        )
        result = tx.run(query,
                        limit=limit)
        self.show_result(result, f'Result of Co-Customer relationship with degree {k}: ')
        return result

    @timeit
    def query_transactions_of_each_period(self, tx, start_date="2018-06-01", end_date="2018-07-01"):
        """Handles fourth query of the project proposal:"""
        query = (
            'MATCH (t:Terminal)<-[:PAYED_TO]-(tx:Transaction) '
            'WHERE date(tx.date) > date($start_date) - Duration({days: 30}) '
            'AND date(tx.date) < date($start_date) '
            'WITH t,avg(tx.amount) AS avg_tx '
            'MATCH (t:Terminal)<-[:PAYED_TO]-(tx:Transaction) '
            'WHERE date(tx.date) > date($start_date) AND date(tx.date) < date($end_date) '
            'AND (avg_tx+(avg_tx/2)) < tx.amount '
            'WITH tx.tx_period AS p, count(tx) AS ctx '
            'MATCH (tx:Transaction) where tx.tx_period = p '
            'RETURN p,ctx,count(tx)'
        )
        result = tx.run(query,
                        start_date=start_date,
                        end_date=end_date)
        self.show_result(result, 'Result of transactions and fraud transactions for each period: ')
        return result

    @staticmethod
    def show_result(result, message='query result: '):
        print(message)
        for row in result:
            print(row)

    def run_queries(self):
        with self.driver.session() as session:
            session.read_transaction(self.query_customer_payments)  # query 1
            session.read_transaction(self.query_terminal_fraud_transactions)  # query 2
            session.read_transaction(self.query_cc_relationship_with_degree)  # query 3
            session.read_transaction(self.query_transactions_of_each_period)  # query 4


def main():
    """This function runs requested queries."""

    Handler.enable_log(logging.INFO, stdout)
    handler = Handler(settings.DB_URL, settings.USERNAME, settings.PASSWORD, settings.WORKING_DB)
    handler.run_queries()
    handler.close()


if __name__ == "__main__":
    main()
