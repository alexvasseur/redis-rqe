import random
import argparse
import pyformance.reporters
import redis
from faker import Faker
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from redisearch import Client, TextField, NumericField, TagField, Query, aggregation, reducers
import pyformance
import multiprocessing

def parse_args():
    parser = argparse.ArgumentParser(description="Read Redis connection arguments")
    parser.add_argument("-H", default="localhost", type=str, required=False, help="Redis host")
    parser.add_argument("-p", default=6379, type=int, required=False, help="Redis port")
    parser.add_argument("-a", default=None, type=str, required=False, help="Redis password")
    return parser.parse_args()

rs = []
registry = pyformance.MetricsRegistry()

def index(host, port, password):
    try:
        # Connect to Redis (Assumes Redis is running on localhost:6379)
        r = redis.Redis(host=host, port=port, password=password, db=0, decode_responses=True)
        r.execute_command(
            "FT.CREATE person_index ON JSON PREFIX 1 person: SCHEMA "
            +"$.country AS country TAG "
            +"$.firstname AS firstname TEXT "
            +"$.age AS age NUMERIC SORTABLE "
        )
        print("Index created successfully.")
    except redis.exceptions.ResponseError as e:
        print(f"Error creating index: {e}")

def query_redis(threadID, count):
    agemin = threadID * 10
    agemax = agemin+5
    query_age = f"@age:[{agemin} ({agemax}]"
    query_country = "@country:{France}"
    query = Query(f"{query_country} {query_age}").sort_by('age', asc=False).paging(0, 10)
    agg = aggregation.AggregateRequest(f"{query_country} {query_age}").load("@$.friends", "AS", "friends").sort_by('@age', asc=False).limit(0, 10).filter("@friends>200")
    for i in range(count):
        if (threadID==0):
            with registry.timer("query").time():
                results = rs[threadID].search(query)
                #results = rs[threadID].aggregate(agg)
                total = results.total
                #rows = results.rows
            print(f"{registry.timer('query').get_mean()*1000}",end='\r')
        else:
            results = rs[threadID].search(query)
            total = results.total

# Main function to configure and run the insert process
def main():
    multiprocessing.set_start_method("fork")
    num_threads = 40   # Set the number of concurrent threads
    num_query = 100_000

    args = parse_args()
    index(args.H, args.p, args.a)

    for t in range(num_threads):
        rs.append(Client("person_index", host=args.H, port=args.p, password=args.a))

    reporter = pyformance.reporters.ConsoleReporter(registry, reporting_interval=5)
    reporter.start()

    # Start threads to process data insertion concurrently
    while True:
        futures = []
        with ProcessPoolExecutor(max_workers=num_threads) as executor:
            for t in range(num_threads):
                futures.append(executor.submit(query_redis, t, num_query//num_threads))

        # Wait for all threads to complete
        for future in futures:
            future.result()

        print("All query thread tasks completed.")
        registry.clear()

if __name__ == "__main__":
    main()