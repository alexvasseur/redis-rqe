import random
import pyformance.reporters
import redis
from faker import Faker
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from redisearch import Client, TextField, NumericField, TagField, Query, aggregation, reducers
import pyformance

# Connect to Redis (Assumes Redis is running on localhost:6379)
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
rs = []
#rs = Client("person_index", host='localhost', port=6379 )
registry = pyformance.MetricsRegistry()

def index():
    try:
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
        try:
            with registry.timer("query").time():
                #results = rs[threadID].search(query)
                results = rs[threadID].aggregate(agg)
                #rs.aggregate()
            #total = results.total
            rows = results.rows
            #TODO do something
            print(f"{registry.timer("query").get_mean()*1000}",end='\r')
        except redis.exceptions.ResponseError as e:
            print(f"Error querying index: {e}")

# Main function to configure and run the insert process
def main():
    num_threads = 20   # Set the number of concurrent threads
    num_query = 100_000
    index()

    for t in range(num_threads):
        rs.append(Client("person_index", host='localhost', port=6379))

    reporter = pyformance.reporters.ConsoleReporter(registry, reporting_interval=5)
    reporter.start()

    # Start threads to process data insertion concurrently
    while True:
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for t in range(num_threads):
                futures.append(executor.submit(query_redis, t, num_query//num_threads))

            # Wait for all threads to complete
            for future in futures:
                future.result()

        print("All query thread tasks completed.")
        registry.clear()

if __name__ == "__main__":
    main()