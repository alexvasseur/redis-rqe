import random
import pyformance.reporters
import redis
from faker import Faker
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pyformance

# Initialize Faker
fake = Faker()
r = []
# Connect to Redis (Assumes Redis is running on localhost:6379)
#r. = redis.Redis(host='localhost', port=6379, db=0)
registry = pyformance.MetricsRegistry()

# Function to generate a random timestamp within the last 2 years
def random_timestamp():
    today = datetime.now()
    start_date = today - timedelta(days=730)
    random_date = start_date + timedelta(days=random.randint(0, 730))
    return int(random_date.timestamp())

# Function to generate a single JSON object (person data)
def generate_person():
    return {
        "firstname": fake.first_name(),
        "lastname": fake.last_name(),
        "city": fake.city(),
        "country": fake.country(),
        "email": fake.email(),
        "age": random.randint(1, 99),
        "score": random.randint(0, 100),
        "friends": random.randint(0, 500),
        "registrationDate": random_timestamp(),
        "connectionDate": random_timestamp(),
    }

# Function to insert data into Redis using pipeline in batches
def insert_into_redis(threadID, start_index, count, batch_size=100):
    pipeline = r[threadID].pipeline()

    for i in range(count):
        person = generate_person()
        key = f"person:{start_index + i}"  # Generate a unique Redis key for each object
        pipeline.json().set(key, '$', person)  # Use RedisJSON to set the full JSON object
        
        # Execute pipeline when batch size is reached
        if (i + 1) % (batch_size) == 0:
            with registry.timer("pipeline").time():
                pipeline.execute()
            #print(f"{start_index + i + 1} JSON objects inserted so far...", end='\r')
            print(f"{registry.timer('pipeline').get_mean()*1000/batch_size}",end='\r')

    # Execute the remaining objects in the pipeline if any
    if count % batch_size != 0:
        pipeline.execute()
        print(f"Final batch for thread starting at {start_index} inserted. Total {count} JSON objects inserted.")

# Main function to configure and run the insert process
def main():
    total_json = 2_000_000  # Set the total number of JSON objects to be created
    batch_size = 100   # Set the pipeline batch size
    num_threads = 20   # Set the number of concurrent threads

    for t in range(num_threads):
        r.append(redis.Redis(host='localhost', port=6379, db=0))

    reporter = pyformance.reporters.ConsoleReporter(registry, reporting_interval=5)
    reporter.start()

    # Calculate the number of objects each thread should handle
    count_per_thread = total_json // num_threads
    remainder = total_json % num_threads

    # Start threads to process data insertion concurrently
    while True:
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for t in range(num_threads):
                start_index = t * count_per_thread
                count = count_per_thread + (1 if t < remainder else 0)  # Distribute any remainder
                futures.append(executor.submit(insert_into_redis, t, start_index, count, batch_size))

            # Wait for all threads to complete
            for future in futures:
                future.result()

        print("All data insertion tasks completed.")
        registry.clear()

if __name__ == "__main__":
    main()