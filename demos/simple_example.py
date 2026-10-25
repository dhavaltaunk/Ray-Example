import ray
import time

# Make this a remote task
@ray.remote
def process(n: int) -> int:
    time.sleep(1)
    return n * n


def run():
    # Initialize Rat
    ray.init()

    input = list(range(8))
    start_time = time.time()

    result_refs = [process.remote(i) for i in input]
    results = ray.get(result_refs)

    end_time = time.time()
    print(f'Processed {len(results)} tasks in {end_time - start_time:.2f} seconds')
    print(f'Results : {results}')

    ray.shutdown()