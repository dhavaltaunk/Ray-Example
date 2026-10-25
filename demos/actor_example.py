import ray
import time

# Define the stateful worker - Actor
@ray.remote
class TaskCounter:
    def __init__(self):
        self.total = 0

    def add(self, n: int):
        self.total += n

    def get_total(self) -> int:
        return self.total

# Ray task accepts actor handle
@ray.remote
def process(n: int, task_counter):
    task_counter.add.remote(n*n)
    time.sleep(1)
    return n*n


def run():
    ray.init()
    inputs = list(range(8))
    counter = TaskCounter.remote()

    task_refs = [process.remote(n, counter) for n in inputs]

    results = ray.get(task_refs)

    expected_total = sum(results)
    final_total_ref = counter.get_total.remote()
    final_total = ray.get(final_total_ref)

    assert final_total == expected_total

    print(f'Final total = Expected total = {final_total}')

    ray.shutdown()
