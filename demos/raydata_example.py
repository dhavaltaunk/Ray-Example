import ray
from typing import Any


INPUT = [
    {'Key': 'A', 'Value': 1},
    {'Key': 'B', 'Value': 2},
    {'Key': 'C', 'Value': 3},
    {'Key': 'D', 'Value': 4},
    {'Key': 'E', 'Value': 5},
    {'Key': 'F', 'Value': 6},
    {'Key': 'G', 'Value': 7},
    {'Key': 'H', 'Value': 8},
    {'Key': 'I', 'Value': 9},
    {'Key': 'J', 'Value': 10}
]


def process(row: dict[str, Any]) -> dict[str, Any]:
    val = row['Value']
    new_val = val * val
    return {'Original Key': row['Key'], 'Original Val': val, 'Squared': new_val}


def run():
    ray.init()

    # Lazy reference to a large dataset
    ds = ray.data.from_items(INPUT)

    # Define the distributed transformation
    processed_ds = ds.map(process)
    # processed_ds = [process.remote(i) for i in INPUT]

    processed_ds.show()
