import csv
from time import time


def measure_time(f):
    def wrapper(*args):
        method_name = f"{str(args[0].__class__).split('.')[-1][:-2]}.{f.__name__}"
        start = time()
        temp_res = f(*args)
        print(f"{method_name} Executed in: {time() - start}")
        return temp_res

    return wrapper


def write_results_to_file(directory, file_name, data):
    with open(f'{directory}/{file_name}.csv', 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(['id', 'name', 'price', 'rating', 'rating_count', 'timestamp'])

        for product in data:
            csv_writer.writerow([product.get("_id", product.get("id")), product["name"], product["price"],
                                product["rating"], product["rating_count"], product["timestamp"]])
