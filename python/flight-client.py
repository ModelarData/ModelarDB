import socket

from numpy.random import rand
import pandas as pd
from pyarrow.flight import FlightClient, FlightDescriptor
import pyarrow.csv
from pyarrow.flight import Ticket


def generate_data(n):
    random_values = rand(n)
    random_data = list(zip(range(1, n+1), random_values))
    return random_data


def save_data(data: list[tuple[int, float]]):
    with open("test-data.csv", "w") as csv_file:
        formatted_data = [f"{ts},{value}\n" for (ts, value) in data]
        csv_file.writelines(formatted_data)


if __name__ == '__main__':
    edgeClient = FlightClient("grpc+tcp://localhost:6006")
    serverClient = FlightClient("grpc+tcp://localhost:7007")
    flightDescriptor = FlightDescriptor.for_path("/test/data")

    # data = pyarrow.csv.read_csv("test-data.csv")
    data = generate_data(100)
    # df = pd.DataFrame(rand(100))
    # arrowData = pyarrow.Table.from_pandas(df)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 9999))
        s.listen()
        conn, _ = s.accept()
        with conn:
            for d in data:
                conn.send(bytearray(f"{d[0]} {d[1]} \n", "UTF-8"))

    # Check edge is empty
    edgeReader = edgeClient.do_get(Ticket("select count(*) from segment"))
    edgeReader.read_pandas()
    edgeCount == 0

    # Write test data to edge
    writer, _ = edgeClient.do_put(flightDescriptor, arrowData.schema)
    writer.write_table(arrowData)
    writer.close()

    # Read test data from server
    reader = serverClient.do_get(Ticket("select count(*) from segment"))
    df = reader.read_pandas()
    print(df.head())
    print(df.columns)