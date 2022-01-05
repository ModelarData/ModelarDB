import socket
from numpy.random import rand
from pyarrow.flight import FlightClient, FlightDescriptor


def generate_data(n):
    random_values = rand(n)
    random_data = list(zip(range(1, n+1), random_values))
    return random_data


edgeClient = FlightClient("grpc+tcp://localhost:6006")
serverClient = FlightClient("grpc+tcp://localhost:7007")
flightDescriptor = FlightDescriptor.for_path("/test/data")

# data = pyarrow.csv.read_csv("test-data.csv")
data = generate_data(100)
# df = pd.DataFrame(rand(100))
# arrowData = pyarrow.Table.from_pandas(df)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind(("127.0.0.1", 9999))
    print("Waiting for connection...")
    s.listen()
    conn, addr = s.accept()
    print(f"Accepted connection from {addr}")
    with conn:
        print("Sending data...")
        for d in data:
            conn.send(bytearray(f"{d[0]} {d[1]} \n", "UTF-8"))
        print("Done")

print("Socket closed")