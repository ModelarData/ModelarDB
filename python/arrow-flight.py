from pyarrow.flight import FlightClient
from pyarrow.flight import Ticket
import pyarrow.flight
import pandas as pd

# edgeClient = FlightClient("grpc+tcp://localhost:6001")
edgeClient = FlightClient("grpc+tcp://localhost:7007")

# edgeReader = edgeClient.do_get(Ticket("select sum_s(*) from segment"))
# edgeReader = edgeClient.do_get(Ticket("select count_s(*) from segment"))
# edgeReader = edgeClient.do_get(Ticket("select * from DataPoint where timestamp < cast('2011-04-19 15:00:00' as timestamp)"))
# edgeReader = edgeClient.do_get(Ticket("select avg_s(value) from DataPoint where timestamp < cast('2011-04-19 15:00:00' as timestamp)"))
# edgeReader = edgeClient.do_get(Ticket("select sum_s(value) from DataPoint where timestamp < cast('2011-04-19 15:00:00' as timestamp)"))
edgeReader = edgeClient.do_get(Ticket("select max_s(value) from DataPoint where timestamp < cast('2011-04-19 15:00:00' as timestamp)"))
df = edgeReader.read_pandas()
# df["start_time"] = pd.to_datetime(df["start_time"], unit="ms")
print(df.to_string())

# results = edgeClient.do_action(pyarrow.flight.Action("GID", b"edge-1,4"))
# results = edgeClient.do_action(pyarrow.flight.Action("TID", b"edge-5,7"))
# for result in results:
#     print("Got result", result.body.to_pybytes())