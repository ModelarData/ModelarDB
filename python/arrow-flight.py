from pyarrow.flight import FlightClient
from pyarrow.flight import Ticket
import pyarrow.flight

# edgeClient = FlightClient("grpc+tcp://localhost:6001")
edgeClient = FlightClient("grpc+tcp://localhost:7007")

# edgeReader = edgeClient.do_get(Ticket("select sum_s(*) from segment"))
# edgeReader = edgeClient.do_get(Ticket("select count_s(*) from segment"))
edgeReader = edgeClient.do_get(Ticket("select * from segment limit 3"))
# edgeReader = edgeClient.do_get(Ticket("select count(*) from segment"))
df = edgeReader.read_pandas()
print(df)

# results = edgeClient.do_action(pyarrow.flight.Action("GID", b"edge-1,4"))
# results = edgeClient.do_action(pyarrow.flight.Action("TID", b"edge-5,7"))
# for result in results:
#     print("Got result", result.body.to_pybytes())