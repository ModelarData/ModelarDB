from pyarrow._flight import FlightClient
from pyarrow.flight import Ticket

# edgeClient = FlightClient("grpc+tcp://localhost:6006")
# edgeReader = edgeClient.do_get(Ticket("select count(*) from segment"))
edgeClient = FlightClient("grpc+tcp://localhost:7007")
# edgeReader = edgeClient.do_get(Ticket("select count(*) from modelardb.segment"))
edgeReader = edgeClient.do_get(Ticket("select count(*) from segment"))
# edgeReader = edgeClient.do_get(Ticket("select * from segment limit 3"))
# edgeReader = edgeClient.do_get(Ticket("select * from segment"))
df = edgeReader.read_pandas()
print(df)