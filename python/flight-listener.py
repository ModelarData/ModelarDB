from pyarrow._flight import FlightServerBase
import pyarrow.flight

class FlightServer(FlightServerBase):
    def __init__(self, location):
        super(FlightServer, self).__init__(location=location)
        self.flights = {}

    def do_put(self, context, descriptor, reader, writer):
        data = reader.read_all()
        # print(data.to_pandas())

    def do_action(self, context, action):
        print(context.peer)
        print(context.peer_identity)
        if action.type == "TID":
            yield pyarrow.flight.Result(pyarrow.py_buffer(b"10"))
        else:
            yield pyarrow.flight.Result(pyarrow.py_buffer(b"20"))


# Listen for Arrow Flight do_put connections and print data received
if __name__ == '__main__':
    flightServer = FlightServer(location="grpc+tcp://localhost:7007")
    flightServer.serve()