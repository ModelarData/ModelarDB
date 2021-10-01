from pyarrow._flight import FlightServerBase


class FlightServer(FlightServerBase):
    def __init__(self, location):
        super(FlightServer, self).__init__(location=location)
        self.flights = {}

    def do_put(self, context, descriptor, reader, writer):
        data = reader.read_all()
        print(data.to_pandas())


# Listen for Arrow Flight do_put connections and print data received
if __name__ == '__main__':
    flightServer = FlightServer(location="grpc+tcp://localhost:7007")
    flightServer.serve()