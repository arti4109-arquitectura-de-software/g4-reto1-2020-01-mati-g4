import socketserver
import engine
import json

class OrderHandler(socketserver.BaseRequestHandler):

    def handle(self):
        while True:
            data = self.request.recv(1024).strip()
            if not data:
                break
            data = json.loads(data.decode('utf-8'))
            order = engine.Order(data['id'],
                                 data['type'],
                                 data['side'],
                                 data['price'],
                                 data['quantity'])
            self.server.matching_engine.process(order)
            self.request.sendall("ACK".encode("utf-8"))

class MatchingEngineServer(socketserver.TCPServer):

    def __init__(self, server_address, matching_engine):
        super().__init__(server_address, OrderHandler)
        self.matching_engine = matching_engine

def serve():
    server = MatchingEngineServer(("localhost", 8080), engine.MatchingEngine())
    server.serve_forever()

if __name__ == "__main__":
    serve()