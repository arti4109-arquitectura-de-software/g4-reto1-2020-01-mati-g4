import socket
import json
import threading
import random
from timeit import default_timer as timer

import engine as m_engine
import server as m_server

# Emparejamiento sin conexiones sobre sockets cargando ordenes a memoria
def direct_benchmark(nr_of_orders, midpoint=100):

    # Instancia de engine (motor emparejamiento)
    engine = m_engine.MatchingEngine()

    # Se declara el tamaño del libro de ordenes
    orderbook_size = len(engine.orderbook)

    # Se define el arreglo de ordenes
    orders = []
    count_bid = 0
    count_ask = 0
    #  bucle para crear un libro de ordenes tipo limite (Venta y Compra) aleatorio asignado a un arreglo de objetos
    #  tipo engine.order.
    for i in range(nr_of_orders):
        # Establece de manera aleatoria si es compra o venta
        side = random.choice(["buy", "sell"])
        if side == "buy":
            count_bid +=1
        else:
            count_ask +=1

        # Establece de manera aleatoria el precio utilizando la funcion de gauss con punto medio 100 variacion entre 95 y 105.
        price = random.gauss(midpoint, 5)
        # Establece un valor aleatorio para la cantidad de activos en la orden.
        quantity = random.expovariate(0.05)
        # Construye la orden segun los datos obtenidos y lo adiciona al arreglo order
        orders.append(m_engine.Order("Null", "limit", side, price, quantity))

    # Comienza a medir el tiempo de emparejamiento.
    start = timer()
    # Bucle que envia a procesamiento orden por orden.
    for order in orders:
        # envio orden a procesamiento del emparejador.
        engine.process(order)
    # Mide el tiempo de emparejamiento despues de ejecutar el total de ordenes.
    end = timer()
    t = end - start
    # Indica el tiempo utilizado para procesar el total del numero de ordenes.

    print('Ordenes de compra: {0}, Ordenes de venta: {1}'.format(count_bid, count_ask ))
    print('{0} ordenes procesadas en {1:.2f} segundos,'.format(nr_of_orders, t))
    print("a una tasa promedio de {0:.0f} orders/second o {1:.2f} microsegundos/order,".format((nr_of_orders/t), (t/nr_of_orders) * 1000 * 1000))
    print('ordenes tipo limite sin emparejamiento: {0} y ordenes emparejadas {1}.'.format( len(engine.orderbook)-orderbook_size, len(engine.trades)))
    print('ordenes de compra sin emparejar: {0}, ordenes de venta sin emparejar: {1}'.format(engine.orderbook.getSizeOfBids(), engine.orderbook.getSizeOfAsks() ))

# Emparejamiento sin conexiones sobre sockets cargando ordenes a memoria
def socket_benchmark(nr_of_orders, midpoint=100):
    # Define un objeto de la clase MatchingEngine.
    engine = m_engine.MatchingEngine()
    # Define un objeto de la clase server.
    server = m_server.MatchingEngineServer(("localhost", 8080), engine)

    # Crea un Thread (Hilo) para este ibro de ordenes
    t = threading.Thread(target=server.serve_forever)
    t.daemon = True
    t.start()

    # Se declara el tamaño del libro de ordenes
    orderbook_size = len(engine.orderbook)
    orders = []
    count_bid = 0
    count_ask = 0

    # Bucle para crear un libro de ordenes tipo limite (Venta y Compra) aleatorio como dato de entrada a un socket.
    for i in range(nr_of_orders):
        side = random.choice(["buy", "sell"])

        if side == "buy":
            count_bid +=1
        else:
            count_ask +=1

        price = random.gauss(midpoint, 5)
        quantity = random.expovariate(0.05)
        orders.append({'id': 0, 'type': 'limit', 'side': side, 'price': price, 'quantity': quantity})

    # Comienza a medir el tiempo de emparejamiento.
    start = timer()
    # Crea el objeto tipo Socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Establece una conexion por ip y puerto
    sock.connect(("localhost", 8080))

    # Bucle para simular el ingreso de las multiples ordenes a traves de conexiones por socket.
    for order in orders:
        serialized_order = json.dumps(order).encode('utf-8')
        sock.sendall(serialized_order)
        acknowledge = str(sock.recv(1024), "utf-8")

    # Finaliza el tiempo de emparejamiento.
    end = timer()
    t = end - start

    print('Ordenes de compra: {0}, Ordenes de venta: {1}'.format(count_bid, count_ask))
    print('{0} ordenes procesadas en {1:.2f} segundos,'.format(nr_of_orders, t))
    print("a una tasa promedio de {0:.0f} orders/second o {1:.2f} microsegundos/order,".format((nr_of_orders / t), (
                t / nr_of_orders) * 1000 * 1000))
    print('ordenes tipo limite sin emparejamiento: {0} y ordenes emparejadas {1}.'.format(
        len(engine.orderbook) - orderbook_size, len(engine.trades)))
    print('ordenes de compra sin emparejar: {0}, ordenes de venta sin emparejar: {1}'.format(
        engine.orderbook.getSizeOfBids(), engine.orderbook.getSizeOfAsks()))

print("direct_benchmark(100000)")
direct_benchmark(100000)
