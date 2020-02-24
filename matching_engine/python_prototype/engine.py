from collections import deque
from sortedcontainers import SortedList
import threading


class Order:

    def __init__(self, id, order_type, side, price, quantity):
        self.id = id
        self.type = order_type
        self.side = side.lower()
        self.price = price
        self.quantity = quantity

    def __str__(self):
        return "[" + str(self.price) + " for " + str(self.quantity) + " shares]"

class Trade:

    def __init__(self, buyer, seller, price, quantity):
        self.buy_order_id = buyer
        self.sell_order_id = seller
        self.price = price
        self.quantity = quantity

    def show(self):
        print("[", self.price, self.quantity, "]")

class OrderBook:

    # Constructor con dos arreglos de entrada bid/ask para las ordenes tipo limite.
    def __init__(self, bids=[], asks=[]):
        # Ordena la lista de compra de mayor a menor y la asigna al arreglo bids.
        self.bids = SortedList(bids, key = lambda order: -order.price)
        # Ordena la lista de venta de menor a mayor y la asigna al arreglo asks.
        self.asks = SortedList(asks, key = lambda order: order.price)

    # Obtiene tamaño de los arreglos
    def __len__(self):
        return len(self.bids) + len(self.asks)

    # Obtiene la mayor oferta de compra que es la posicion 0 del arreglo
    def best_bid(self):
        if len(self.bids) > 0:
            return self.bids[0].price
        else:
            return 0

    # Obtiene la menor oferta de venta que es la posicion 0 del arreglo
    def best_ask(self):
        if len(self.asks) > 0:
            return self.asks[0].price
        else:
            return 0

    # Funcion para agregar una nueva orden al libro de ordenes.
    def add(self, order):
        # Adiciona la nueva orden al arreglo de bids
        if order.side == 'buy':
            index = self.bids.bisect_right(order)
            self.bids.add(order)
        # Adiciona la nueva orden al arreglo de asks
        elif order.side == 'sell':
            index = self.asks.bisect_right(order)
            self.asks.add(order)

    # Funcion para eliminar una orden al libro de ordenes.
    def remove(self, order):
        # Elimina la orden al arreglo de bids
        if order.side == 'buy':
            self.bids.remove(order)
        # Elimina la orden al arreglo de asks
        elif order.side == 'sell':
            self.asks.remove(order)

    def getSizeOfBids(self):
        return len(self.bids)

    def getSizeOfAsks(self):
        return len(self.asks)

class MatchingEngine:

    # define el constructor para MatchineEngine
    def __init__(self):
        # Inicializo las variables del objeto
        self.queue = deque()
        self.orderbook = OrderBook()
        self.trades = deque()

    # Metodo para procesar las ordenes que se indican en la variable de entrada.
    def process(self, order):
        # Para las ordenes tipo limite se ejecuta el metodo de emparejamiento de ordenes.
        if order.type == "limit":
            self.match_limit_order(order)

    def get_trades(self):
        trades = list(self.trades)
        return trades

    # Metodo para realizar emparejamiento entre ordenes bids y asks
    def match_limit_order(self, order):
        # Si es una orden de compra se verifica que supere la mejor oferta de venta del libro de ordenes
        if order.side == 'buy' and order.price >= self.orderbook.best_ask():
            filled = 0
            consumed_asks = []
            # Bucle con el total del numero de ofertas de ventas para verificar las condiciones de emparejamiento
            # para la orden de compra entrante.
            for i in range(len(self.orderbook.asks)):
                ask = self.orderbook.asks[i]

                # Condicion 1: El precio de compra debe ser mayor que el mejor precio de venta del libro de ordenes.
                if ask.price > order.price:
                    break # El precio de la oferta de venta es mayor que el de compra, no se puede llenar la orden.
                elif filled == order.quantity:
                    break # La orden fue llenada.

                # Condicion 2: Si la cantidad de la orden de compra es mayor o igual que la orden de venta,
                # se consumira toda la orden de venta.
                if filled + ask.quantity <= order.quantity:
                    filled += ask.quantity

                    # Se adiciona a la cola trade un nuevo objeto tipo Trade con el resultado del emparejamiento.
                    trade = Trade(order.id, ask.id, ask.price, ask.quantity)
                    self.trades.append(trade)
                    # Se adiciona el objeto ask al arreglo de ofertas de venta consumidas para satisfacer la compra.
                    consumed_asks.append(ask)

                # Condicion 3: La cantidad de la orden de compra es menor que la orden de venta. Sera utilizada
                # parcialmente.
                elif filled + ask.quantity > order.quantity:
                    volume = order.quantity-filled
                    filled += volume

                    # Se adiciona a la cola trade el nuevo objeto Trade.
                    trade = Trade(order.id, ask.id, ask.price, volume)
                    self.trades.append(trade)
                    # Se resta a la cantidad de la oferta de venta el valor utilizado por la oferta de compra.
                    ask.quantity -= volume

            # Bucle para borrar del libro de ordenes las ofertas de ventas consumidas.
            for ask in consumed_asks:
                self.orderbook.remove(ask)

            # Condicion 4: si la orden de compra no se lleno completamente, adicionar la orden al libro de ordenes.
            if filled < order.quantity:
                self.orderbook.add(Order(order.id, "limit", order.side, order.price, order.quantity-filled))

        # Si es una orden de venta se verifica que supere la mejor oferta de compra del libro de ordenes.
        elif order.side == 'sell' and order.price <= self.orderbook.best_bid():
            filled = 0
            consumed_bids = []
            # Bucle con el total del numero de ofertas de compras para verificar las condiciones de emparejamiento
            # para la orden de venta entrante.
            for i in range(len(self.orderbook.bids)):
                bid = self.orderbook.bids[i]

                # Condicion 1: El precio de venta debe ser menor que el mejor precio de compra del libro de ordenes.
                if bid.price < order.price:
                    break # El precio de compra es muy bajo para realizar el emparejamiento.
                if filled == order.quantity:
                    break # La orden fue llenada completamente.

                # Condicion 2: Si la cantidad de la orden de venta es mayor o igual que la orden de compra,
                # se consumira toda la orden de compra.
                if filled + bid.quantity <= order.quantity: # order not yet filled, bid will be consumed whole
                    filled += bid.quantity

                    # Se adiciona a la cola trade un nuevo objeto tipo Trade con el resultado del emparejamiento.
                    trade = Trade(order.id, bid.id, bid.price, bid.quantity)
                    self.trades.append(trade)
                    # Se resta a la cantidad de la oferta de compra el valor utilizado por la oferta de venta.
                    consumed_bids.append(bid)

                # Condicion 3: Si la cantidad de la orden de compra es menor que la orden de venta. Sera utilizada
                # parcialmente.
                elif filled + bid.quantity > order.quantity: # order is filled, bid will be consumed partially
                    volume = order.quantity-filled
                    filled += volume
                    trade = Trade(order.id, bid.id, bid.price, volume)
                    self.trades.append(trade)
                    # Se resta a la cantidad de la oferta de compra el valor utilizado por la oferta de venta.
                    bid.quantity -= volume

            # Bucle para borrar del libro de ordenes las ofertas de compras consumidas.
            for bid in consumed_bids:
                self.orderbook.remove(bid)

            # Condicion 4: si la orden de venta no se lleno completamente, adicionar la orden al libro de ordenes.
            if filled < order.quantity:
                self.orderbook.add(Order(order.id, "limit", order.side, order.price, order.quantity-filled))

        # Si no cumple lo anterior, la orden no supero el spread y sera registrada en el libro de ordenes.
        else:
            # Order did not cross the spread, place in order book
            self.orderbook.add(order)

    def cancel_order(self, cancel):
        pass