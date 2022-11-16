from collections import deque
import heapq

f = open("supermarkt.txt", "w")
fc = open("supermarkt_customer.txt", "w")
fs = open("supermarkt_station.txt", "w")


# print on console and into supermarket log
def my_print(msg):
    print(msg)
    f.write(msg + '\n')


# print on console and into customer log
# k: customer name
# s: station name
def my_print1(k, s, msg):
    t = EvQueue.time
    print(str(round(t, 4)) + ':' + k + ' ' + msg + ' at ' + s)
    fc.write(str(round(t, 4)) + ':' + k + ' ' + msg + ' at ' + s + '\n')


# print on console and into station log
# s: station_name
# name: customer name
def my_print2(s, msg, name):
    t = EvQueue.time
    # print(str(round(t,4))+':'+s+' '+msg)
    fs.write(str(round(t, 4)) + ':' + s + ' ' + msg + ' ' + name + '\n')


# class consists of instance variables:
# t: time stamp
# work: job to be done
# args: list of arguments for job to be done
# prio: used to give leaving, being served, and arrival different priorities
class Ev:
    counter = 0

    def __init__(self, t, work, args=(), prio=255):
        self.t = t
        self.n = Ev.counter
        self.work = work
        self.args = args
        self.prio = prio
        Ev.counter += 1


# class consists of
# q: event queue
# time: current time
# evCount: counter of all popped events
# methods push, pop, and start as described in the problem description
class EvQueue:
    q = []
    time = 0
    evCount = 0

    def push(self, event):
        heapq.heappush(self.q, (event.t, event.prio, event.n, event.work, event.args))

    def pop(self):
        return heapq.heappop(self.q)

    def start(self):
        heapq.heapify(self.q)
        while self.q:
            entry = self.pop()
            self.time = entry[0]
            work = entry[3]
            station = work[2]
            costumer = work[3]
            if costumer.set_next_work():
                ev = Ev(self.time + work[1], costumer.run, prio=1)
                self.push(ev)
            if station.has_items_in_queue() and not station.get_is_busy():
                new_costumer = station.remove_customer_from_queue()
                ev = Ev(self.time + work[1], new_costumer[0].run, prio=1)
                self.push(ev)

            print(str(entry[0]) + "; " + str(entry[1]) + "; " + str(entry[2]) + ";:; " + str(work[0]) + "; " + str(work[1]) + "; " + str(station.name) + "; " + str(costumer.name))
            self.evCount += 1


# class consists of
# name: station name
# buffer: customer queue
# delay_per_item: service time
# CustomerWaiting, busy: possible states of this station
class Station:
    buffer = deque()
    is_busy = False
    current_waiting_time = 0

    def __init__(self, time_per_item, station_name):
        self.name = station_name
        self.delay_per_item = time_per_item

    def has_items_in_queue(self):
        return True if self.buffer else False

    def get_is_busy(self):
        return self.is_busy

    def set_is_busy(self, new_state):
        self.is_busy = new_state

    def get_waiting_time(self):
        return self.current_waiting_time

    def add_customer_to_queue(self, customer):
        self.buffer.append((customer, self.current_waiting_time))
        self.current_waiting_time += customer.run[1] * self.delay_per_item

    def remove_customer_from_queue(self):
        customer = self.buffer.popleft()
        self.current_waiting_time -= customer[0].run[1] * self.delay_per_item
        return customer


# class consists of
# statistics variables
# and methods as described in the problem description
class Customer:
    served = dict()
    dropped = dict()
    complete = 0
    duration = 0
    duration_cond_complete = 0
    count = 0
    possible_work = ["go_to_station", "buy_at_station", "wait", "jump"]

    def __init__(self, einkaufsliste, name, time):
        self.count += 1
        self.run = None
        self.work_list = deque()
        self.einkaufsliste = list(einkaufsliste)
        self.name = name
        self.time = time
        self.begin()
        self.set_next_work()

    def begin(self):
        for work in self.einkaufsliste:  # what to do, how long, station, customer
            self.work_list.append((self.possible_work[0], work[0], work[1], self))
            self.work_list.append((self.possible_work[1], work[2] * work[1].delay_per_item, work[1], self))

    def set_next_work(self) -> bool:
        if not self.work_list:
            self.complete += 1
            return False
        else:
            next_val = self.work_list.popleft()
            if self.run is not None:
                if next_val[0] == "buy_at_station" and next_val[2].get_is_busy():
                    next_val[2].add_customer_to_queue(next_val[3])
                    self.run = next_val
                    return False
                elif next_val[0] == "buy_at_station":
                    next_val[2].set_is_busy(True)

                if self.run[0] == "go_to_station":
                    self.run = next_val
                elif self.run[0] == "buy_at_station":
                    self.run[2].set_is_busy(False)
                    self.served[self.run[2].name] += 1
                    self.run = next_val
            else:
                self.run = next_val
            return True
# einkaufsliste1 = [(10, baecker, 10, 10), (30, metzger, 5, 10), (45, kaese, 3, 5), (60, kasse, 30, 20)] # timeT, obj, anzN, notsurpasstimeW


def reset():
    Customer.served['Bäcker'] = 0
    Customer.served['Metzger'] = 0
    Customer.served['Käse'] = 0
    Customer.served['Kasse'] = 0
    Customer.dropped['Bäcker'] = 0
    Customer.dropped['Metzger'] = 0
    Customer.dropped['Käse'] = 0
    Customer.dropped['Kasse'] = 0


def startCustomers(einkaufsliste, name, sT, dT, mT):
    i = 1
    t = sT
    while t < mT:
        kunde = Customer(list(einkaufsliste), name + str(i), t)
        ev = Ev(t, kunde.run, prio=1)
        evQ.push(ev)
        i += 1
        t += dT


evQ = EvQueue()
baecker = Station(10, 'Bäcker')
metzger = Station(30, 'Metzger')
kaese = Station(60, 'Käse')
kasse = Station(5, 'Kasse')

reset()

einkaufsliste1 = [(10, baecker, 10, 10), (30, metzger, 5, 10), (45, kaese, 3, 5), (60, kasse, 30, 20)]
einkaufsliste2 = [(30, metzger, 2, 5), (30, kasse, 3, 20), (20, baecker, 3, 20)] # timeT, obj, anzN, notsurpasstimeW
# startCustomers(einkaufsliste1, 'A', 0, 200, 30 * 60 + 1)
# startCustomers(einkaufsliste2, 'B', 1, 60, 30 * 60 + 1)

# Test
startCustomers(einkaufsliste1, 'A', 0, 200, 400)
startCustomers(einkaufsliste2, 'B', 1, 60, 400)

evQ.start()

my_print('Simulationsende: %is' % EvQueue.time)
my_print('Anzahl Kunden: %i' % Customer.count)
my_print('Anzahl vollständige Einkäufe %i' % Customer.complete)
x = Customer.duration / Customer.count
my_print(str('Mittlere Einkaufsdauer %.2fs' % x))
x = Customer.duration_cond_complete / Customer.complete
my_print('Mittlere Einkaufsdauer (vollständig): %.2fs' % x)
S = ('Bäcker', 'Metzger', 'Käse', 'Kasse')
for s in S:
    x = Customer.dropped[s] / (Customer.served[s] + Customer.dropped[s]) * 100
    my_print('Drop percentage at %s: %.2f' % (s, x))

f.close()
fc.close()
fs.close()
