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
def my_print1(k, station_name, msg):
    t = EvQueue.time
    print(str(round(t, 4)) + ':' + k + ' ' + msg + ' at ' + station_name)
    fc.write(str(round(t, 4)) + ':' + k + ' ' + msg + ' at ' + station_name + '\n')


# print on console and into station log
# s: station_name
# name: customer name
def my_print2(station_name, msg, name):
    t = EvQueue.time
    print(str(round(t, 4)) + ':' + station_name + ' ' + msg)
    fs.write(str(round(t, 4)) + ':' + station_name + ' ' + msg + ' ' + name + '\n')


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
            EvQueue.evCount += 1
            EvQueue.time = entry[0]
            work = entry[3]
            station = work[2]
            customer = work[3]

            ev_list = customer.work()

            if ev_list is not None:
                for ev in ev_list:
                    self.push(ev)

            print(str(entry[0]) + "; " + str(entry[1]) + "; " + str(entry[2]) + ";:; " + str(work[0]) + "; " +
                  str(work[1]) + "; " + str(station.name) + "; " + str(customer.name))


# class consists of
# name: station name
# buffer: customer queue
# delay_per_item: service time
# CustomerWaiting, busy: possible states of this station
class Station:

    def __init__(self, time_per_item, station_name):
        self.name = station_name
        self.delay_per_item = time_per_item
        self.buffer = deque()
        self.state = 'idle'  # idle, busy
        self.current_waiting_time = 0
        self.customer_start_time = 0

    def has_items_in_queue(self):
        if self.buffer:
            return True
        else:
            return False

    def add_to_current_waiting_time(self, val):
        self.current_waiting_time = self.current_waiting_time + val
        # print(self.name + " add " + str(val) + " Now: " + str(self.current_waiting_time))

    def remove_from_current_waiting_time(self, val):
        self.current_waiting_time = self.current_waiting_time - val
        # print(self.name + " remove " + str(val) + " Now: " + str(self.current_waiting_time))

    def add_customer_to_queue(self, customer):
        self.buffer.append(customer)
        self.add_to_current_waiting_time(customer.current_time_needed)

    def remove_customer_from_queue(self):
        customer = self.buffer.popleft()
        return customer

    def put_in_queue(self, customer):
        if self.state == 'busy' or self.has_items_in_queue():
            if Customer.Simulation_with_drop and \
                    self.current_waiting_time - (EvQueue.time - self.customer_start_time) > customer.current_max_time:
                customer.set_next_work()
                customer.last_station_time_needed = 0
                ev = Ev(EvQueue.time, customer.run, prio=1)
                Customer.dropped[self.name] += 1
                customer.jumped_station = True
                return ev
            self.add_customer_to_queue(customer)
            return None
        else:
            self.state = 'busy'
            self.customer_start_time = EvQueue.time
            self.add_to_current_waiting_time(customer.current_time_needed)
            time_for_this_event = customer.current_time_needed
            customer.set_next_work()
            ev = Ev(EvQueue.time + time_for_this_event, customer.run, prio=1)
            return ev

    def finished(self, customer):
        Customer.served[self.name] += 1
        self.remove_from_current_waiting_time(customer.last_station_time_needed)
        if self.has_items_in_queue():
            cust = self.remove_customer_from_queue()
            self.customer_start_time = EvQueue.time
            time_for_this_event = cust.current_time_needed
            cust.set_next_work()
            ev = Ev(EvQueue.time + time_for_this_event, cust.run, prio=1)
            return ev
        else:
            self.state = 'idle'
            return None


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
    possible_work = ["leave_station", "arrive_at_station", "begin", "exit"]
    Simulation_with_drop = True

    def __init__(self, einkaufsliste, name, time):
        Customer.count += 1
        self.run = None
        self.current_objective = None
        self.current_time_needed = None
        self.current_station = None
        self.current_max_time = None
        self.last_station_time_needed = None
        self.work_list = deque()
        self.einkaufsliste = list(einkaufsliste)
        self.name = name
        self.time = time
        self.jumped_station = False
        self.begin()

    def begin(self):
        prev_station = None
        for work in self.einkaufsliste:  # what to do, how long, station, customer, max_wait_time
            if prev_station is None:
                self.work_list.append((Customer.possible_work[2], work[0], work[1], self, 0))
                self.work_list.append((Customer.possible_work[1], work[1].delay_per_item * work[2],
                                       work[1], self, work[3]))
            else:
                self.work_list.append((Customer.possible_work[0], work[0], prev_station, self, 0))
                self.work_list.append((Customer.possible_work[1], work[1].delay_per_item * work[2],
                                       work[1], self, work[3]))
            prev_station = work[1]

        self.run = self.work_list.popleft()
        self.current_objective = self.run[0]
        self.current_time_needed = self.run[1]
        self.current_station = self.run[2]
        self.current_max_time = self.run[4]

    def set_next_work(self):
        if self.current_time_needed is not None:
            self.last_station_time_needed = self.current_time_needed
        if not self.work_list:
            self.run = (Customer.possible_work[3], 0, self.run[2], self.run[3], 0)
        else:
            self.run = self.work_list.popleft()
        self.current_objective = self.run[0]
        self.current_time_needed = self.run[1]
        self.current_station = self.run[2]
        self.current_max_time = self.run[4]

    def work(self):

        event_list = []

        if self.current_objective == Customer.possible_work[3]:
            ev = self.current_station.finished(self)
            if ev is not None:
                event_list.append(ev)
            Customer.duration += EvQueue.time - self.time
            if not self.jumped_station:
                Customer.duration_cond_complete += EvQueue.time - self.time
                Customer.complete += 1
        elif self.current_objective == Customer.possible_work[1]:
            ev = self.current_station.put_in_queue(self)
            if ev is not None:
                event_list.append(ev)
        elif self.current_objective == Customer.possible_work[0] or self.current_objective == Customer.possible_work[2]:
            if not self.current_objective == Customer.possible_work[2]:
                ev = self.current_station.finished(self)
                if ev is not None:
                    event_list.append(ev)
            time_for_this_ev = self.current_time_needed
            self.set_next_work()
            next_ev = Ev(EvQueue.time + time_for_this_ev, self.run, prio=1)
            event_list.append(next_ev)

        return event_list


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
einkaufsliste2 = [(30, metzger, 2, 5), (30, kasse, 3, 20), (20, baecker, 3, 20)]  # timeT, obj, anzN, notsurpasstimeW
startCustomers(einkaufsliste1, 'A', 0, 200, 30 * 60 + 1)
startCustomers(einkaufsliste2, 'B', 1, 60, 30 * 60 + 1)

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
