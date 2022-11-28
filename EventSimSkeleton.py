from collections import deque
import heapq
import threading
import time
from datetime import datetime

f = open("supermarkt.txt", "w")
fc = open("supermarkt_customer.txt", "w")
fs = open("supermarkt_station.txt", "w")


# print on console and into supermarket log
def my_print(msg):
    print(msg)
    f.write(msg + '\n')


# class consists of
# name: station name
# buffer: customer queue
# delay_per_item: service time
# CustomerWaiting, busy: possible states of this station
class Station(threading.Thread):

    def __init__(self, time_per_item, station_name):
        threading.Thread.__init__(self)
        self.name = station_name
        self.delay_per_item = time_per_item
        self.buffer = deque()
        self.state = 'idle'  # idle, busy
        self.current_waiting_time = 0
        self.customer_start_time = 0
        self.customer_waiting_event = threading.Event()

    def run(self):
        self.wait_for_customer()

    def wait_for_customer(self):
        while True:
            self.customer_waiting_event.clear()
            print(self.name + " now waiting")
            self.customer_waiting_event.wait()
            print(self.name + " now serving")
            self.serve_customer()

    def customer_arrived(self, customer):
        print(customer.name + " arrived at " + self.name)
        serv_event = threading.Event()
        self.add_customer_to_queue(customer, serv_event)
        self.customer_waiting_event.set()
        serv_event.wait()

    def serve_customer(self):
        while self.has_items_in_queue():
            customer, serv_event = self.remove_customer_from_queue()
            print(self.name + " serve for " + str(customer.current_time_needed) + "s")
            time.sleep(customer.current_time_needed * Customer.fast_Simulation)
            self.remove_from_current_waiting_time(customer.current_time_needed)
            serv_event.set()

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

    def add_customer_to_queue(self, customer, serv_event):
        self.buffer.append((customer, serv_event))
        self.add_to_current_waiting_time(customer.current_time_needed)

    def remove_customer_from_queue(self):
        customer, serv_event = self.buffer.popleft()
        return customer, serv_event

    # def put_in_queue(self, customer):
    #     if self.state == 'busy' or self.has_items_in_queue():
    #         if Customer.Simulation_with_drop and \
    #                 self.current_waiting_time - (EvQueue.time - self.customer_start_time) > customer.current_max_time:
    #             customer.set_next_work()
    #             customer.last_station_time_needed = 0
    #             ev = Ev(EvQueue.time, customer.crun, prio=1)
    #             Customer.dropped[self.name] += 1
    #             customer.jumped_station = True
    #             return ev
    #         self.add_customer_to_queue(customer)
    #         return None
    #     else:
    #         self.state = 'busy'
    #         self.customer_start_time = EvQueue.time
    #         self.add_to_current_waiting_time(customer.current_time_needed)
    #         time_for_this_event = customer.current_time_needed
    #         customer.set_next_work()
    #         ev = Ev(EvQueue.time + time_for_this_event, customer.crun, prio=1)
    #         return ev
    #
    # def finished(self, customer):
    #     Customer.served[self.name] += 1
    #     self.remove_from_current_waiting_time(customer.last_station_time_needed)
    #     if self.has_items_in_queue():
    #         cust = self.remove_customer_from_queue()
    #         self.customer_start_time = EvQueue.time
    #         time_for_this_event = cust.current_time_needed
    #         cust.set_next_work()
    #         ev = Ev(EvQueue.time + time_for_this_event, cust.crun, prio=1)
    #         return ev
    #     else:
    #         self.state = 'idle'
    #         return None


# class consists of
# statistics variables
# and methods as described in the problem description
class Customer(threading.Thread):
    served_lock = threading.Lock()
    served = dict()
    dropped_lock = threading.Lock()
    dropped = dict()
    complete_lock = threading.Lock()
    complete = 0
    duration_lock = threading.Lock()
    duration = 0
    duration_cond_lock = threading.Lock()
    duration_cond_complete = 0
    count_lock = threading.Lock()
    count = 0
    possible_work = ["leave_station", "arrive_at_station", "begin", "exit"]
    Simulation_with_drop = True
    fast_Simulation = 0.001

    def __init__(self, einkaufsliste, name):
        threading.Thread.__init__(self)
        Customer.count_lock.acquire()
        Customer.count += 1
        Customer.count_lock.release()
        self.crun = None
        self.current_objective = None
        self.current_time_needed = None
        self.current_station = None
        self.current_max_time = None
        self.last_station_time_needed = None
        self.work_list = deque()
        self.einkaufsliste = list(einkaufsliste)
        self.name = name
        self.time = datetime.now()
        self.jumped_station = False

    def run(self):
        self.begin()
        self.work()

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

        self.crun = self.work_list.popleft()
        self.current_objective = self.crun[0]
        self.current_time_needed = self.crun[1]
        self.current_station = self.crun[2]
        self.current_max_time = self.crun[4]

    def set_next_work(self):
        if self.current_time_needed is not None:
            self.last_station_time_needed = self.current_time_needed
        if not self.work_list:
            self.crun = (Customer.possible_work[3], 0, self.crun[2], self.crun[3], 0)
        else:
            self.crun = self.work_list.popleft()
        self.current_objective = self.crun[0]
        self.current_time_needed = self.crun[1]
        self.current_station = self.crun[2]
        self.current_max_time = self.crun[4]

    def work(self):

        if self.current_objective == Customer.possible_work[3]:  # exit
            now = datetime.now()
            complete_time = (now.hour - self.time.hour) * 60 * 60 + (now.minute - self.time.minute) * 60 + \
                            (now.second - self.time.second)
            print(complete_time)
            Customer.duration_lock.acquire()
            Customer.duration += complete_time
            Customer.duration_lock.release()
            if not self.jumped_station:
                Customer.duration_cond_lock.acquire()
                Customer.duration_cond_complete += complete_time
                Customer.duration_cond_lock.release()
                Customer.complete_lock.acquire()
                Customer.complete += 1
                Customer.complete_lock.release()
            print(self.name + " finished")
            return

        elif self.current_objective == Customer.possible_work[1]:  # arrive_at_station
            self.current_station.customer_arrived(self)

        elif self.current_objective == Customer.possible_work[0] or self.current_objective == Customer.possible_work[2]:
            # leave_station or begin
            time_for_this_ev = self.current_time_needed
            print(self.name + " sleeps for " + str(time_for_this_ev) + "s to get to new station")
            time.sleep(time_for_this_ev * Customer.fast_Simulation)

        self.set_next_work()
        self.work()


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
    time.sleep(sT * Customer.fast_Simulation)
    i = 1
    t = sT
    kunde_liste = []
    while t < mT:
        kunde = Customer(list(einkaufsliste), name + str(i))
        print(kunde.name + " start")
        kunde.start()
        kunde_liste.append(kunde)
        time.sleep(dT * Customer.fast_Simulation)
        # ev = Ev(t, kunde.crun, prio=1)
        # evQ.push(ev)
        i += 1
        t += dT
    for k in kunde_liste:
        k.join()
    Customer.end_time = t


# evQ = EvQueue()
baecker = Station(10, 'Bäcker')
metzger = Station(30, 'Metzger')
kaese = Station(60, 'Käse')
kasse = Station(5, 'Kasse')

# start threads
baecker.start()
metzger.start()
kaese.start()
kasse.start()

reset()

start_time = datetime.now()

einkaufsliste1 = [(10, baecker, 10, 10), (30, metzger, 5, 10), (45, kaese, 3, 5), (60, kasse, 30, 20)]
einkaufsliste2 = [(30, metzger, 2, 5), (30, kasse, 3, 20), (20, baecker, 3, 20)]  # timeT, obj, anzN, notsurpasstimeW
# startCustomers(einkaufsliste1, 'A', 0, 200, 30 * 60 + 1)
# startCustomers(einkaufsliste2, 'B', 1, 60, 30 * 60 + 1)
t1 = threading.Thread(target=startCustomers, args=(einkaufsliste1, 'A', 0, 200, 60), daemon=True)
t2 = threading.Thread(target=startCustomers, args=(einkaufsliste2, 'B', 1, 60, 60), daemon=True)

t1.start()
t2.start()

t1.join()
t2.join()

# evQ.start()

end_time = datetime.now()

my_print('Simulationsende: ' + str(end_time.hour - start_time.hour) + "hours " + \
         str(end_time.minute - start_time.minute) \
         + "minutes " + str(end_time.second - start_time.second) + "seconds")
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
