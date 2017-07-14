from kafka import KafkaConsumer
import threading, logging,time, signal
import Queue
from datetime import datetime
import kafka.errors as Errors



logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s', datefmt='%Y-%m-%d,%H:%M:%S',
                    level=logging.INFO)
kafka_logger = logging.getLogger('kafka')
kafka_logger.setLevel(logging.ERROR)

class kafkaConsumerThread(threading.Thread) :
    def __init__(self, queue =None, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None, consumer= None):
        super(kafkaConsumerThread, self).__init__()
        self.target = target
        self.name = name
        self.queue = queue
        self.shutdown_flag = threading.Event()
        self.consumer = consumer


    # def run(self):
    #     self.consumer.subscribe(['Events'])
    #     while not self.shutdown_flag.is_set():
    #         message = self.consumer.poll(max_records =1)
    #         if any(message):
    #             for key, records in message.items():
    #                 for record in records :
    #                     # print("{}".format(record.value))
    #                     self.queue.put(record.value)
    #         # for message in consumer:
    #         #     print("message from kafka :{}".format(message.value))
    #         #     self.queue.put(message.value)
    #         #     if self.shutdown_flag.is_set():
    #         #         print("kill signal caputured")
    #         #         consumer.close()
    #         #         break
    #     print("shutting down kafka consumer thread")
    #     logging.info("shutting down kafka consumer thread")

    def run(self):
        self.consumer.subscribe(['Events'])
        for message in self.consumer :
            self.queue.put(message.value)
            if self.shutdown_flag.is_set():
                break;
        print("shutting down kafka consumer thread")
        logging.info("shutting down kafka consumer thread")

class queueConsumerThread(threading.Thread) :
    def __init__( self, queue=None, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None, max_record=None):
        super(queueConsumerThread, self).__init__()
        self.target = target
        self.name = name
        self.queue = queue
        self.shutdown_flag = threading.Event()
        self.write_to_newfile_flag = threading.Event()
        self.max_record = max_record

    def run(self):
        file_name_extent = time.time()
        today = datetime.now().strftime('%Y-%m-%d')
        file_name = "logs/Events_{}_{}.txt".format(today,file_name_extent)
        logging.info('new file {} is created'.format(file_name))
        print('new file {} is created'.format(file_name))
        f = open(file_name, 'w')
        count = 0
        while not self.shutdown_flag.is_set():
            while not self.write_to_newfile_flag.is_set():
                if not self.queue.empty():
                    record = self.queue.get()
                    self.queue.task_done()
                    if not self.write_to_newfile_flag.is_set() :
                        # print("message from queue:{}".format(record))
                        time.sleep(1)
                        if count < self.max_record :
                            count= count +1
                        else :
                            self.write_to_newfile_flag.set()
                            count = 0
                    f.write(record)
                    f.write('\n')
            # in the case, thread is shut down from the main(), there are still some records in the queue, so flush them to the file
            # before close it
            if self.shutdown_flag.is_set() :
                logging.info("if there are some records left in the queue, just flush them to the file before close it")
                while not self.queue.empty():
                    record = self.queue.get()
                    f.write(record)
                    f.write('\n')
            f.close()
            self.write_to_newfile_flag.clear()
            print("close file {}".format(f.name))
            logging.info("close file {}".format(f.name))
            #in the case, max records is reached or the time intervel is reached, then close the current file and create a new one
            if not self.shutdown_flag.is_set() :
                file_name_extent = time.time()
                today = datetime.now().strftime('%Y-%m-%d')
                file_name = "logs/Events_{}_{}.txt".format(today, file_name_extent)
                f = open(file_name, 'w')
                print("new log file {} is created".format(file_name))
                logging.info("new log file {} is created".format(file_name))
        print("clean queue consumer")
        logging.info("clean queue consumer")
        if not f.closed :
            f.close()
            print("close file {}".format(f.name))
            logging.info("close file {}".format(f.name))
        return


class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass

def service_shutdown(signum, frame):
    print('Caught signal %d' % signum)
    logging.info('Caught signal %d' % signum)
    raise ServiceExit

def main():
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)
    QUEUE_SIZE = 1000
    WRITE_FILE_INTERVAL = 60*60*2 # every 2 hours write to a log file
    q = Queue.Queue(QUEUE_SIZE)

    # consumer = KafkaConsumer(bootstrap_servers=['10.1.10.62:9092', '10.1.10.63:9092', '10.1.10.64:9092'],group_id='my-topic',auto_offset_reset='earliest')

    try :
        consumer = KafkaConsumer(bootstrap_servers=['10.6.3.228:9092', '10.6.3.221:9092', '10.6.3.51:9092'],
                                 group_id='event_archiver_test', auto_offset_reset='earliest', max_poll_records=20,
                                 request_timeout_ms=40000, session_timeout_ms=30000, consumer_timeout_ms=10000)
        start = time.time()

        p = kafkaConsumerThread(queue=q,name='producer',consumer= consumer)
        # every 500k record to write to a new log file
        c = queueConsumerThread(queue=q, max_record=500*1000, name='consumer')
        p.start()
        time.sleep(1)
        c.start()
        time.sleep(1)
        while True:
            time.sleep(1)
            elapsed = time.time() - start
            if elapsed > WRITE_FILE_INTERVAL :
                print("elapsed {} second".format(elapsed))
                logging.info("elapsed {} second".format(elapsed))
                c.write_to_newfile_flag.set()
                start = time.time()
    except ServiceExit :
        p.shutdown_flag.set()
        c.write_to_newfile_flag.set()
        c.shutdown_flag.set()
        p.join()
        c.join()
        consumer.close()
    except Errors.NoBrokersAvailable:
        logging.info("cannot connect to brokers")
        print("cannot connect to brokers")

    print('Exiting main program')
    logging.info('Exiting main program')
if __name__ == '__main__':
    main()
