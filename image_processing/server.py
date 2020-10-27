import os
import time
import boto3
import logging
from multiprocessing import Process, Queue
from botocore.exceptions import ClientError
import statistics


# time interval for checking new message in queue, in seconds
CHECK_INTERVAL = 0.5

# maxium of running processes
MAXIUM_PROCESS = 8


def worker(input, output):
    args = input.get()
    result = Result(data=calculate_statistcs(args) ,success=True, pid=os.get_pid())
    output.put(result)


def calculate_statistcs(msg):
    if not msg.body.replace(',','').isdecimal():
        return False, None
    L = msg.body.split(',')
    Li = [int(s) for s in L if not s == '']
    return True, [min(Li), max(Li), statistics.mean(Li), statistics.median(Li)]



if __name__ == '__main__':
    sqs = boto3.resource('sqs')
    processes_pid = set()
    
    task_queue = Queue()
    done_queue = Queue()

    try:
        aws_queue = sqs.get_queue_by_name(QueueName='test')
    except ClientError as e:
        # pseudo
        if "QueueNotExist" in str(e):
            # pseudo
            sqs.create_queue("test")
        else:
            raise e
    
    while True:
        # not sure if once receive_messages is called all the messages would be dequed or not
        # currently assume that they would
        messages = aws_queue.receive_messages(MessageAttributeNames=['Key'])
        if len(messages) >= 1:
            for msg in messages:
                task_queue.put(msg)
                msg.delete()
        for i in range(min(MAXIUM_PROCESS, task_queue.qsize())):
            p = Process(target=worker, args=(task_queue, done_queue))
            p.start()
        while done_queue.qsize() > 0:
            result = done_queue.get()
            # Pseudo
            upload_result_to_aws_sqs()
            create_log_file() # using logging module or just basic IO
            upload_log_file_to_s3()

        time.sleep(CHECK_INTERVAL)
            

class Result:
    def __init__(self, data, success, pid):
        if not isinstance(success, bool) :
            raise TypeError("`success` must be boolean type.")
        self.success = success
        try:
            self.pid = int(pid)
        except ValueError:
            raise TypeError("`pid` must int or valid string.")
        self.data = data