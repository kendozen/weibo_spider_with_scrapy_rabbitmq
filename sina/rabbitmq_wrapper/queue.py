# system packages
import time
import pika
import logging
import re

# module packages
from source.sina.rabbitmq_wrapper import connection

logger = logging.getLogger(__name__)


class IQueue(object):
    """Per-spider queue/stack base class"""

    def __init__(self):
        """Init method"""
        raise NotImplementedError

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, url):
        """Push an url"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop an url"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        raise NotImplementedError


class RabbitMQQueue(IQueue):
    """Per-spider FIFO queue"""

    memory_queue = []
    from_local_queue = False

    def __init__(self, connection_url, exchange_name, queue_name, route_key, max_request):
        """Initialize per-spider RabbitMQ queue.

        Parameters:
            connection_url -- rabbitmq connection url
            key -- rabbitmq routing key
        """
        self.connection_url = connection_url
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.route_key = route_key
        self.max_request = max_request
        self.server = None
        self.connect()

    def __len__(self):
        """Return the length of the queue"""
        declared = self.channel.queue_declare(self.queue_name, passive=True)
        return declared.method.message_count

    def _try_operation(function):
        """Wrap unary method by reconnect procedure"""

        def wrapper(self, *args, **kwargs):
            retries = 0
            while retries < 10:
                try:
                    return function(self, *args, **kwargs)
                except Exception as e:
                    retries += 1
                    msg = 'Function %s failed. Reconnecting... (%d times)' % \
                          (str(function), retries)
                    logger.info(msg)
                    self.connect()
                    time.sleep((retries - 1) * 5)
            return None

        return wrapper

    @_try_operation
    def pop(self, no_ack=False):
        """Pop a message"""
        # TODO 如果本地队列中有 10个以上的url，则先处理本地请求
        if len(self.memory_queue) >= self.max_request:
            self.from_local_queue = True
        elif len(self.memory_queue) == 0:
            self.from_local_queue = False

        if self.from_local_queue:
            return None, None, self.memory_queue.pop()

        return self.channel.basic_get(queue=self.queue_name, no_ack=no_ack)

    @_try_operation
    def ack(self, delivery_tag):
        """Ack a message"""
        # 如果不为-1 表示是从rabbitmq来的消息
        if delivery_tag != -1:
            self.channel.basic_ack(delivery_tag=delivery_tag)

    @_try_operation
    def push(self, body, headers=None):
        """Push a message"""
        # TODO 本地新的请求URL都进本地队列中，如果是任务型的消息，重新入队列中取
        length = len(re.findall('{"uid":"(\d+)","id":(\d+),"type":(\d+)}', body))
        if length == 3:
            properties = None
            if headers:
                properties = pika.BasicProperties(headers=headers)
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.route_key,
                body=body,
                properties=properties
            )
        else:
            self.memory_queue.append(body)


    def connect(self):
        """Make a connection"""
        if self.server:
            try:
                self.server.close()
            except:
                pass
        self.server = connection.connect(self.connection_url)
        self.channel = connection.get_channel(self.server, self.queue_name)

    def close(self):
        """Close channel"""
        self.channel.close()

    def clear(self):
        """Clear queue/stack"""
        self.channel.queue_purge(self.queue_name)
