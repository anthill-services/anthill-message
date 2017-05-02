from tornado.gen import coroutine, Return, sleep, Future, with_timeout, TimeoutError
from tornado.queues import Queue, QueueEmpty
from tornado.ioloop import IOLoop

from conversation import AccountConversation, DeliveryFlags

from common.model import Model
from common.rabbitconn import RabbitMQConnection
from common.options import options
from common.validate import validate

from . import MessageSendError

import logging
import ujson
import uuid
import datetime

from pika import BasicProperties


class MessagesQueueError(Exception):
    def __init__(self, message):
        self.message = message


class MessagesQueueModel(Model):

    DELIVERY_TIMEOUT = 5
    PROCESS_TIMEOUT = 60

    def __init__(self, history):
        self.history = history

        self.connection = RabbitMQConnection(options.message_broker)
        self.channel = None
        self.exchange = None
        self.queue = None
        self.callback_queue = None
        self.handle_futures = {}

        self.outgoing_message_workers = options.outgoing_message_workers
        self.message_incoming_queue_name = options.message_incoming_queue_name
        self.message_prefetch_count = options.message_prefetch_count

    # noinspection PyBroadException
    @coroutine
    def started(self):

        try:
            self.channel = yield self.connection.channel()

            yield self.channel.basic_qos(prefetch_count=self.message_prefetch_count)

            self.queue = yield self.channel.queue(queue=self.message_incoming_queue_name, durable=True)
            yield self.queue.consume(self.__on_message__)

            self.callback_queue = yield self.channel.queue(exclusive=True)
            yield self.callback_queue.consume(self.__on_callback__, no_ack=True)

        except Exception:
            logging.exception("Failed to start message consuming queue")
        else:
            logging.info("Started message consuming queue")

    @coroutine
    def stopped(self):
        logging.info("Releasing message consuming queue")

        if self.queue:
            yield self.queue.delete()

        if self.channel:
            # noinspection PyBroadException
            try:
                yield self.channel.close()
            except:
                pass

        self.connection = None

        self.exchange = None
        self.queue = None

    @coroutine
    def __on_message__(self, channel, method, properties, body):
        try:
            yield self.__process__(channel, method, properties, body)
        except MessagesQueueError as e:
            logging.error("Failed to process incoming message: " + e.message)

        channel.basic_ack(delivery_tag=method.delivery_tag)

    @coroutine
    def __on_callback__(self, channel, method, properties, body):

        message_uuid = properties.correlation_id
        delivered = body == 'true'

        try:
            f = self.handle_futures.pop(message_uuid)
        except KeyError:
            pass
        else:
            f.set_result(delivered)

    @coroutine
    def __process__(self, channel, method, properties, body):
        try:
            message = ujson.loads(body)
        except (KeyError, ValueError):
            raise MessagesQueueError("Corrupted body")

        try:
            gamespace_id = message[AccountConversation.GAMESPACE]
            sender = message[AccountConversation.SENDER]
            recipient_class = message[AccountConversation.RECIPIENT_CLASS]
            recipient_key = message[AccountConversation.RECIPIENT_KEY]
            message_type = message[AccountConversation.TYPE]
            payload = message[AccountConversation.PAYLOAD]
        except KeyError as e:
            raise MessagesQueueError("Missing field: " + e.args[0])

        flags = message.get(AccountConversation.FLAGS, [])

        # noinspection PyBroadException
        try:
            yield self.__deliver_message__(
                gamespace_id, sender, recipient_class,
                recipient_key, message_type, payload, flags)

        except Exception:
            logging.exception("Failed to deliver message")

    @coroutine
    def __deliver_message__(self, gamespace_id, sender, recipient_class,
                            recipient_key, message_type, payload, flags):
        """
        Tries to deliver a message to recipient (recipient_class, recipient_key)
        :param gamespace_id: gamespace of the message
        :param sender: account id of the sender
        :param recipient_class: a class of recipient (user, group, etc)
        :param recipient_key: a key in such class (account id, group id etc)
        :param payload: a dict containing the message to be delivered
        :param message_type: a kind of a message (like a subject)
        :param flags: message flags

        :returns whenever message was delivered, or not (in that case it's stored in a database)
        """

        if not isinstance(payload, dict):
            raise MessageSendError("Payload expected to be a dict")

        if flags:
            flags = DeliveryFlags.parse(flags)
        else:
            flags = []

        message_uuid = str(uuid.uuid4())

        body = ujson.dumps({
            AccountConversation.GAMESPACE: gamespace_id,
            AccountConversation.MESSAGE_UUID: message_uuid,
            AccountConversation.SENDER: sender,
            AccountConversation.RECIPIENT_CLASS: recipient_class,
            AccountConversation.RECIPIENT_KEY: recipient_key,
            AccountConversation.TYPE: message_type,
            AccountConversation.PAYLOAD: payload,
            AccountConversation.FLAGS: flags
        })

        exchange_id = AccountConversation.__id__(recipient_class, recipient_key)

        channel = yield self.connection.channel()

        try:
            f = Future()

            def cancel_handle():
                try:
                    del self.handle_futures[message_uuid]
                except KeyError:
                    pass

            def delivered_(m):
                import pika
                if f.running():
                    if not isinstance(m.method, pika.spec.Basic.Ack):
                        cancel_handle()
                        f.set_result(False)

            def closed(ch, reason, param):
                if f.running():
                    cancel_handle()
                    f.set_result(False)

            # add the future to the handles in case callback_queue will bring something
            self.handle_futures[message_uuid] = f

            yield channel.confirm_delivery(delivered_)
            yield channel.add_on_close_callback(closed)

            from pika import BasicProperties

            properties = BasicProperties(
                content_type='text/plain',
                reply_to=self.callback_queue.routing_key,
                correlation_id=message_uuid)

            yield channel.basic_publish(
                exchange_id,
                '',
                body,
                properties=properties,
                mandatory=True)

            try:
                delivered = yield with_timeout(
                    timeout=datetime.timedelta(seconds=MessagesQueueModel.DELIVERY_TIMEOUT),
                    future=f)
            except TimeoutError:
                cancel_handle()
                delivered = False
        finally:
            if channel.is_open:
                yield channel.close()

        logging.debug("Message '{0}' {1} been delivered.".format(message_uuid, "has" if delivered else "has not"))

        history = self.history

        if delivered and (DeliveryFlags.REMOVE_DELIVERED in flags):
            raise Return(delivered)

        yield history.add_message(
            gamespace_id,
            sender,
            message_uuid,
            str(recipient_class),
            str(recipient_key),
            datetime.datetime.utcnow(),
            message_type,
            payload,
            flags,
            delivered=delivered)

        raise Return(delivered)

    # noinspection PyBroadException
    @coroutine
    def __outgoing_message_worker__(self, gamespace, sender, queue):

        channel = yield self.connection.channel()

        properties = BasicProperties(
            delivery_mode=2,
        )

        class Wrapper(object):
            future = None

        wrapped = Wrapper()

        def delivered_(m):
            import pika
            f = wrapped.future
            if not f:
                return
            if f.running():
                f.set_result(isinstance(m.method, pika.spec.Basic.Ack))

        def closed(ch, reason, param):
            f = wrapped.future
            if not f:
                return
            if f.running():
                f.set_result(False)

        yield channel.confirm_delivery(delivered_)
        yield channel.add_on_close_callback(closed)

        try:
            while True:
                wrapped.future = Future()

                try:
                    body = queue.get_nowait()
                except QueueEmpty:
                    raise Return(True)

                try:
                    yield channel.basic_publish(
                        '',
                        self.message_incoming_queue_name,
                        body,
                        mandatory=True,
                        properties=properties)
                except Exception:
                    logging.exception("Failed to public message.")

                success = yield wrapped.future
                wrapped.future = None
                queue.task_done()

                if not success:
                    raise Return(False)
        finally:
            yield channel.close()

    @coroutine
    @validate(gamespace="int", sender="int", messages="json_list")
    def add_messages(self, gamespace, sender, messages):

        out_queue = Queue()

        for message in messages:

            try:
                recipient_class = message["recipient_class"]
                recipient_key = message["recipient_key"]
                message_type = message["message_type"]
                payload = message["payload"]
            except (KeyError, ValueError):
                logging.error("A message '{0}' skipped since missing fields.".format(ujson.dumps(message)))
                continue

            flags = message.get("flags")

            if flags and not isinstance(flags, list):
                logging.error("A message '{0}' flags should be a list.".format(ujson.dumps(message)))
                continue

            message_uuid = str(uuid.uuid4())

            body = ujson.dumps({
                AccountConversation.GAMESPACE: gamespace,
                AccountConversation.MESSAGE_UUID: message_uuid,
                AccountConversation.SENDER: sender,
                AccountConversation.RECIPIENT_CLASS: recipient_class,
                AccountConversation.RECIPIENT_KEY: recipient_key,
                AccountConversation.TYPE: message_type,
                AccountConversation.PAYLOAD: payload,
                AccountConversation.FLAGS: flags
            })

            out_queue.put_nowait(body)

        workers_count = min(self.outgoing_message_workers, out_queue.qsize())

        for i in xrange(0, workers_count):
            IOLoop.current().add_callback(self.__outgoing_message_worker__, gamespace, sender, out_queue)

        yield out_queue.join(timeout=datetime.timedelta(seconds=MessagesQueueModel.PROCESS_TIMEOUT))

    @coroutine
    @validate(gamespace="int", sender="int", recipient_class="str",
              recipient_key="str", message_type="str", payload="json",
              flags="json_list_of_strings")
    def add_message(self, gamespace, sender, recipient_class, recipient_key, message_type, payload, flags):

        channel = yield self.connection.channel()

        properties = BasicProperties(
            delivery_mode=2,  # make message persistent
        )

        # noinspection PyBroadException
        try:
            message_uuid = str(uuid.uuid4())

            body = ujson.dumps({
                AccountConversation.GAMESPACE: gamespace,
                AccountConversation.MESSAGE_UUID: message_uuid,
                AccountConversation.SENDER: sender,
                AccountConversation.RECIPIENT_CLASS: recipient_class,
                AccountConversation.RECIPIENT_KEY: recipient_key,
                AccountConversation.TYPE: message_type,
                AccountConversation.PAYLOAD: payload,
                AccountConversation.FLAGS: flags
            })

            f = Future()

            def delivered_(m):
                import pika
                if f.running():
                    f.set_result(isinstance(m.method, pika.spec.Basic.Ack))

            def closed(ch, reason, param):
                if f.running():
                    f.set_result(False)

            yield channel.confirm_delivery(delivered_)
            yield channel.add_on_close_callback(closed)

            yield channel.basic_publish(
                '',
                self.message_incoming_queue_name,
                body,
                mandatory=True,
                properties=properties)

            result = yield f
        except Exception as e:
            logging.exception("Failed to public message.")
            result = False
        finally:
            yield channel.close()

        raise Return(result)
