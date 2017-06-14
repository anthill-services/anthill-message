from tornado.gen import coroutine, Return, sleep, Future, with_timeout, TimeoutError
from tornado.queues import Queue, QueueEmpty
from tornado.ioloop import IOLoop

from conversation import AccountConversation, MessageFlags

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

    """
    This model represents an incoming queue of all messages being sent and delivered over messaging system.

    Each message has this lifecycle:
    1. Some party decides to send some message to someone, for example, by calling 'add_message'
    2. That message goes into the incoming queue
    3. Then the message workers fetch the queue constantly for processing
    4. Upon processing, the message is tried to be delivered real time first, using recipient_class and recipient_key
        as the queue name (such recipient should be listening on it if he's online)
    5. Then the message may be stored depending on the flags of the message itself and whenever it was delivered

    Same cycle applies for updating and deleting the message

    """

    DELIVERY_TIMEOUT = 5
    PROCESS_TIMEOUT = 60

    def __init__(self, history):
        self.history = history

        self.connection = RabbitMQConnection(options.message_broker, connection_name="message.queue")
        self.channel = None
        self.exchange = None
        self.queue = None
        self.callback_queue = None
        self.handle_futures = {}

        self.outgoing_message_workers = options.outgoing_message_workers
        self.message_incoming_queue_name = options.message_incoming_queue_name
        self.message_prefetch_count = options.message_prefetch_count

        self.actions = {
            AccountConversation.ACTION_NEW_MESSAGE: self.__action_new_message__
        }

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
            action = message[AccountConversation.ACTION]
            gamespace_id = message[AccountConversation.GAMESPACE]
            sender = message[AccountConversation.SENDER]
            recipient_class = message[AccountConversation.RECIPIENT_CLASS]
            recipient_key = message[AccountConversation.RECIPIENT_KEY]
        except KeyError as e:
            raise MessagesQueueError("Missing field: " + e.args[0])

        # noinspection PyBroadException
        try:
            action_method = self.actions.get(action, self.__action_simple_deliver__)

            if action_method:
                yield action_method(gamespace_id, sender, recipient_class, recipient_key, message)

        except Exception:
            logging.exception("Failed to deliver message")

    def __action_simple_deliver__(self, gamespace_id, sender, recipient_class, recipient_key, message):
        try:
            message_uuid = message[AccountConversation.MESSAGE_UUID]
        except KeyError as e:
            raise MessagesQueueError("Missing field: " + e.args[0])

        return self.__deliver_message__(message_uuid, recipient_class, recipient_key, message)

    @coroutine
    def __action_new_message__(self, gamespace_id, sender, recipient_class, recipient_key, message):

        try:
            message_uuid = message[AccountConversation.MESSAGE_UUID]
            message_type = message[AccountConversation.TYPE]
            payload = message[AccountConversation.PAYLOAD]
        except KeyError as e:
            raise MessagesQueueError("Missing field: " + e.args[0])

        # noinspection PyBroadException
        try:
            delivered = yield self.__deliver_message__(
                message_uuid, recipient_class, recipient_key, message)

        except Exception:
            logging.exception("Failed to deliver message")
            return

        history = self.history

        flags = MessageFlags(message.get(AccountConversation.FLAGS, []))

        if delivered and (MessageFlags.REMOVE_DELIVERED in flags):
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

    @coroutine
    def __deliver_message__(self, message_uuid, recipient_class, recipient_key, message):

        if not isinstance(message, dict):
            raise MessageSendError("Payload message to be a dict")

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

            dumped = ujson.dumps(message)

            properties = BasicProperties(
                content_type='text/plain',
                reply_to=self.callback_queue.routing_key,
                correlation_id=message_uuid)

            yield channel.basic_publish(
                exchange_id,
                '',
                dumped,
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

            flags_ = message.get("flags", [])

            if flags_ and not isinstance(flags_, list):
                logging.error("A message '{0}' flags should be a list.".format(ujson.dumps(message)))
                continue

            flags = MessageFlags(flags_)

            message_uuid = str(uuid.uuid4())

            body = ujson.dumps({
                AccountConversation.ACTION: AccountConversation.ACTION_NEW_MESSAGE,
                AccountConversation.GAMESPACE: gamespace,
                AccountConversation.MESSAGE_UUID: message_uuid,
                AccountConversation.SENDER: sender,
                AccountConversation.RECIPIENT_CLASS: recipient_class,
                AccountConversation.RECIPIENT_KEY: recipient_key,
                AccountConversation.TYPE: message_type,
                AccountConversation.PAYLOAD: payload,
                AccountConversation.FLAGS: flags.as_list()
            })

            out_queue.put_nowait(body)

        workers_count = min(self.outgoing_message_workers, out_queue.qsize())

        for i in xrange(0, workers_count):
            IOLoop.current().add_callback(self.__outgoing_message_worker__, gamespace, sender, out_queue)

        yield out_queue.join(timeout=datetime.timedelta(seconds=MessagesQueueModel.PROCESS_TIMEOUT))

    @validate(gamespace="int", sender="int", recipient_class="str",
              recipient_key="str", message_type="str", payload="json_dict",
              flags=MessageFlags)
    def add_message(self, gamespace, sender, recipient_class, recipient_key, message_type, payload, flags):

        message_uuid = str(uuid.uuid4())

        message = {
            AccountConversation.ACTION: AccountConversation.ACTION_NEW_MESSAGE,
            AccountConversation.GAMESPACE: gamespace,
            AccountConversation.MESSAGE_UUID: message_uuid,
            AccountConversation.SENDER: sender,
            AccountConversation.RECIPIENT_CLASS: recipient_class,
            AccountConversation.RECIPIENT_KEY: recipient_key,
            AccountConversation.TYPE: message_type,
            AccountConversation.PAYLOAD: payload,
            AccountConversation.FLAGS: flags.as_list()
        }

        return self.__enqueue_message__(message)

    @validate(gamespace="int", sender="int", recipient_class="str",
              recipient_key="str", message_uuid="str")
    def delete_message(self, gamespace, sender, recipient_class, recipient_key, message_uuid):

        message = {
            AccountConversation.ACTION: AccountConversation.ACTION_MESSAGE_DELETED,
            AccountConversation.GAMESPACE: gamespace,
            AccountConversation.MESSAGE_UUID: message_uuid,
            AccountConversation.SENDER: sender,
            AccountConversation.RECIPIENT_CLASS: recipient_class,
            AccountConversation.RECIPIENT_KEY: recipient_key
        }

        return self.__enqueue_message__(message)

    @validate(gamespace="int", sender="int", recipient_class="str",
              recipient_key="str", message_uuid="str", payload="json_dict")
    def update_message(self, gamespace, sender, recipient_class, recipient_key, message_uuid, payload):

        message = {
            AccountConversation.ACTION: AccountConversation.ACTION_MESSAGE_UPDATED,
            AccountConversation.GAMESPACE: gamespace,
            AccountConversation.MESSAGE_UUID: message_uuid,
            AccountConversation.SENDER: sender,
            AccountConversation.RECIPIENT_CLASS: recipient_class,
            AccountConversation.RECIPIENT_KEY: recipient_key,
            AccountConversation.PAYLOAD: payload,
        }

        return self.__enqueue_message__(message)

    @coroutine
    @validate(message="json_dict")
    def __enqueue_message__(self, message):

        channel = yield self.connection.channel()

        properties = BasicProperties(
            delivery_mode=2,  # make message persistent
        )

        # noinspection PyBroadException
        try:
            body = ujson.dumps(message)

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
