
from tornado.gen import Future, with_timeout, TimeoutError
from tornado.queues import Queue, QueueEmpty
from tornado.ioloop import IOLoop

from anthill.common.model import Model
from anthill.common.rabbitconn import RabbitMQConnection
from anthill.common.options import options
from anthill.common.validate import validate
from anthill.common.access import utc_time

from . import MessageSendError
from . conversation import AccountConversation, MessageFlags

import logging
import ujson
import uuid
import datetime
import pytz
import pika

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
    async def started(self, application):

        try:
            self.channel = await self.connection.channel()

            await self.channel.basic_qos(prefetch_count=self.message_prefetch_count)

            self.queue = await self.channel.queue(queue=self.message_incoming_queue_name, durable=True)
            self.callback_queue = await self.channel.queue(exclusive=True)

            await self.queue.consume(self.__on_message__)
            await self.callback_queue.consume(self.__on_callback__, no_ack=True)

        except Exception:
            logging.exception("Failed to start message consuming queue")
        else:
            logging.info("Started message consuming queue")

    async def stopped(self):
        logging.info("Releasing message consuming queue")

        if self.queue:
            await self.queue.delete()

        if self.channel:
            # noinspection PyBroadException
            try:
                self.channel.close()
            except:
                pass

        self.connection = None

        self.exchange = None
        self.queue = None

    def __on_message__(self, channel, method, properties, body):
        try:
            self.__process__(channel, method, properties, body)
        except MessagesQueueError as e:
            logging.error("Failed to process incoming message: " + e.message)

        channel.basic_ack(delivery_tag=method.delivery_tag)

    def __on_callback__(self, channel, method, properties, body):

        message_uuid = properties.correlation_id
        delivered = body == b'true'

        try:
            f = self.handle_futures.pop(message_uuid)
        except KeyError:
            pass
        else:
            f.set_result(delivered)

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

        action_method = self.actions.get(action, self.__action_simple_deliver__)

        if action_method:
            IOLoop.current().spawn_callback(
                action_method, gamespace_id, sender,
                recipient_class, recipient_key, message)

    def __action_simple_deliver__(self, gamespace_id, sender, recipient_class, recipient_key, message):
        try:
            message_uuid = message[AccountConversation.MESSAGE_UUID]
            message_type = message[AccountConversation.TYPE]
        except KeyError as e:
            raise MessagesQueueError("Missing field: " + e.args[0])

        return self.__deliver_message__(message_uuid, message_type, recipient_class, recipient_key, message)

    async def __action_new_message__(self, gamespace_id, sender, recipient_class, recipient_key, message):

        try:
            message_uuid = message[AccountConversation.MESSAGE_UUID]
            message_type = message[AccountConversation.TYPE]
            payload = message[AccountConversation.PAYLOAD]
            time = message[AccountConversation.TIME]
        except KeyError as e:
            raise MessagesQueueError("Missing field: " + e.args[0])

        # noinspection PyBroadException
        try:
            delivered = await self.__deliver_message__(
                message_uuid, message_type, recipient_class, recipient_key, message)

        except Exception:
            logging.exception("Failed to deliver message")
            return

        history = self.history

        flags = MessageFlags(message.get(AccountConversation.FLAGS, []))

        if MessageFlags.DO_NOT_STORE in flags:
            return delivered

        if delivered and (MessageFlags.REMOVE_DELIVERED in flags):
            return delivered

        await history.add_message(
            gamespace_id,
            sender,
            message_uuid,
            str(recipient_class),
            str(recipient_key),
            datetime.datetime.fromtimestamp(time, tz=pytz.utc),
            message_type,
            payload,
            flags,
            delivered=delivered)

        return delivered

    async def __deliver_message__(self, message_uuid, message_type, recipient_class, recipient_key, message):

        if not isinstance(message, dict):
            raise MessageSendError(400, "Payload message to be a dict")

        exchange_id = AccountConversation.__id__(recipient_class, recipient_key)

        channel = await self.connection.channel()

        try:
            f = Future()

            def cancel_handle():
                try:
                    del self.handle_futures[message_uuid]
                except KeyError:
                    pass

            def delivered_(m):
                if not f.done():
                    if not isinstance(m.method, pika.spec.Basic.Ack):
                        cancel_handle()
                        f.set_result(False)

            def closed(ch, reason, param):
                if not f.done():
                    cancel_handle()
                    f.set_result(False)

            # add the future to the handles in case callback_queue will bring something
            self.handle_futures[message_uuid] = f

            channel.confirm_delivery(delivered_)
            channel.add_on_close_callback(closed)

            from pika import BasicProperties

            dumped = ujson.dumps(message)

            properties = BasicProperties(
                content_type='text/plain',
                reply_to=self.callback_queue.routing_key,
                correlation_id=message_uuid,
                headers={
                    AccountConversation.TYPE: message_type
                })

            channel.basic_publish(
                exchange_id,
                '',
                dumped,
                properties=properties,
                mandatory=True)

            try:
                delivered = await with_timeout(
                    timeout=datetime.timedelta(seconds=MessagesQueueModel.DELIVERY_TIMEOUT),
                    future=f)
            except TimeoutError:
                cancel_handle()
                delivered = False
        finally:
            if channel.is_open:
                channel.close()

        logging.debug("Message '{0}' {1} been delivered.".format(message_uuid, "has" if delivered else "has not"))

        return delivered

    # noinspection PyBroadException
    async def __outgoing_message_worker__(self, gamespace, sender, queue):

        channel = await self.connection.channel()

        properties = BasicProperties(
            delivery_mode=2,
        )

        class Wrapper(object):
            future = None

        wrapped = Wrapper()

        def delivered_(m):
            import pika
            f = wrapped.future
            if not f or f.done():
                return
            f.set_result(isinstance(m.method, pika.spec.Basic.Ack))

        def closed(ch, reason, param):
            f = wrapped.future
            if not f or f.done():
                return
            f.set_result(False)

        channel.confirm_delivery(delivered_)
        channel.add_on_close_callback(closed)

        try:
            while True:
                wrapped.future = Future()

                try:
                    body = queue.get_nowait()
                except QueueEmpty:
                    return True

                try:
                    channel.basic_publish(
                        '',
                        self.message_incoming_queue_name,
                        body,
                        mandatory=True,
                        properties=properties)
                except Exception:
                    logging.exception("Failed to public message.")

                success = await wrapped.future
                wrapped.future = None
                queue.task_done()

                if not success:
                    return False
        finally:
            channel.close()

    @validate(gamespace="int", sender="int", messages="json_list", authoritative="bool")
    async def add_messages(self, gamespace, sender, messages, authoritative=False):

        out_queue = Queue()

        time = utc_time()

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

            if MessageFlags.SERVER in flags:
                raise MessageSendError(409, "Cannot set 'server' flag directly, "
                                            "use scope 'message_authoritative' instead.")

            if authoritative:
                flags.set(MessageFlags.SERVER)

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
                AccountConversation.FLAGS: flags.as_list(),
                AccountConversation.TIME: time
            })

            out_queue.put_nowait(body)

        workers_count = min(self.outgoing_message_workers, out_queue.qsize())

        for i in range(0, workers_count):
            IOLoop.current().add_callback(self.__outgoing_message_worker__, gamespace, sender, out_queue)

        await out_queue.join(timeout=datetime.timedelta(seconds=MessagesQueueModel.PROCESS_TIMEOUT))

    @validate(gamespace="int", sender="int", recipient_class="str",
              recipient_key="str", message_type="str", payload="json_dict",
              flags=MessageFlags, authoritative="bool")
    def add_message(self, gamespace, sender, recipient_class, recipient_key, message_type, payload, flags,
                    authoritative=False):

        if MessageFlags.SERVER in flags:
            raise MessageSendError(409, "Cannot set 'server' flag directly, "
                                        "use scope 'message_authoritative' instead.")

        if authoritative:
            flags.set(MessageFlags.SERVER)

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
            AccountConversation.FLAGS: flags.as_list(),
            AccountConversation.TIME: utc_time()
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

    @validate(message="json_dict")
    async def __enqueue_message__(self, message):

        channel = await self.connection.channel()

        properties = BasicProperties(
            delivery_mode=2,  # make message persistent
        )

        # noinspection PyBroadException
        try:
            body = ujson.dumps(message)

            f = Future()

            def delivered_(m):
                import pika
                if not f.done():
                    f.set_result(isinstance(m.method, pika.spec.Basic.Ack))

            def closed(ch, reason, param):
                if not f.done():
                    f.set_result(False)

            channel.confirm_delivery(delivered_)
            channel.add_on_close_callback(closed)

            channel.basic_publish(
                '',
                self.message_incoming_queue_name,
                body,
                mandatory=True,
                properties=properties)

            result = await f
        except Exception as e:
            logging.exception("Failed to public message.")
            result = False
        finally:
            channel.close()

        return result
