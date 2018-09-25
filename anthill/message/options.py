
from anthill.common.options import define

# Main

define("host",
       default="http://localhost:9511",
       help="Public hostname of this service",
       type=str)

define("listen",
       default="port:9511",
       help="Socket to listen. Could be a port number (port:N), or a unix domain socket (unix:PATH)",
       type=str)

define("name",
       default="message",
       help="Service short name. User to discover by discovery service.",
       type=str)

# MySQL database

define("db_host",
       default="127.0.0.1",
       type=str,
       help="MySQL database location")

define("db_username",
       default="root",
       type=str,
       help="MySQL account username")

define("db_password",
       default="",
       type=str,
       help="MySQL account password")

define("db_name",
       default="dev_message",
       type=str,
       help="MySQL database name")

# Messaging

define("message_broker",
       default="amqp://guest:guest@127.0.0.1:5672/",
       help="RabbitMQ broker location for messaging (amqp).",
       group="message",
       type=str)

define("message_broker_max_connections",
       default=10,
       help="Maximum connections to maintain.",
       group="message",
       type=int)

define("group_cluster_size",
       default=1000,
       type=int,
       group="groups",
       help="Cluster size to group users groups around")

define("message_incoming_queue_name",
       default="message.incoming.queue",
       help="RabbitMQ incoming queue name.",
       group="message",
       type=str)

define("message_prefetch_count",
       default=32,
       type=int,
       group="message",
       help="How much of messages can be prefetch")

define("outgoing_message_workers",
       default=32,
       type=int,
       group="message",
       help="How much workers process the outgoing messages")
