
from common.options import define

# Main

define("host",
       default="http://message-dev.anthill",
       help="Public hostname of this service",
       type=str)

define("listen",
       default="port:11000",
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
       default="anthill",
       type=str,
       help="MySQL account username")

define("db_password",
       default="",
       type=str,
       help="MySQL account password")

define("db_name",
       default="message",
       type=str,
       help="MySQL database name")

# Messaging

define("message_broker",
       default="amqp://anthill:anthill@127.0.0.1:5672/dev",
       help="RabbitMQ broker location for messaging (amqp).",
       group="message",
       type=str)

define("message_broker_max_connections",
       default=10,
       help="Maximum connections to maintain.",
       group="message",
       type=int)
