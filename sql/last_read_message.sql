CREATE TABLE `last_read_message` (
  `gamespace_id` int(11) NOT NULL,
  `account_id` int(11) unsigned NOT NULL,
  `message_recipient_class` varchar(64) NOT NULL DEFAULT '',
  `message_recipient` varchar(64) NOT NULL DEFAULT '',
  `last_message_time` datetime NOT NULL,
  `last_message_uuid` varchar(36) NOT NULL DEFAULT '',
  UNIQUE KEY `gamespace_id` (`gamespace_id`,`account_id`,`message_recipient_class`,`message_recipient`),
  KEY `account_id` (`account_id`),
  KEY `message_recipient` (`message_recipient`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;