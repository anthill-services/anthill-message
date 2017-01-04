CREATE TABLE `messages` (
  `message_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `gamespace_id` int(11) unsigned NOT NULL,
  `message_uuid` varchar(40) DEFAULT NULL,
  `message_sender` int(11) NOT NULL,
  `message_recipient_class` varchar(64) NOT NULL,
  `message_recipient` varchar(255) NOT NULL,
  `message_time` datetime NOT NULL,
  `message_type` varchar(64) NOT NULL,
  `message_payload` json NOT NULL,
  `message_delivered` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;