CREATE TABLE `messages` (
  `message_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `gamespace_id` int(11) unsigned NOT NULL,
  `message_uuid` varchar(40) DEFAULT NULL,
  `message_sender` int(11) NOT NULL,
  `message_recipient_class` varchar(64) NOT NULL,
  `message_recipient` varchar(255) NOT NULL DEFAULT '',
  `message_time` datetime NOT NULL,
  `message_type` varchar(64) NOT NULL,
  `message_payload` json NOT NULL,
  `message_delivered` tinyint(1) NOT NULL DEFAULT '0',
  `message_flags` set('REMOVE_DELIVERED','EDITABLE','DELETABLE','SERVER') DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  UNIQUE KEY `message_uuid` (`message_uuid`),
  KEY `message_recipient` (`message_recipient`),
  KEY `message_sender` (`message_sender`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;