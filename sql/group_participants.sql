CREATE TABLE `group_participants` (
  `participation_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `gamespace_id` int(11) unsigned NOT NULL,
  `group_id` int(11) unsigned NOT NULL,
  `participation_account` int(11) NOT NULL,
  `participation_role` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`participation_id`),
  UNIQUE KEY `unique` (`gamespace_id`,`group_id`,`participation_account`),
  KEY `group_id` (`group_id`),
  CONSTRAINT `group_participants_ibfk_1` FOREIGN KEY (`group_id`) REFERENCES `groups` (`group_id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;