CREATE TABLE `groups` (
  `group_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `gamespace_id` int(11) unsigned NOT NULL,
  `group_class` varchar(64) NOT NULL DEFAULT '',
  `group_key` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`group_id`),
  UNIQUE KEY `group_unique` (`gamespace_id`,`group_class`,`group_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;