CREATE TABLE `group_cluster_accounts` (
  `account_id` int(11) unsigned NOT NULL,
  `gamespace_id` int(11) unsigned NOT NULL,
  `cluster_id` int(11) unsigned NOT NULL,
  `cluster_data` int(11) unsigned NOT NULL,
  PRIMARY KEY (`account_id`,`gamespace_id`,`cluster_id`,`cluster_data`),
  UNIQUE KEY `account_id` (`account_id`,`gamespace_id`,`cluster_id`),
  KEY `cluster_id` (`cluster_id`),
  KEY `account_id_2` (`account_id`),
  KEY `cluster_id_2` (`cluster_id`),
  CONSTRAINT `group_cluster_accounts_ibfk_1` FOREIGN KEY (`cluster_id`) REFERENCES `group_clusters` (`cluster_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;