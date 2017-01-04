CREATE TABLE `group_clusters` (
  `cluster_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `gamespace_id` int(11) unsigned NOT NULL,
  `cluster_size` int(11) unsigned NOT NULL,
  `cluster_data` int(11) unsigned NOT NULL,
  PRIMARY KEY (`cluster_id`),
  KEY `leaderboard_id` (`cluster_data`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;