use test_schema;

DROP TABLE IF EXISTS `t_varchar`;
CREATE TABLE IF NOT EXISTS `t_varchar` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `c_varchar0` varchar(1) DEFAULT NULL,
  `c_varchar1` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4;


INSERT INTO `t_varchar` (`id`, `c_varchar0`, `c_varchar1`) VALUES
	(1, '1', '在此次事件中，腾讯被曝泄露的数据记录最多，达到15亿条，其中14亿条来自腾讯QQ。'),
	(2, 'a', '1月23日，据媒体报道，有安全研究人员于发出警告，发现了一个包含至少260亿条泄露数据记录的数据库蜗'),
	(3, NULL, NULL),
	(4, ' ', ' ');