use test_schema;

DROP TABLE IF EXISTS `t_varchar`;
CREATE TABLE IF NOT EXISTS `t_varchar` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `c_varchar0` varchar(1) DEFAULT NULL,
  `c_varchar1` varchar(160) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4;


INSERT INTO `t_varchar` (`id`, `c_varchar0`, `c_varchar1`) VALUES
	(1, '1', 'In this incident, Tencent was exposed to have leaked the most data records, reaching 1.5 billion, of which 1.4 billion were from Tencent QQ.'),
	(2, 'a', 'On January 23rd, according to media reports, security researchers issued a warning and discovered a database containing at least 26 billion leaked data records'),
	(3, NULL, NULL),
	(4, ' ', ' ');