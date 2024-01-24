use test_schema;

DROP TABLE IF EXISTS `t_char`;
CREATE TABLE IF NOT EXISTS `t_char` (
  `id` char(32) NOT NULL,
  `name` char(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `t_char` (`id`, `name`) VALUES
	('a', 'a_name'),
	('b', 'b_name'),
	('c', 'c_name'),
	('d', NULL),
	('e', '');