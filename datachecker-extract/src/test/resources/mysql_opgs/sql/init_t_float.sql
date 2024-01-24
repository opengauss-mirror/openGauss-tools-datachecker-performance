use test_schema;
DROP TABLE IF EXISTS `t_float`;
CREATE TABLE IF NOT EXISTS `t_float` (
  `id` int(11) NOT NULL,
  `c_float` float DEFAULT NULL,
  `c_float2` float(10,0) DEFAULT NULL,
  `c_float3` float(10,3) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `t_float` (`id`, `c_float`, `c_float2`, `c_float3`) VALUES
	(1, 1, 1, 1.000),
	(2, 0, 2, 0.000),
	(3, 1, 3, 1.000),
	(4, 1.001, 4, 1.001),
	(5, 0.000147, 5, 0.001),
	(6, 0.00000985, 66, 852.001),
	(7, NULL, NULL, NULL);
