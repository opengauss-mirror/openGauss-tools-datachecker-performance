use test_schema;

DROP TABLE IF EXISTS `t_double`;
CREATE TABLE IF NOT EXISTS `t_double` (
  `id` int(11) NOT NULL,
  `c_double` double DEFAULT NULL,
  `c_double0` double(10,0) DEFAULT NULL,
  `c_double1` double(10,1) DEFAULT NULL,
  `c_double2` double(10,2) DEFAULT NULL,
  `c_double3` double(10,3) DEFAULT NULL,
  `c_double4` double(10,4) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

INSERT INTO `t_double` (`id`, `c_double`, `c_double0`, `c_double1`, `c_double2`, `c_double3`, `c_double4`) VALUES
	(1, 1, 1, 1.0, 1.00, 1.000, 1.0000),
	(2, 1.1, 2, 1.1, 1.10, 1.001, 1.0001),
	(3, -0.001, -2, 1.2, 1.01, 1.100, 1.1000),
	(4, 9999.999, 100000000, 2.1, 0.00, 0.000, 0.0000),
	(5, -2.1, 1000000000, -2.1, -0.01, -0.001, -0.0001),
	(6, 0, 0, 0.0, -1000.09, -10000.190, -1000.1900),
	(7, -1.99, NULL, -1.0, -1.99, 9999.999, 999.9999),
	(8, NULL, NULL, NULL, NULL, NULL, NULL);