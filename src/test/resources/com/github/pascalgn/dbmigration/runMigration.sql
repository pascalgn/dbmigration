-- H2 initialization script --

CREATE TABLE IF NOT EXISTS `User` (
  `id` int(11) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `password` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
);

MERGE INTO User(id, name, password) VALUES (1, 'user1', 'secret123');

CREATE TABLE IF NOT EXISTS `UserGroup` (
  `id` int(11) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

MERGE INTO `UserGroup`(id, name) VALUES (1, 'group1');
