-- H2 initialization script --

CREATE TABLE IF NOT EXISTS `User` (
  `id` int(11) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

MERGE INTO User(id, name) VALUES (1, 'user1');
