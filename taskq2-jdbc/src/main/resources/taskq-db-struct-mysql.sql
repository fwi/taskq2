-- Proper UTF8 support, see https://mathiasbynens.be/notes/mysql-utf8mb4
-- Max. length for use in index is varchar(191) instead of varchar(255)
-- Command line: mysql -u root -p
-- Create database
-- create database taskq2;
-- Create user
-- Note that '%' does NOT include 'localhost'
-- Note that dbname must be between backticks if a minus-sign is included in the database name.
-- drop user ''@'localhost';
-- create user 'taskq2'@'localhost' identified by 'taskq2';
-- grant all privileges on `taskq2`.* to 'taskq2'@'localhost';
-- create user 'taskq2'@'%' identified by 'taskq2';
-- grant all privileges on `taskq2`.* to 'taskq2'@'%';
-- flush privileges;
-- Update user password
-- set password for 'taskq2' = password('taskq2');
-- flush privileges;
-- Import this sql-script:
-- mysql -u taskq2 -p taskq2 < taskq-db-struct-mysql.sql
-- []
create table if not exists taskq_props
(
taskq_key varchar(191) primary key not null,
taskq_value varchar(4096)
)
default character set = utf8mb4
collate = utf8mb4_unicode_ci
engine = InnoDB;

-- []
create table if not exists taskq_servers
(
id mediumint not null auto_increment primary key,
created timestamp default current_timestamp,
last_active timestamp not null,
abandoned boolean default false,
name varchar(191) not null,
taskq_group varchar(128)
)
default character set = utf8mb4
collate = utf8mb4_unicode_ci
engine = InnoDB;

-- []
create table if not exists taskq_tasks
( 
id serial,
created timestamp default current_timestamp,
server_id mediumint not null,
abandoned boolean default false,
expire_date timestamp not null,
retry_count smallint default 0,
qname varchar(128) not null,
qos_key varchar(191) default null
)
default character set = utf8mb4
collate = utf8mb4_unicode_ci
engine = InnoDB;

-- []
create table if not exists taskq_items
(
task_id bigint unsigned primary key not null,
created timestamp default current_timestamp,
item blob not null,
constraint fk_taskq_items_task_id
	foreign key (task_id)
	references taskq_tasks (id)
	on delete cascade
)
default character set = utf8mb4
collate = utf8mb4_unicode_ci
engine = InnoDB;

