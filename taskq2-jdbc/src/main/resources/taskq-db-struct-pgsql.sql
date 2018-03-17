-- Create database
-- bin\createdb taskq2
-- Login
-- bin\psql -d taskq2
-- Create user
-- create user taskq2 with password 'taskq2';
-- grant all privileges on database taskq2 to taskq2;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO taskq2;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO taskq2;
-- \q
-- bin\psql -d taskq2 -U taskq2
-- \i './taskq-db-struct-pgsql.sql'
-- []
create table if not exists taskq_props
(
taskq_key varchar(255) primary key not null,
taskq_value varchar(4096)
);

-- []
create table if not exists taskq_servers
(
id serial primary key,
created timestamp default current_timestamp,
last_active timestamp not null,
abandoned boolean default false,
name varchar(255) not null,
taskq_group varchar(128)
);

-- []
create table if not exists taskq_tasks
( 
id bigserial primary key,
created timestamp default current_timestamp,
server_id integer not null,
abandoned boolean default false,
expire_date timestamp not null,
retry_count smallint default 0,
qname varchar(128) not null,
qos_key varchar(255) default null
);

-- []
create table if not exists taskq_items
(
task_id bigint primary key not null,
created timestamp default current_timestamp,
item bytea not null,
constraint fk_taskq_items_task_id
	foreign key (task_id)
	references taskq_tasks (id)
	on delete cascade
);
