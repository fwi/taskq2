--[]
create table taskq_props
(
taskq_key varchar(256) primary key not null,
taskq_value varchar(4096)
);

--[]
create table taskq_servers
(
id integer generated always as identity(start with 1) primary key,
created timestamp default now,
last_active timestamp not null,
abandoned boolean default false,
name varchar(256) not null,
taskq_group varchar(128)
);

--[]
create table taskq_tasks 
( 
id bigint generated always as identity(start with 1) primary key,
created timestamp default now,
server_id integer not null,
abandoned boolean default false,
expire_date timestamp not null,
retry_count integer default 0,
qname varchar(128) not null,
qos_key varchar(256) default null
);

--[]
create table taskq_items 
(
task_id bigint primary key not null,
created timestamp default now,
item blob not null,
constraint fk_taskq_items_task_id
	foreign key (task_id)
	references taskq_tasks (id)
	on delete cascade
);
