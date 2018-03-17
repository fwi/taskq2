-- Default Queries used by workq
-- This file must use UTF-8 for character encoding.
-- Names of queries are set as comments that start with "--[" and end with a "]"
-- The value between an opening comment value [] and closing comment value [/] 
-- is not escaped in any manner and used literally as query.

--[TASKQ.GET_PROP]
select taskq_value from taskq_props where taskq_key=@key
--[TASKQ.MERGE_PROP]
-- Kind of hard to read but this is HSQLDB's "update or insert", see http://hsqldb.org/doc/2.0/guide/dataaccess-chapt.html#dac_merge_statement
merge into taskq_props using (values(@key, @value))
as vals(k, v) on taskq_props.taskq_key = vals.k
when matched then update set taskq_props.taskq_value=vals.v
when not matched then insert (taskq_key, taskq_value) values (vals.k, vals.v)
--[TASKQ.DELETE_PROP]
delete from taskq_props where taskq_key=@key

--[TASKQ.FIND_SERVER]
select id from taskq_servers where name=@name and taskq_group=@group
--[TASKQ.INSERT_SERVER]
insert into taskq_servers (name, taskq_group, last_active)
values (@name, @group, @lastActive)
--[TASKQ.SERVER_ACTIVE]
update taskq_servers set abandoned=false, last_active=@lastActive where id=@id

--[TASKQ.TASKS_ACTIVE]
select count(*) as tasks_count from taskq_tasks where server_id=@serverId and abandoned=false

--[TASKQ.INSERT_ITEM]
insert into taskq_items (task_id, item) values (@taskId, @item)

--[TASKQ.INSERT_TASK]
insert into taskq_tasks (server_id, qname, expire_date, qos_key)
values (@serverId, @qname, @expireDate, @qosKey)
--[TASKQ.LOAD_TASK]
select t.id, t.qname, t.expire_date, t.qos_key, t.retry_count, i.item
from taskq_tasks t, taskq_items i
where t.id=@id and abandoned=false and t.server_id=@serverId and i.task_id=t.id
--[TASKQ.DELETE_TASK]
delete from taskq_tasks where id=@id and server_id=@serverId
--[TASKQ.UPDATE_TASK_QNAME]
update taskq_tasks set qname=@qname, expire_date=@expireDate, retry_count=0 where id=@id and server_id=@serverId and abandoned=false
--[TASKQ.UPDATE_TASK_RETRY]
update taskq_tasks set retry_count=@retryCount, expire_date=@expireDate where id=@id and server_id=@serverId and abandoned=false
--[TASKQ.ABANDON_TASK]
update taskq_tasks set abandoned=true where id=@id and server_id=@serverId

--[TASKQ.HAVE_EXPIRED]
select distinct qname from taskq_tasks where server_id=@serverId and abandoned=false and expire_date <= now() 
--[TASKQ.EXPIRED_PER_Q]
select id from taskq_tasks where server_id=@serverId and abandoned=false and qname=@qname and expire_date <= now() order by id limit @maxAmount
--[TASKQ.UPDATE_EXPIRED]
update taskq_tasks set expire_date=@expireDate where id=@id and server_id=@serverId and abandoned=false

--[TASKQ.DEAD_SERVERS]
select id, last_active from taskq_servers where abandoned=false and taskq_group=@group and last_active < @lastActive 
--[TASKQ.LOCK_SERVER]
select id from taskq_servers where id=@deadServerId and abandoned=false for update 
--[TASKQ.UPDATE_FAIL_OVER]
update taskq_tasks set server_id=@serverId, expire_date=now() where server_id=@deadServerId and abandoned=false
--[TASKQ.ABANDON_DEAD_SERVER]
update taskq_servers set abandoned=true where id=@deadServerId