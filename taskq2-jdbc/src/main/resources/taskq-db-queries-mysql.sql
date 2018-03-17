-- "Overload" queries from taskq-db-queries.sql with MySQL specifics

--[TASKQ.MERGE_PROP]
insert into taskq_props (taskq_key, taskq_value) values (@key, @value)
on duplicate key update taskq_value=values(taskq_value)
