# taskq2
Task queues with optional persistence and fail-over.

Message queues in a database are a bit of an anti-pattern and software like [Apache Kafka](https://kafka.apache.org/)
provide a superior alternative. But if you like to manage tasks and/or messages via queues
and want the option to store them in a database, TaskQ2 could be an alternative for you.

Features:
  - Core:
    - Event driven, efficient threads usage. 
    - Specify amount of workers per queue. 
    - Pause a queue or all queues.
    - Queue with "Quality of Service" (Qos) management available.
  - Jdbc
    - Store tasks/messages as BLOBs in the database.
    - Pause all queues when the database is unavailable.
    - Reload expired tasks from the database.
    - Fail-over: if one server stops, another server takes over the work.
  - Demo
    - Shows the TaskQ2 mechanics.
    - Can be used to test load and throughput for many thousands of tasks. 

# Core

The core has only one depedency on `slf4j-api`. It is suited for managing work from a batch.
The core can manage all resources (like thread-pools) internally and once all tasks are added,
a call to `awaitAllTasksDone` can be used to wait for all processing to be done.
This simple form of usage is shown in the test-class [TestTqTasksSimple](./taskq2-core/src/test/java/com/github/fwi/taskq2/TestTqTasksSimple.java). 

# Jdbc

The persistence part has an additional dependency on [`fwutil-jdbc`](https://github.com/fwi/fwutil-jdbc)
for low-level JDBC query management and has support for [HikariCP](https://github.com/brettwooldridge/HikariCP) as database pool
(another database pool can be used, as long as it can be made available via a `javax.sql.DataSource`).

There are many "knobs" to tune database usage, 
these are all shown in [TqDbConf](./taskq2-jdbc/src/main/java/com/github/fwi/taskq2/db/TqDbConf.java).
Fail-over is implemented via the concept of servers that are part of a server-group.
Each server has a heart-beat and if other servers in the same group see an outdated heart-beat,
a server in the same group will attempt to take over the work of the dead server.
If the database is unavailable, fail-over will be disabled for a while (`DbGracePeriod`).
 
If a server fails to update the heart-beat in the database, the server will consider the database unavailable
and all queues are paused. As soon as the heart-beat is updated in the database again,
all queues that were previously unpaused will be unpaused (i.e. queues that were paused 
before the database became unavailable, will remain paused).

Running TaskQ2 with persistence does take some work to properly setup which the Demo shows.
The Demo also shows task error- and retry-handling which is not provided out-of-the-box.

Error- and retry-handling for processing of tasks by a queue is not part of TaskQ2 by design:
every application has different requirements for retrying and different interpretations of what an error is.
TaskQ2 is robust however: TaskQ2 will not stop just because a queue throws an error while handling a task. 

# Demo and queue design

The [Demo](./taskq2-demo/src/main/java/com/github/fwi/taskq2/demo/Demo.java) 
shows what is needed to get TaskQ2 going backed by a database. 
The Dmeo also shows how retries can be implemented and how error-handling can be done.

The Demo itself is bad design: queues do trivial work that can easily be repeated without consequences.
This in turn generates too much database traffic. But it does serves the purposes of the Demo.
Just keep in mind that queues should be idempotent when possible 
and a "unit of work" for a queue should be within the boundaries of a state-change.

A queue that updates the database should do so within the same transaction for updating TaskQ2 database entries.
In this manner, if the database update fails, the task can be reloaded and executed again without side-effects.

Below the steps for a queue that may only send a file ONCE to another resource (e.g. FTP server somewhere):
  - Check database if file was marked "being send". If so, send task to error queue because at this stage, file might have been send, or not.
  - Check database if file was marked "send", if so do not send file but update task only (i.e jump to last steps).
  - Prepare sending of file, mark in database file as "being send".
  - On error, log the error and unmark file as "being send" and retry task later.
  - On succes, log the succes and mark file as "send" in database.
  - Perform any additional cleanup (e.g. move file from send-directory to archive-directory).
  - Finish up: update database, remove task from TaskQ2 tables, create new tasks, etc..
  
With the steps outlined above, the chance of "double delivery" is minimized while at the same time,
required manual intervention is minimized (the latter is only required when the database updates fail
after a file was or failed to be delivered).

### TasKQ1

Version 2 because reworked from [TaskQ](https://github.com/fwi/TaskQ) version 1.
