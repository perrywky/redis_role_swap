Redis role swap
===============

This is a redis clone of [mysql_role_swap](https://github.com/37signals/mysql_role_swap), which automatically promote slave to master as this [docs](http://redis.io/topics/admin) described.

You must use virtual ip to use this script.

Install dependencies
====================

- iproute2 (/sbin/ip)
- ruby
- ruby bundle install

Usage
=====

.   /redis_role_swap.rb -c cluster.yml

Maunal operations
=================

Do arping virtual ip: arping -U -c 4 -I #{INTERFACE} #{FLOATING_IP}

References
==========

- Redis replication - https://groups.google.com/forum/?fromgroups=#!searchin/redis-db/slave$20up$20to$20date/redis-db/JPvnyfUWx_Q/Un8XNwRkW04J
