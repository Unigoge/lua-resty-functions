# lua-resty-functions

PERMANENTLY IN-PROGRESS

# TODOs

Extend "function" module functionality:

- add `_deferred.execute_sync` and `_deferred.wait` methods - should allow to execute functions across [rewrite|access|content]_by_lua_block boundaries
- add `_concurrent.wait_for_any_thread` method - just seems logical to have it
- add module for “shared” functions execution - should allow to store function info and required args in shared dictionary queues to be executed by any worker process
- add module for “scheduled” functions execution - should allow to execute “shared” functions at set time by any worker
- add tests which should be compliant with the framework that other lua_resty_xxxx nodules use
- add README and usage examples
