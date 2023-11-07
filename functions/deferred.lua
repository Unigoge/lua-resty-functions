local core       = require "resty.core";
local semaphore  = require "ngx.semaphore";
local lock       = require "resty.lock";
local http       = require "resty.http";
local cjson      = require "cjson";

local utils      = require "functions.utils";

local str_gmatch = string.gmatch;
local str_lower  = string.lower;
local str_upper  = string.upper;
local str_find   = string.find;
local str_sub    = string.sub;
local str_len    = string.len;
local str_gsub   = string.gsub;
local tostring   = tostring;
local toupper    = string.upper;
local tolower    = string.lower;
local tbl_concat = table.concat;
local tbl_insert = table.insert;
local pcall      = pcall;

local _deferred = {
    _VERSION = '0.01',
}

local mt = { __index = _deferred }

local deferred_singletons = { };
local deferred_monitor_initialized = false;
local deferred_monitor_sema = semaphore.new()

-- queue_name is mandatory, other args are optional but are welcome at the first initial call
function _deferred.get( queue_name, queue_size, tasks_num, replace_oldest )
    
    if not queue_name then return nil end
    
    if deferred_singletons[queue_name] then return deferred_singletons[queue_name] end
    
    -- need to create a new singleton object
    local deferred_queue = {
        queue = utils.new_queue(queue_size or 2000, replace_oldest or true),
        queue_sema = semaphore.new(),
        tasks_sema = semaphore.new(),
        tasks_num = tasks_num or 100
    };
    
    deferred_queue.tasks_sema:post(deferred_queue.tasks_num);
    
    deferred_singletons[queue_name] = setmetatable( deferred_queue, mt );
    
    if deferred_monitor_initialized == false then _deferred.init()
    else deferred_monitor_sema:post(1) end

    return deferred_singletons[queue_name];
end

function _deferred.init()
    
    if deferred_monitor_initialized == true then return end
    
    local monitor;
    monitor = function( premature )

        if premature then 
            -- kill all executor threads
            for queue_name, deferred_queue in pairs( deferred_singletons ) do
                if deferred_queue.executor then pcall( function() ngx.thread.kill( deferred_queue.executor ) end ) end
            end
            
            return;
        end
        
        local monitor_thread = ngx.thread.spawn( function()
            
            while true do
                
                if ngx.worker.exiting() == true then 
                    -- kill all executor threads
                    for queue_name, deferred_queue in pairs( deferred_singletons ) do
                        if deferred_queue.executor then pcall( function() ngx.thread.kill( deferred_queue.executor ) end ) end
                    end
                    
                    -- ngx.log(ngx.STDERR, "exiting monitor thread - ", ngx.time() );
                    return; 
                end
                
                -- check that executors are running for all deferred queues
                for queue_name, deferred_queue in pairs( deferred_singletons ) do
                   
                    if not deferred_queue.executor or coroutine.status( deferred_queue.executor ) == "dead" 
                                                   or coroutine.status( deferred_queue.executor ) == "zombie" then
                    
                        if deferred_queue.executor and coroutine.status( deferred_queue.executor ) == "zombie" then
                           pcall( function() ngx.thread.kill( deferred_queue.executor ) end ); 
                        end
                        
                        -- ngx.log( ngx.STDERR, "starting executor thread for ", queue_name );
                        deferred_queue:run_executor( queue_name ); 
                    end
                end
                
                deferred_monitor_sema:wait(5); -- sleeping for 5 seconds
                
            end
            
        end );
    
        local ok, err = ngx.thread.wait( monitor_thread );
        if not ok and err then
            ngx.log( ngx.STDERR, "_deferred.init - monitor thread has failed with error: ", err );
        end
        
        if ngx.worker.exiting() == true then return end
            
        ngx.timer.at( 0, monitor );
    end

    ngx.timer.at( 0, monitor );
    
end

function _deferred.run_executor( self, queue_name )

    if ngx.worker.exiting() == true then return end

    if not self then
        if queue_name then
            self = _deferred.get( queue_name );
        end
    end

    self.executor = ngx.thread.spawn( function()

            while true do
                
                while self.queue:count() == 0 do
                    if ngx.worker.exiting() == true then return end

                    self.queue_sema:wait(1); -- sleeping for 1 second
                end
                
                repeat
                    if ngx.worker.exiting() == true then return end

                    -- to make sure that there are only up to self.tasks_num timers running at any time
                    local ok, err = self.tasks_sema:wait(0.1);
                until ok and ok == true;

                if self.queue:count() > 0 then
                    
                    -- ngx.log(ngx.STDERR, "executing tasks - queue_size=", self.queue:count(), ", timer threads running=", self.tasks_num - self.tasks_sema:count());

                    if self.queue_sema:count() > self.queue:count() then self.queue_sema:wait(0) end
                    
                    -- run tasks in separate timer contexts to avoid accumulating large numbers of used corutines
                    ngx.timer.at( 0, function()

                            local cnt = 0;
                            local ok, ret;
                            
                            while self.queue:count() > 0 and ( ret or cnt < 100 ) do
                                
                                if ngx.worker.exiting() == true then return end

                                local next_task = self.queue:pop();
                                if next_task then
                                    
                                    if self.queue_sema:count() > self.queue:count() then self.queue_sema:wait(0) end
                                    
                                    ok, ret = pcall( next_task, ret, self.queue:count() );
                                    if not ok and ret then
                                        ngx.log( ngx.ERR, queue_name, " - executor task has failed with error: ", ret );
                                    end
                                    
                                    if not ret then
                                        ngx.sleep(0.01); -- yield
                                    end
                                    
                                    cnt = cnt + 1;
                                else
                                    if self.queue_sema:count() > self.queue:count() then self.queue_sema:wait(0) end
                                    break;
                                end
                            end

                            self.tasks_sema:post(1);
                            -- ngx.log(ngx.STDERR, "stoping ngx.timer, executed tasks num=", cnt );
                    end );

                    ngx.sleep(0.001); -- yield
                else
                    self.tasks_sema:post(1);
                end
            end
    end ); -- end of thread code
   
end

function _deferred.execute_async( self, task, queue_name )

    if ngx.worker.exiting() == true then return end

    if not self then
        if queue_name then
            self = _deferred.get( queue_name );
        else
            return false, "unknown queue";
        end
    end
    
    local ok, err = self.queue:push( task );
    if ok then self.queue_sema:post(1) end

    return ok, err;    
end

-------------------------------------------------------------
--[[ TESTS

local deferred_queue = _deferred.get( "test1", 10, 1, true );
-- execute 10 individual tasks
for cnt=1,10 do
    local task = function()
       ngx.log( ngx.STDERR, "cnt=", cnt ); 
    end
    deferred_queue:execute_async( task );
end

ngx.sleep(1);
ngx.log( ngx.STDERR, "********** end test1" );
ngx.log( ngx.STDERR, "********** start test1.1" );
deferred_queue = _deferred.get( "test1-1", 10, 10, true );
-- execute 10 individual tasks simulteniously 
for cnt=1,10 do
    local task = function()
       ngx.log( ngx.STDERR, "cnt=", cnt );
       ngx.sleep(0.01);
    end
    deferred_queue:execute_async( task );
end

ngx.sleep(1);
ngx.log( ngx.STDERR, "********** end test1.1" );
ngx.log( ngx.STDERR, "********** start test2" );

deferred_queue = _deferred.get( "test2", 10, 5, true );
-- execute 10 tasks as a sequence
for cnt=1,10 do
    local task = function( sequence, remaining )
        if remaining > 0 and not sequence then ngx.log( ngx.STDERR, "cnt=", cnt, ", start sequence " );
        else ngx.log( ngx.STDERR, "cnt=", cnt, ", seq ", sequence ) end
    
        if remaining > 0 then return true 
        else ngx.log( ngx.STDERR, "end sequence" ) end 
    end
    deferred_queue:execute_async( task );
end

ngx.sleep(1);
ngx.log( ngx.STDERR, "********** end test2" );
ngx.log( ngx.STDERR, "********** start test2-1" );

deferred_queue = _deferred.get( "test2-1", 10, 5, true );
-- execute tasks as 2 sequences
for cnt=1,10 do
    local task = function( sequence, remaining )
        if not sequence then 
            ngx.log( ngx.STDERR, "start sequence " );
            sequence = tostring(cnt);
        else
            sequence = sequence..tostring(cnt);
        end
    
        if #sequence == 5 or remaining == 0 then
            ngx.log( ngx.STDERR, "end sequence ", sequence );
            return nil;
        end
        return sequence;
    end
    deferred_queue:execute_async( task );
end

ngx.sleep(1);
ngx.log( ngx.STDERR, "********** end test2-1" );
ngx.log( ngx.STDERR, "********** start test3" );

deferred_queue = _deferred.get( "test3", 1000, 50, true );
-- execute 1000 tasks as fast as possible
local total_cnt = 0;
ngx.update_time();
local start_time = ngx.now();

for cnt=1,1000 do
    local task = function( sequence, remaining )
        total_cnt = total_cnt + 1;
    end
    deferred_queue:execute_async( task );
end

while total_cnt < 1000 do
    ngx.update_time();
    local curr_time = ngx.now();
    if curr_time - start_time > 5 then break end
    ngx.sleep(0.001);
end
    
ngx.update_time();
local end_time = ngx.now();
    
ngx.log( ngx.STDERR, "total_cnt=", total_cnt, ", run_time=", end_time - start_time );
ngx.log( ngx.STDERR, "********** end test2" );

ngx.sleep(30);

]]

return _deferred;
