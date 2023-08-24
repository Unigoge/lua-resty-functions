local core       = require "resty.core";
local semaphore  = require "ngx.semaphore";
local lock       = require "resty.lock";
local http       = require "resty.http";
local cjson      = require "cjson";
local ffi        = require "ffi";

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
local ffi_new    = ffi.new;

local _concurrent = {
    _VERSION = '0.01',
}

local mt = { __index = _concurrent }

local FREE_LIST_REF = 0;
local running_threads = {};

local function ref_thread(th)
    if th == nil then
        return -1;
    end
    
    local ref = running_threads[FREE_LIST_REF];
    if ref and ref ~= 0 then
         running_threads[FREE_LIST_REF] = running_threads[ref];
    else
        ref = #running_threads + 1;
    end
    running_threads[ref] = th;

    return ref;
end

local function unref_thread(ref)
    if ref >= 0 then
        running_threads[ref] = running_threads[FREE_LIST_REF];
        running_threads[FREE_LIST_REF] = ref;
    end
end

local function gc_thread(cdata)
    
    local thread_id = tonumber(cdata.thread_id);

    if thread_id > 0 then
        
        -- ngx.log(ngx.STDERR, "collecting thread: ", thread_id );
        
        local th = running_threads[thread_id];
        unref_thread(thread_id);
        
        local ok, err = pcall( function()
            ngx.thread.kill(th.thread);
        end );
    
        if not ok then
            ngx.log(ngx.ERR, "failed to kill thread ", err);
        else
            pcall( function() ngx.ctx.threads_running = (ngx.ctx.threads_running - 1) end );
        end
        
        cdata.thread_id = 0
    end
end

local ctype = ffi.metatype("struct { int thread_id; }", { __gc = gc_thread } );
        
function _concurrent.make_handler( threads_params )
    
    local handler_obj = setmetatable( {
        threads = {},
        gc_cdata = {},
        res = {},
        start_time = ngx.now(),
        total_run_time_limit = 0
    }, mt );
    
    if ngx.ctx.threads_running == nil then ngx.ctx.threads_running = 0 end
    
    if threads_params and threads_params.total_run_time_limit then
        handler_obj.total_run_time_limit = threads_params.total_run_time_limit;
        
        handler_obj.timeout_thread = ngx.thread.spawn( function()
            ngx.sleep( handler_obj.total_run_time_limit );
            
            pcall( function() ngx.ctx.threads_running = (ngx.ctx.threads_running - 1) end );
            return "0.timeout";
        end );
    
        local cdata = ffi_new(ctype);
        cdata.thread_id = ref_thread( { thread = handler_obj.timeout_thread } );
        handler_obj.gc_timeout_thread = cdata;
        
        -- to signal that caller should call collectgarbage("collect") before exiting
        pcall( function() ngx.ctx.threads_running = ((ngx.ctx.threads_running or 0) + 1) end );
    end
    
    if threads_params and threads_params.threads then
        if type(threads_params.threads) == "table" then
            for thread_name, params in pairs(threads_params.threads) do
                if type(thread_name) ~= "string" then thread_name = tostring(thread_name) end
                if params.run then
                    _concurrent.start_thread( handler_obj, thread_name, params.run, params );
                end
            end
        elseif type(threads_params.threads) == "function" then
            _concurrent.start_thread( handler_obj, "1", threads_params.threads );
        end
    end
    
    return handler_obj;
end
                
function _concurrent.start_thread( self, thread_name, run_function, params )
    
    if not self or type(self) ~= "table" then
        
        if self and type(self) == "string" then thread_name = self;
        elseif self and type(self) == "function" then run_function = self end
    
        self = _concurrent.make_handler();
    end
    
    if thread_name and type(thread_name) == "function" then 
        run_function = thread_name;
        thread_name = nil;
    end
    
    if not thread_name then        
        thread_name = tostring(#self.threads + 1);
    end
    
    thread_name = string.gsub( thread_name, "%.", "_" ); -- dots are not allowed in thread names
    
    if not run_function or type(run_function) ~= "function" then return nil, "no function" end
    
    if not params then params = {} end
    
    local th = { name = thread_name };
    
    th.thread = ngx.thread.spawn( function(args)
        
        local ret = { pcall( function()
                    
            local res = { run_function(table.unpack(args or {})) };
            
            return table.unpack(res or {});
        end ) };
        
        local ok = ret[1];
        table.remove( ret, 1 );
        
        if ok == true then
            th.res = ret;
        else
            th.err = ret[1];
        end
        
        if th.timeout_sema then
            -- kill timeout 
            pcall( function() th.timeout_sema:post(1) end );
        end
                
        -- ngx.log( ngx.STDERR, "Thread exiting: ", utils.value_tostring( th ) );
        
        pcall( function() ngx.ctx.threads_running = (ngx.ctx.threads_running - 1) end );
        return tostring(thread_name), ok, ret;
        
    end, params.args );

    th.thread_id = ref_thread(th);
    local cdata = ffi_new(ctype);
    cdata.thread_id = th.thread_id;
    self.gc_cdata[ th.thread_id ] = cdata;

    if params.timeout then
        -- need to start a timer thread
        th.timeout_sema = semaphore.new();
        
        th.timeout_thread = ngx.thread.spawn( function()
                
            th.timeout_sema:wait( params.timeout );
            
            local finished = false;
            if not th.res and not th.err then
                th.err = "timeout";
                finished = true;
            end
            
            pcall( function() ngx.ctx.threads_running = (ngx.ctx.threads_running - 1) end );
            return tostring(thread_name)..".timeout", finished;
        end );
    
        local cdata = ffi_new(ctype);
        th.timeout_thread_id = ref_thread( { thread = th.timeout_thread } );
        cdata.thread_id = th.timeout_thread_id;
        self.gc_cdata[ th.timeout_thread_id ] = cdata;
        
        -- to signal that caller should call collectgarbage("collect") before exiting
        pcall( function() ngx.ctx.threads_running = ((ngx.ctx.threads_running or 0) + 2) end );    
    else
        -- to signal that caller should call collectgarbage("collect") before exiting
        pcall( function() ngx.ctx.threads_running = ((ngx.ctx.threads_running or 0) + 1) end );
    end
    
    self.threads[th.name] = th.thread_id;
    
    return self;
end

local function save_res_and_cleanup_thread_data( self, th )

    pcall( function()
            if th.thread then
                if coroutine.status( th.thread ) ~= "dead" and coroutine.status( th.thread ) ~= "zombie" then
                    pcall( function() ngx.thread.kill( th.thread ) end );
                    pcall( function() ngx.ctx.threads_running = (ngx.ctx.threads_running - 1) end );
                end

                th.thread = nil;
            end

            if th.timeout_thread then
                if coroutine.status( th.timeout_thread ) ~= "dead" and coroutine.status( th.timeout_thread ) ~= "zombie" then
                    pcall( function() ngx.thread.kill( th.timeout_thread ) end );
                    pcall( function() ngx.ctx.threads_running = (ngx.ctx.threads_running - 1) end );
                end

                th.timeout_thread = nil;
            end

            -- clean up for timeout thread
            if th.timeout_thread_id and th.timeout_thread_id > 0 then
                unref_thread(th.timeout_thread_id);
                if self.gc_cdata[ th.timeout_thread_id ] and self.gc_cdata[ th.timeout_thread_id ].thread_id == th.timeout_thread_id then
                    self.gc_cdata[ th.timeout_thread_id ].thread_id = 0;
                end
                th.timeout_thread_id = 0;
            end

            -- clean up for finished thread
            if th.thread_id and th.thread_id > 0 then
                unref_thread(th.thread_id);
                if self.gc_cdata[ th.thread_id ] and self.gc_cdata[ th.thread_id ].thread_id == th.thread_id then
                    self.gc_cdata[ th.thread_id ].thread_id = 0;
                end                
                th.thread_id = 0;
            end
    
            if th.err or th.res then
                self.res[th.name] = { err = th.err, res = th.res };
            end
            
            self.threads[th.name] = 0;
    end );                           

end

function _concurrent.wait_for_all_threads( self, timeout )
    
    local timeout_thread;
    if timeout and timeout > 0 and not self.err and self.err ~= "timeout" then
        timeout_thread = ngx.thread.spawn( function()
            ngx.sleep(timeout);
            
            return "_.timeout", true;
        end );
    end
    
    local threads = {};
    
    local threads_num;
    
    repeat
        
        threads_num = 0;
        
        local cleaned_threads_num = 0;
        
        if self.err and self.err == "timeout" then break end
        
        for th_name, th_id in pairs( self.threads ) do
            if th_id > 0 then
                local th = running_threads[th_id];
                if th then
                    if not th.res and not th.err then
                        if th.thread and ( coroutine.status( th.thread ) ~= "dead" or coroutine.status( th.thread ) ~= "zombie" ) then
                            threads[#threads + 1] = th.thread;
                            threads_num = threads_num + 1;
                        end
                        if th.timeout_thread and ( coroutine.status( th.timeout_thread ) ~= "dead" or coroutine.status( th.timeout_thread ) ) ~= "zombie" then
                            threads[#threads + 1] = th.timeout_thread;
                        end                
                    else -- save results and make sure that threads are dead
                        save_res_and_cleanup_thread_data( self, th );
                        cleaned_threads_num = cleaned_threads_num + 1;
                    end
                else
                    ngx.log( ngx.STDERR, "unable to find thread ", th_name, " with id: ", th_id );
                end
            end
        end
        
        if self.timeout_thread then
            if threads_num == 0 then -- all threads are done - need to kill timeout
                _concurrent.kill_all_threads( self ); -- it would kill timeout as well
                break;
            end
            if coroutine.status( self.timeout_thread ) == "dead" or coroutine.status( self.timeout_thread ) == "zombie" then
                self.err = "timeout";
                _concurrent.kill_all_threads( self ); 
                break;                
            end
            
            threads[#threads + 1] = self.timeout_thread;
        end
        
        if timeout and timeout == 0 then break end -- collect results without waiting
        if threads_num == 0 then break end -- all threads are done
        
        if timeout_thread then threads[#threads + 1] = timeout_thread end
            
        local thread_finished, thread_name, run_finished;
        local ok, err = pcall( function()
                thread_finished, thread_name, run_finished = ngx.thread.wait( table.unpack(threads) );
                while thread_name == nil and cleaned_threads_num > 0 do
                        -- could happen if thread has finished and already got cleaned on line 312
                        thread_finished, thread_name, run_finished = ngx.thread.wait( table.unpack(threads) );
                        cleaned_threads_num = cleaned_threads_num - 1;
                end                
        end );
    
        -- ngx.log( ngx.STDERR, "ngx.thread.wait returned: ", ok, " ", err, " ", thread_finished, " ", thread_name, " ", run_finished );
        if not ok then -- just in case if ngx.thread.wait would crash
            ngx.log( ngx.ERR, "ngx.thread.wait exited with error: ", err or "unknown" );
            ngx.sleep(0.001); -- yield before continuing
        else
            if thread_finished == false then
                -- to handle killed threads - only log for debugging
                if thread_name then -- thread_name should be an error string in this case
                    ngx.log( ngx.ERR, "thread was aborted with error: ", thread_name );
                end
            elseif thread_name then
                -- should parse returned thread_name
                local thread_name_parts = utils.split( thread_name, "%." );
                if thread_name_parts[1] == "_" and thread_name_parts[2] == "timeout" then
                    -- should stop waiting and return
                    return nil, "timeout";
                elseif thread_name_parts[1] == "0" and thread_name_parts[2] == "timeout" then
                    self.err = "timeout";
                    _concurrent.kill_all_threads( self ); 
                    break;                    
                elseif thread_name_parts[2] == "timeout" then
                    -- use thread_name_parts[1] to find and kill a running thread
                    local th_id = self.threads[ thread_name_parts[1] ];
                    if th_id and th_id > 0 then
                        local th = running_threads[th_id];
                        if th then

                            -- ngx.log( ngx.STDERR, "finished timeout thread: ", th.name );

                            save_res_and_cleanup_thread_data( self, th );
                        else
                            ngx.log( ngx.ERR, "unable to find thread ", thread_name_parts[1], " with id: ", th_id );
                        end
                    else
                        ngx.log( ngx.WARN, "timeout for finished thread: ", thread_name_parts[1] );
                    end
                else
                    -- use thread_name_parts[1] to find and kill timeout thread
                    local th_id = self.threads[ thread_name_parts[1] ];
                    if th_id and th_id > 0 then
                        local th = running_threads[th_id];
                        if th then

                            -- ngx.log( ngx.STDERR, "finished thread: ", th.name );

                            save_res_and_cleanup_thread_data( self, th );
                        else
                            ngx.log( ngx.ERR, "unable to find thread ", thread_name_parts[1], " with id: ", th_id );
                        end
                    else
                        ngx.log( ngx.ERR, "unable to find thread: ", thread_name_parts[1] );
                    end
                end
            else
                -- could happen if some thread has got cleaned already
                ngx.sleep(0.001); -- yield before continuing
            end
        end
        
        threads = {};
        ngx.sleep(0.001); -- yield before continuing
        
        -- ngx.log( ngx.STDERR, "threads_num: ", threads_num );
    until threads_num == 0;
    
    if timeout_thread then pcall( function() ngx.thread.kill(timeout_thread) end ) end
    
    -- collect results
    local ret = {};
    
    ret.res = {};
    utils.tableMerge( ret.res, self.res );
    
    for th_name, th_id in pairs( self.threads ) do
        if th_id > 0 then
            local th = running_threads[th_id];
            if th and th_name == th.name then
                if th.res then
                    ret.res[th.name] = { res = th.res };
                elseif th.err then
                    ret.res[th.name] = { err = th.err };
                else
                    if th.thread and ( coroutine.status( th.thread ) ~= "dead" or coroutine.status( th.thread ) ~= "zombie" ) then
                        -- some thread is still running
                        ret.err = "incomplete";
                    end
                end
            else
                ngx.log( ngx.ERR, "unable to find thread ", th_name, " with id: ", th_id );
            end
        end
    end
    
    if self.err then
        ret.err = self.err;
    end
    
    return ret;
end

function _concurrent.kill_all_threads( self )
 
    local ret = {};

    ret.res = {};
    utils.tableMerge( ret.res, self.res );
    
    for th_name, th_id in pairs( self.threads ) do
        if th_id > 0 then
            local th = running_threads[th_id];
            if th and th_name == th.name then
                if th.res then
                    ret.res[th.name] = { res = th.res };
                elseif th.err then
                    ret.res[th.name] = { err = th.err };
                else
                    if th.thread and ( coroutine.status( th.thread ) ~= "dead" or coroutine.status( th.thread ) ~= "zombie" ) then
                        -- some thread is still running
                        ret.err = "incomplete";
                    end
                end

                save_res_and_cleanup_thread_data( self, th );
            else
                ngx.log( ngx.ERR, "unable to find thread ", th_name, " with id: ", th_id );
            end
        end
    end
    
    if self.timeout_thread then
        if coroutine.status( self.timeout_thread ) == "dead" or coroutine.status( self.timeout_thread ) == "zombie" then
            -- set "timeout" error only if there were some running threads
            if ret.err then 
                ret.err = "timeout";
            end
        else
            pcall( function() ngx.thread.kill( self.timeout_thread ) end );
            pcall( function() ngx.ctx.threads_running = (ngx.ctx.threads_running - 1) end );            
        end
        
        -- clean up for timeout thread
        if self.gc_timeout_thread and self.gc_timeout_thread.thread_id > 0 then
            unref_thread(self.gc_timeout_thread.thread_id);
            self.gc_timeout_thread.thread_id = 0;
        end
        
        self.timeout_thread = nil;
    end
    
    if self.err then
        ret.err = self.err;
    end
    
    return ret;
end

------------------------------------
-- TESTS
--[[
-- testing ngx.ctx in the timer context
ngx.ctx.test = "test";
ngx.log( ngx.STDERR, "ngx.ctx.test = ", ngx.ctx.test );

-- testing garbage collection via ctype/cdata
local th;                       
local function test_gc()
        
        -- thread should be running for 30 seconds if not killed
        th = ngx.thread.spawn( function() ngx.sleep(30); end );
        
        local cdata = ffi_new(ctype);
        cdata.thread_id = ref_thread( { thread=th } );
        local table = { "1", "2", "3", cdata }; -- to be collected
    
end

test_gc();
ngx.sleep(1);

ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

-- run garbage collector - otherwise program would no exit for 30 seconds till thread is running
collectgarbage("collect");
ngx.log(ngx.STDERR, "Thread is ", coroutine.status(th));

ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

ngx.sleep(1);

-- testing with one thread no timeouts
-- Test 1 - start thread with arguments
local wh1 = _concurrent.make_handler();

wh1:start_thread( "test1", function( two, four ) ngx.sleep(1); return two + 1, four + 1; end, { args = { 2, 4 } } );

local ret = wh1:wait_for_all_threads();

ngx.log( ngx.STDERR, "Test1 results: ", utils.value_tostring( ret ) );

wh1 = nil;
collectgarbage("collect");
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

-- Test 2 - start thread that throws
local wh2 = _concurrent.make_handler();

wh2:start_thread( "test2", function() ngx.sleep(1); error("boom!!!"); end );

ret = wh2:wait_for_all_threads();

ngx.log( ngx.STDERR, "Test2 results: ", utils.value_tostring( ret ) );

wh2 = nil;
collectgarbage("collect");
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

-- Test 3 - start thread with timeout
local wh3 = _concurrent.make_handler();

wh3:start_thread( "test3", function() ngx.sleep(30); end, { timeout = 1 } );

ret = wh3:wait_for_all_threads();

ngx.log( ngx.STDERR, "Test3 results: ", utils.value_tostring( ret ) );

wh3 = nil;
collectgarbage("collect");
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

-- Test 4 - make handler with timeout
local wh4 = _concurrent.make_handler( { total_run_time_limit = 1 });

wh4:start_thread( "test4", function() ngx.sleep(30); end );

ret = wh4:wait_for_all_threads();

ngx.log( ngx.STDERR, "Test4 results: ", utils.value_tostring( ret ) );

wh4 = nil;
collectgarbage("collect");
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

-- Test 5 - test simple thread start mode

local wh5 = _concurrent.start_thread( function() ngx.sleep(1); return 2, 3; end );

ret = wh5:wait_for_all_threads();

ngx.log( ngx.STDERR, "Test5 results: ", utils.value_tostring( ret ) );

wh5 = nil;
collectgarbage("collect");
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

-- Test 6 - testing with 4 threads

local wh6 = _concurrent.make_handler();

wh6:start_thread( "test6_1", function() ngx.sleep(30); end, { timeout = 1 } );
wh6:start_thread( "test6_2", function() ngx.sleep(1.1); error("boom!!!"); end );
wh6:start_thread( "test6_3", function() ngx.sleep(1.2); return 1, 2, 3; end );
wh6:start_thread( "test6_4", function() ngx.sleep(1.3); end, { timeout = 30 } );

ret = wh6:wait_for_all_threads();

ngx.log( ngx.STDERR, "wh6: ", utils.value_tostring( wh6 ) );
ngx.log( ngx.STDERR, "Test6 results: ", utils.value_tostring( ret ) );

wh6 = nil;
collectgarbage("collect");
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

-- Test 7 - testing garbage collection

local function start_threads()
    local wh7 = _concurrent.make_handler();

    wh7:start_thread( "test7_1", function() ngx.sleep(30); end, { timeout = 5 } );
    wh7:start_thread( "test7_2", function() ngx.sleep(5.1); error("boom!!!"); end );
    wh7:start_thread( "test7_3", function() ngx.sleep(5.2); return 1, 2, 3; end );
    wh7:start_thread( "test7_4", function() ngx.sleep(5.3); end, { timeout = 30 } );
    
    ngx.sleep(0.1);
    ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);
    
    return wh7;
end

local wh7 = start_threads();
ngx.sleep(2);

ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);
-- wh7:wait_for_all_threads();

collectgarbage("collect");
    
ngx.sleep(2);
wh7 = nil;

collectgarbage("collect");
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

-- Test 8 - testing collecting results of finished threads

local wh8 = _concurrent.make_handler();

wh8:start_thread( "test8_1", function() ngx.sleep(30); end, { timeout = 1 } );
wh8:start_thread( "test8_2", function() ngx.sleep(1.1); error("boom!!!"); end );
wh8:start_thread( "test8_3", function() ngx.sleep(1.2); return 1, 2, 3; end );
wh8:start_thread( "test8_4", function() ngx.sleep(1.3); end, { timeout = 30 } );
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

ngx.sleep(3);

-- should return immediately
ret = wh8:wait_for_all_threads();

-- ngx.log( ngx.STDERR, "wh8: ", utils.value_tostring( wh6 ) );
ngx.log( ngx.STDERR, "Test8 results: ", utils.value_tostring( ret ) );

wh8 = nil;
collectgarbage("collect");
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

-- Test 9 - testing killing all threads and collecting results of finished threads

local wh9 = _concurrent.make_handler();

wh9:start_thread( "test9_1", function() ngx.sleep(30); end, { timeout = 1 } );
wh9:start_thread( "test9_2", function() ngx.sleep(1.1); error("boom!!!"); end );
wh9:start_thread( "test9_3", function() ngx.sleep(3.2); return 1, 2, 3; end );
wh9:start_thread( "test9_4", function() ngx.sleep(3.3); end, { timeout = 30 } );
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

ngx.sleep(2);

-- should return immediately
ret = wh9:kill_all_threads();

ngx.log( ngx.STDERR, "Test9 results: ", utils.value_tostring( ret ) );

wh9 = nil;
collectgarbage("collect");
ngx.log(ngx.STDERR,"ngx.ctx.threads_running: ", ngx.ctx.threads_running);

if ngx.ctx.threads_running > 0 then ngx.log( ngx.STDERR, utils.value_tostring( running_threads ) ) end
]]

return _concurrent;
