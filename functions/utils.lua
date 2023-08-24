local core       = require "resty.core";
local semaphore  = require "ngx.semaphore";

local _utils = {
    _VERSION = '0.01',
}

local mt = { __index = _utils }

local ffi = require("ffi")

                ffi.cdef[[

                char *strstr(const char *haystack, const char *needle);

                ]]

function _utils.is_substring( haystack, needle )

          local str1 = ffi.new("char[?]", #haystack + 64 )
          ffi.copy( str1, haystack )

          local str2 = ffi.new("char[?]", #needle + 64 )
          ffi.copy( str2, needle )

          local res = ffi.C.strstr( str1, str2 );

          if( res ~= nil ) then return true; end

          return false;
end

function _utils.serializeTable(val, name, skipnewlines, depth)
          skipnewlines = skipnewlines or false
          depth = depth or 0

          local tmp = string.rep(" ", depth)

          if name then tmp = tmp .. name .. " = " end

          if type(val) == "table" then
              tmp = tmp .. "{" .. (not skipnewlines and "\n" or "")

              for k, v in pairs(val) do
                  tmp =  tmp .. _utils.serializeTable(v, k, skipnewlines, depth + 1) .. "," .. (not skipnewlines and "\n" or "")
              end

              tmp = tmp .. string.rep(" ", depth) .. "}"
          elseif type(val) == "number" then
              tmp = tmp .. tostring(val)
          elseif type(val) == "string" then
              tmp = tmp .. string.format("%q", val)
          elseif type(val) == "boolean" then
              tmp = tmp .. (val and "true" or "false")
          else
              tmp = tmp .. "\"[inserializeable datatype:" .. type(val) .. "]\""
          end

          return tmp
end

function _utils.value_tostring( value )
    if type( value ) == "number" then return tostring(value) end
    if type( value ) == "string" then return value end
    if type( value ) ~= "table" then return "" end
    local value, n = string.gsub( _utils.serializeTable( value ), "\n", " ");
    return value;
end

function _utils.parse_uri(uri)

    local m, err = ngx.re.match(uri, [[^(http[s]?)://([^:/]+)(?::(\d+))?(.*)]],
        "jo")

    if not m then
        if err then
            return nil, "failed to match the uri: " .. uri .. ", " .. err
        end

        return nil, "bad uri: " .. uri
    else
        if m[3] then
            m[3] = tonumber(m[3])
        else
            if m[1] == "https" then
                m[3] = 443
            else
                m[3] = 80
            end
        end
        if not m[4] or "" == m[4] then m[4] = "/" end
        return m, nil
    end

end

function _utils.split(str, pat)
   local t = {}  -- NOTE: use {n = 0} in Lua-5.0
   local fpat = "(.-)" .. pat
   local last_end = 1
   local s, e, cap = str:find(fpat, 1)
   while s do
      if s ~= 1 or cap ~= "" then
   table.insert(t,cap)
      end
      last_end = e+1
      s, e, cap = str:find(fpat, last_end)
   end
   if last_end <= #str then
      cap = str:sub(last_end)
      table.insert(t, cap)
   end
   return t
end

function _utils.tablecopy(orig)
    local orig_type = type(orig)
    local copy
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in next, orig, nil do
            copy[ _utils.tablecopy(orig_key)] = _utils.tablecopy(orig_value)
        end
        setmetatable(copy, _utils.tablecopy(getmetatable(orig)))
    else -- number, string, boolean, etc
        copy = orig
    end
    return copy
end

function _utils.tableMerge(t1, t2)
    local was_updated = false;

    for k,v in pairs(t2) do
        if type(v) == "table" then
            if type(t1[k] or false) == "table" then

                if was_updated then _utils.tableMerge(t1[k] or {}, t2[k] or {})
                else was_updated = _utils.tableMerge(t1[k] or {}, t2[k] or {}) end

            else
                t1[k] = _utils.tablecopy(v)
                was_updated = true
            end
        elseif t1[k] and ( ( type(t1[k]) ~= type(v) ) or ( t1[k] ~= v ) ) then
            t1[k] = v
            was_updated = true
        elseif not t1[k] then
            t1[k] = v
            was_updated = true
        end
    end
    return was_updated
end

function _utils.tableCompare(t1, t2)
    if t1 == t2 then return true end
    local t1Type = type(t1)
    local t2Type = type(t2)
    if t1Type ~= t2Type then return false end
    if t1Type ~= 'table' then return false end

    local keySet = {}

    for key1, value1 in pairs(t1) do
        local value2 = t2[key1]
        if value2 == nil or _utils.tableCompare(value1, value2) == false then
            return false
        end
        keySet[key1] = true
    end

    for key2, _ in pairs(t2) do
        if not keySet[key2] then return false end
    end
    return true
end

function _utils.objectcopy_safe(orig)
    local orig_type = type(orig)
    local copy
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in next, orig, nil do
            if type(orig_value) ~= "userdata" and ( orig_key ~= "__index" or ( orig_key == "__index" and orig_value ~= orig )) then
                copy[ _utils.objectcopy_safe(orig_key) ] = _utils.objectcopy_safe(orig_value)
            end
        end
        if orig["__index"] and orig["__index"] == orig then
            copy["__index"] = copy;
        else
            setmetatable(copy, _utils.objectcopy_safe(getmetatable(orig)));
        end
    else -- number, string, boolean, etc
        copy = orig;
    end
    return copy
end

function _utils.escape_json_str(s)
      local in_char  = {'\\', '"', '/', '\b', '\f', '\n', '\r', '\t'}
      local out_char = {'\\', '"', '/',  'b',  'f',  'n',  'r',  't'}
      for i, c in ipairs(in_char) do
          s = s:gsub(c, '\\' .. out_char[i])
      end
      return s
end

function _utils.new_queue(size, allow_wrapping)
    -- Head is next insert, tail is next read
    local head, tail = 1, 1;
    local items = 0; -- Number of stored items
    local t = {}; -- Table to hold items
    return {
        _items = t;
        size = size;
        count = function (self) return items; end;
        push = function (self, item)
            if items >= size then
              if allow_wrapping then
                tail = (tail%size)+1; -- Advance to next oldest item
                items = items - 1;
              else
                return nil, "queue full";
              end
            end
            t[head] = item;
            items = items + 1;
            head = (head%size)+1;
            return true;
        end;
        pop = function (self)
            if items == 0 then
              return nil;
            end
            local item;
            item, t[tail] = t[tail], 0;
            tail = (tail%size)+1;
            items = items - 1;
            return item;
        end;
        peek = function (self)
            if items == 0 then
              return nil;
            end
            return t[tail];
        end;
        items = function (self)
            return function (t, pos)
              if pos >= t:count() then
                return nil;
              end
              local read_pos = tail + pos;
              if read_pos > t.size then
                read_pos = (read_pos%size);
              end
              return pos+1, t._items[read_pos];
            end, self, 0;
        end;
    };
end

-- threads is a table with the key that is a thread id and the value that is a thread handle
function _utils.wait_for_any_thread( threads, expected_number_of_threads )

    local handles = {};
    local terminated_tids = {};

    local tid, handle;
    for tid, handle in pairs( threads ) do
        if coroutine.status( handle ) ~= "dead" and coroutine.status( handle ) ~= "zombie" then
            handles[ #handles + 1 ] = handle;
        else
            terminated_tids[ #terminated_tids + 1 ] = tid;
        end
    end

    for _, tid in ipairs( terminated_tids ) do
        threads[ tid ] = nil;
    end

    if #handles == 0 then
        return 0;
    end

    if expected_number_of_threads and #handles < expected_number_of_threads then
        return #handles;
    end

    -- ngx.log( ngx.ERR, "Waiting for threads:\n", _utils.serializeTable(threads));

    -- wait for any thread to return
    ngx.thread.wait( table.unpack( handles ));
    return #handles - 1;

end

function _utils.wait_for_all_threads( threads )

    local threads_num = _utils.wait_for_any_thread( threads );

    while threads_num > 0 do
        threads_num = _utils.wait_for_any_thread( threads );            
    end

    -- should return immediately
    _utils.wait_for_any_thread( threads );

    return 0;
end

function _utils.kill_thread( handle )
    if handle == nil then return end
    
    if coroutine.status( handle ) == "dead" or coroutine.status( handle ) == "zombie" then
        return("dead"); 
    else
        pcall( function() ngx.thread.kill( handle ) end );
        return coroutine.status( handle ) or "killed";
    end
end

function _utils.send_http_req( url, args )

    local http  = require "resty.http";

    local httpc, err = http.new();
    if not httpc then
        return nil, "HTTP client's object is not initialized.";
    end

    if( ( string.sub( url, 1, 6) == "https:" ) and ( args.ssl_verify == nil ) ) then
        args.ssl_verify = false;
    end

    httpc:set_timeouts( args.timeout or 10000, args.timeout or 10000, args.timeout or 40000 ); -- 10 sec to connect, 10 sec to send, 40 seconds to read

    args.keepalive_timeout = args.keepalive_timeout or 60000;
    args.keepalive_pool = args.keepalive_pool or 60;

    local ok, res, err = pcall( httpc.request_uri, httpc, url, args );
    if not ok then
        return nil, res;
    end
    if not res and err and ( err == "closed" or err == "connection reset by peer" ) then -- retry only once in case of "connection reset by peer"

        httpc, err = http.new();
        if not httpc then
            return nil, "HTTP client's object is not initialized.";
        end

        httpc:set_timeouts( args.timeout or 10000, args.timeout or 10000, args.timeout or 40000 ); -- 10 sec to connect, 10 sec to send, 40 seconds to read

        res, err = httpc:request_uri( url, args );

    elseif res and res.status == 301 and res.headers["location"] then

        local original_url_parts = httpc:parse_uri( url, true );
        local new_url_parts = httpc:parse_uri( res.headers["location"], true );

        if new_url_parts[1] == "https" then
            if ( args.ssl_verify == nil ) then args.ssl_verify = false; end
            if not new_url_parts[3] or new_url_parts[3] == 80 then new_url_parts[3] = 443 end
        else
            if not new_url_parts[3] then new_url_parts[3] = 80 end
        end
        if string.sub( original_url_parts[4], 1, 2 ) == "/" then original_url_parts[4] = string.sub( original_url_parts[4], 2 ); end
        res, err = httpc:request_uri( new_url_parts[1] .. "://" .. new_url_parts[2] .. ":" .. tostring(new_url_parts[3]) .. "/" .. original_url_parts[4], args );

    end

    return res, err;
end

local deferred_queue;

function _utils.queue_deferred_function( deferred_function )
    
    if not deferred_queue then
        pcall( function()
            local functions_deferred = require "stash.functions.deferred";
            deferred_queue = functions_deferred.get( "stash.utils", 5000, 100, true );
        end )
    
        if not deferred_queue then return false, "not implemented" end
    end
    
    return deferred_queue:execute_async( deferred_function );
end

return _utils;
