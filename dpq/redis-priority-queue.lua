local action = ARGV[1];
local queueName = ARGV[2];
local delayedQueue = queueName .. "::delayed"
local retriesLookup = queueName .. "::retries"
local delayKeyPrefix = queueName .. "::delay::"

-- returns true if empty or null
-- http://stackoverflow.com/a/19667498/50501
local function isempty(s)
    return s == nil or s == '' or type(s) == 'userdata'
end

local function serialize(priority, payload)
    return cmsgpack.pack({priority, payload})
end

local function deserialize(packed)
    return cmsgpack.unpack(packed)
end

local function create_group_delay_key(group_id)
    return delayKeyPrefix .. group_id
end

assert(not isempty(action), 'ERR1: Action is missing')
assert(not isempty(queueName), 'ERR2: Queue name is missing')

local function _already_queued(payload, priority)
    local in_runnable = redis.call('ZSCORE', queueName, payload)

    local in_delayed = redis.call('ZSCORE', delayedQueue, serialize(priority, payload)) 

    return in_runnable or in_delayed
end

-- FIXME: for debugging binary 
local b='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/' 
local function enc(data)
    return ((data:gsub('.', function(x) 
        local r,b='',x:byte()
        for i=8,1,-1 do r=r..(b%2^i-b%2^(i-1)>0 and '1' or '0') end
        return r;
    end)..'0000'):gsub('%d%d%d?%d?%d?%d?', function(x)
        if (#x < 6) then return '' end
        local c=0
        for i=1,6 do c=c+(x:sub(i,i)=='1' and 2^(6-i) or 0) end
        return b:sub(c+1,c+1)
    end)..({ '', '==', '=' })[#data%3+1])
end


local function _push()
    local payload = ARGV[3]
    local priority = ARGV[4]
    local invisibleUntil = tonumber(ARGV[5])
    local retries = tonumber(ARGV[6])
    local group_id = ARGV[7]

    assert(not isempty(payload), 'ERR5: Payload is missing')

    -- redis.log(redis.LOG_WARNING, 'payload: ', payload)
    -- redis.log(redis.LOG_WARNING, 'group_id: ', group_id)

    local packed_payload = cmsgpack.pack({payload, group_id})

    redis.log(redis.LOG_WARNING, 'push() payload: ', enc(packed_payload))

    -- redis.log(redis.LOG_WARNING, 'push() payload: ', enc(payload))

    if _already_queued(packed_payload, priority) then
        return
    end

    redis.call('HSET', retriesLookup, packed_payload, retries)

    if invisibleUntil > 0 then
        redis.call('ZADD', delayedQueue, 'NX', invisibleUntil, serialize(priority, packed_payload))
    else
        redis.call('ZADD', queueName, 'NX', priority, packed_payload)
    end
end

local function _pop()
    local invisibleUntil = ARGV[3]

    -- FIXME: can't run BZPOPMAX (blocking zpopmax) in user script
    -- is there some way to make worker block when queue is empty?
    --
    -- local popped = redis.call('BZPOPMAX', queueName, timeout)

    local found_runnable = false

    while true
    do
        local value = redis.call('ZPOPMAX', queueName, 1)

        -- redis.log(redis.LOG_WARNING, 'pop() payload: ', enc(task[1]))

        local packed_payload = value[1]
        local priority = value[2]

        if packed_payload == nil then
            return nil
        end

        local unpacked_payload = cmsgpack.unpack(packed_payload)
        local payload = unpacked_payload[1]
        local group_id = unpacked_payload[2]

        local delay = redis.call('GET', create_group_delay_key(group_id))
        redis.log(redis.LOG_WARNING, '--delay-- ', delay)

        if delay then
            redis.call( 'ZADD', delayedQueue, 'NX', delay, serialize(priority, packed_payload))
        else
            local remaining_attempts = redis.call('HINCRBY', retriesLookup, packed_payload, -1)

            if remaining_attempts == -1 then
                redis.call('HDEL', retriesLookup, packed_payload)
            else

                -- when worker takes a task, we make it invisible for a specified 
                -- amount of time in which client should finish processing and delete the task
                -- if worker crashes task will later become visible and another worker will take it.
                -- if a worker fails processing it should make the task visibile right away.
                redis.call('ZADD', delayedQueue, 'NX', invisibleUntil, serialize(priority, packed_payload))

                return {payload, group_id, priority, remaining_attempts}
            end
        end
    end


end

local function _enqueue_delayed()
    local now = tonumber(ARGV[3])
    local tasks = redis.call('ZRANGEBYSCORE', delayedQueue, 0, now)

    redis.log(redis.LOG_WARNING, "_enqueue_delayed")

    for k,task in pairs(tasks) do 
        local deserilized = deserialize(task)
        local priority = deserilized[1]
        local payload = deserilized[2]

        redis.call('ZADD', queueName, 'NX', priority, payload)
        redis.call('ZREM', delayedQueue, task)
    end
end

local function _remove_from_delayed_queue()
    local payload = ARGV[3]
    local group_id = ARGV[4]
    local priority = ARGV[5] 

	payload = cmsgpack.pack({payload, group_id})

    redis.call('HDEL', retriesLookup, payload)
    redis.call('ZREM', delayedQueue, serialize(priority, payload))
end

local function _set_visibility()
    local payload = ARGV[3]
	local group_id = ARGV[4]
    local priority = ARGV[5] 
    local visibility = ARGV[6] 

	payload = cmsgpack.pack({payload, group_id})

    redis.call('ZADD', delayedQueue, 'XX', visibility, serialize(priority, payload))
end

local function _delay_group()
	local group_id = ARGV[3]
	local delay = tonumber(ARGV[4])
	local expire = tonumber(ARGV[5])

    redis.log(redis.LOG_WARNING, ' setting group delay = ', delay)
    redis.call('SET', create_group_delay_key(group_id), delay, 'EX', expire)
end

local function _get_size()
    local runnable_size = redis.call('ZCARD', queueName)
    local invisible_size = redis.call('ZCARD', delayedQueue)

    return runnable_size + invisible_size
end

if action == 'push' then
    return _push()
elseif action == 'pop' then
    return _pop()
elseif action == 'enqueue_delayed' then
    return _enqueue_delayed()
elseif action == 'remove_from_delayed_queue' then
    return _remove_from_delayed_queue()
elseif action == 'set_visibility' then
    return _set_visibility()
elseif action == 'delay_group' then
    return _delay_group()
elseif action == 'get_size' then
    return _get_size()
else
    error('ERR3: Invalid action.')
end
