local action = ARGV[1];
local queueName = ARGV[2];
local delayedQueue = queueName .. "::delayed"
local retriesLookup = queueName .. "::retries"

-- returns true if empty or null
-- http://stackoverflow.com/a/19667498/50501
local function isempty(s)
    return s == nil or s == '' or type(s) == 'userdata'
end

local function serialize(priority, payload)
    return priority .. '::' .. payload
end

local function split(s, delimiter)
    local result = {}
    for match in (s..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match)
    end
    return result
end

assert(not isempty(action), 'ERR1: Action is missing')
assert(not isempty(queueName), 'ERR2: Queue name is missing')

local function _already_queued(payload, priority)
    if redis.call('ZSCORE', queueName, payload) ~= nil then
        return true
    end

    if redis.call('ZSCORE', delayedQueue, serialize(priority, payload)) ~= nil then
        return true
    end

    return false
end

local function _push()
    local payload = ARGV[3]
    local priority = ARGV[4]
    local invisibleUntil = tonumber(ARGV[5])
    local retries = tonumber(ARGV[6])

    assert(not isempty(payload), 'ERR5: Payload is missing')

    if _already_queued(payload, priority) then
        return
    end

    redis.call('HSET', retriesLookup, payload, retries)

    if invisibleUntil > 0 then
        redis.call('ZADD', delayedQueue, 'NX', invisibleUntil, serialize(priority, payload))
    else
        redis.call('ZADD', queueName, 'NX', priority, payload)
    end
end

local function _pop()
    local invisibleUntil = ARGV[3]

    -- FIXME: can't run BZPOPMAX (blocking zpopmax) in user script
    -- is there some way to make worker block when queue is empty?
    --
    -- local popped = redis.call('BZPOPMAX', queueName, timeout)
    
    local task = redis.call('ZPOPMAX', queueName, 1)
    local payload = task[1]
    local priority = task[2]

    if payload == nil then
        return nil
    end

    local remaining_attempts = redis.call('HINCRBY', retriesLookup, payload, -1)

    if remaining_attempts == -1 then
        redis.call('HDEL', retriesLookup, payload)
        return nil
    end

    -- when worker takes a task, we make it invisible for a specified 
    -- amount of time in which client should finish processing and delete the task
    -- if worker crashes task will later become visible and another worker will take it.
    -- if a worker fails processing it should make the task visibile right away.

    redis.call('ZADD', delayedQueue, 'NX', invisibleUntil, serialize(priority, payload))

    return {payload, priority, remaining_attempts}
end

local function _enqueue_delayed()
    local now = tonumber(ARGV[3])
    local tasks = redis.call('ZRANGEBYSCORE', delayedQueue, 0, now)

    redis.log(redis.LOG_WARNING, "_enqueue_delayed")

    for k,task in pairs(tasks) do 


        -- FIXME: temporary hack, fix ASAP.
        -- this will probably break when there's another :: somewhere in the task
        local deserilized = split(task, '::')
        local priority = deserilized[1]
        local payload = deserilized[2]

        redis.log(redis.LOG_WARNING, "task: ", payload)


        redis.call('ZADD', queueName, 'NX', priority, payload)
        redis.call('ZREM', delayedQueue, task)
    end
end

local function _remove_from_delayed_queue()
    local payload = ARGV[3]
    local priority = ARGV[4] 

    redis.call('ZREM', delayedQueue, serialize(priority, payload))
end

local function _set_visibility()
    local payload = ARGV[3]
    local priority = ARGV[4] 
    local visibility = ARGV[5] 

    redis.call('ZADD', delayedQueue, 'XX', visibility, serialize(priority, payload))
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
elseif action == 'get_size' then
    return _get_size()
else
    error('ERR3: Invalid action.')
end
