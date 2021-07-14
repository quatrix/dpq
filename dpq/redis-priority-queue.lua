-- redis-priority-queue
-- Author: Gabriel Bordeaux (gabfl)
-- Github: https://github.com/gabfl/redis-priority-queue
-- Version: 1.0.4
-- (can only be used in 3.2+)

-- Get mandatory vars
local action = ARGV[1];
local queueName = ARGV[2];
local delayedQueue = queueName .. "::delayed"

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

-- Making sure required fields are not nil
assert(not isempty(action), 'ERR1: Action is missing')
assert(not isempty(queueName), 'ERR2: Queue name is missing')

local function _push()
    local payload = ARGV[3]
    local priority = ARGV[4]
    local invisibleUntil = tonumber(ARGV[5])

    assert(not isempty(payload), 'ERR5: Payload is missing')

    if invisibleUntil > 0 then
        redis.call('ZADD', delayedQueue, 'NX', invisibleUntil, serialize(priority, payload))
    else
        return redis.call('ZADD', queueName, 'NX', priority, payload)
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

    if task[1] == nil then
        return nil
    end

    -- when worker takes a task, we make it invisible for a specified 
    -- amount of time in which client should finish processing and delete the task
    -- if worker crashes task will later become visible and another worker will take it.
    -- if a worker fails processing it should make the task visibile right away.

    redis.call('ZADD', delayedQueue, 'NX', invisibleUntil, serialize(priority, payload))

    return task
end

local function _enqueue_delayed()
    local now = tonumber(ARGV[3])
    local tasks = redis.call('ZRANGEBYSCORE', delayedQueue, 0, now)

    for k,task in pairs(tasks) do 
        local deserilized = split(task, '::')
        local priority = deserilized[1]
        local payload = deserilized[2]

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

    redis.call('ZADD', delayedQueue, 'NX', visibility, serialize(priority, payload))
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
else
    error('ERR3: Invalid action.')
end
