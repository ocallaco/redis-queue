local QCONFIG = "RESERVED:QCONFIG"
local QCONFIGCHANNEL = "RESERVEDCHANNEL:QCONFIG"

local STANDARD_QUEUE = "QUEUE"
local LOADBAL_QUEUE = "LBQUEUE"

RedisQueueConfig = {meta = {}}

RedisQueueConfig.queueTypes = {
   STANDARD_QUEUE,
   LOADBAL_QUEUE
}


function RedisQueueConfig.meta:__index(key)
   return RedisQueueConfig[key]
end


-- sample_config = {
--    "TEST1" = STANDARD_QUEUE,
--    "TEST2" = LOADBAL_QUEUE,
-- }

-- valid queue cannot contain any non alphanumeric chars
function RedisQueueConfig:setConfig(config)
   local queueEntries = {}
   for queue,qType in pairs(config) do
      if queue:find("%W") then
         error("Illegal character in queue name: " .. queue)
      end
      table.insert(queueEntries, queue .. ":" .. qType)
   end

   queueEntries = table.concat(queueEntries, ",")
   
   self.client.eval([[
      local config_addr = KEYS[1]

      local queueEntryString = ARGV[1]

      local queueEntries = {}
      
      local i = 1
      while true do
         local a,b,queue,entry = queueEntryString:find("(%w-)%:(%w*)", i)
         if entry == nil then break end
         table.insert(queueEntries,queue)
         table.insert(queueEntries,entry)
         i = b + 1
      end


      redis.call('del', config_addr)
      return redis.call('hmset', config_addr, unpack(queueEntries))

   ]], 1, QCONFIG, queueEntries)

end

function RedisQueueConfig:fetchConfig(cb)
   self.client.hgetall(QCONFIG, function(res)
      self.configtbl = {}
      for i=1,#res,2 do
         self.configtbl[res[i]] = res[i+1]
      end

      if cb then
         cb()
      end
   end)
end

function RedisQueueConfig:getqueuetype(queue)
   if self.configtbl == nil then
      error("No config loaded")
   elseif self.configtbl[queue] == nil then
      error("Queue not in config: " .. queue)
   end

   return self.configtbl[queue]
end

function RedisQueueConfig:new(redis)
   local newconfig = {}
   newconfig.client = redis

   setmetatable(newconfig, RedisQueueConfig.meta)

   return newconfig
end


local queueconfig = {
}

setmetatable(queueconfig, {
   __call = RedisQueueConfig.new,
})

return queueconfig
