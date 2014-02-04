local zmq      = require "lzmq"
local zloop    = require "lzmq.loop"
local zthreads = require "lzmq.threads"
local mp       = require "cmsgpack"
local zassert  = zmq.assert

local ENDPOINT = "inproc://main"

local THREAD_STARTER = [[
  local ENDPOINT = ]] .. ("%q"):format(ENDPOINT) .. [[
  local zmq      = require "lzmq"
  local zthreads = require "lzmq.threads"
  local mp       = require "cmsgpack"
  local zassert  = zmq.assert

  function TASK(do_work)
    local ctx      = zthreads.get_parent_ctx()

    local s, err   = ctx:socket{zmq.DEALER, connect = ENDPOINT}
    if not s then return end

    s:sendx(0, 'READY')
    while not s:closed() do
      local tid, cmd, args = s:recvx()
      if not tid then
        if cmd and (cmd:no() == zmq.errors.ETERM) then break end
        zassert(nil, cmd)
      end
      assert(tid and cmd)

      if cmd == 'END' then break end

      assert(cmd == 'TASK', "invalid command " .. tostring(cmd))
      assert(args, "invalid args in command")

      local res, err = mp.pack( do_work(mp.unpack(args)) )
      s:sendx(tid, 'RESP', res)
    end

    ctx:destroy()
  end
]]

local function pcall_ret_pack(ok, ...)
  if ok then return mp.pack(ok, ...) end
  return nil, ...
end

local function pcall_ret(ok, ...)
  if ok then return pcall_ret_pack(...) end
  return nil, ...
end

local function ppcall(...)
  return pcall_ret(pcall(...))
end

local function thread_alive(self)
  local ok, err = self:join(0)
  if ok then return false end
  if err == 'timeout' then return true end
  return nil, err
end

local function parallel_for_impl(code, src, snk, N, cache_size)
  N = N or 4

  zthreads.set_bootstrap_prelude(THREAD_STARTER)

  local src_err, snk_err

  local loop = zloop.new()

  function loop:add_thread(code, ...)
    return zthreads.run(self:context(), code, ... )
  end

  local cache   = {} -- заранее рассчитанные данные для заданий
  local threads = {} -- рабочие потоки

  local MAX_CACHE = cache_size or 1

  local function call_src()
    if src and not src_err then
      local args, err = ppcall(src)
      if args then return args end
      if err then
        src_err = err
        return nil, err
      end
      src = nil
    end
  end

  local function next_src()
    local args = table.remove(cache, 1)
    if args then return args end
    return call_src()
  end

  local function cache_src()
    if #cache >= MAX_CACHE then return end
    for i = #cache, MAX_CACHE do
      local args = call_src()
      if args then cache[#cache + 1] = args else break end
    end
  end

  local skt, err = loop:create_socket{zmq.ROUTER, bind = ENDPOINT}
  zassert(skt, err)

  loop:add_socket(skt, function(skt)
    local identity, tid, cmd, args = skt:recvx()
    zassert(tid, cmd)

    if cmd ~= 'READY' then
      assert(cmd == 'RESP')
      if not snk_err then
        if snk then
          local ok, err = ppcall(snk, mp.unpack(args))
          if not ok and err then snk_err = err end
        end
      else
        skt:sendx(identity, tid, 'END')
        return
      end
    end

    if #cache == 0 then cache_src() end

    local args, err = next_src()

    if args ~= nil then
      skt:sendx(identity, tid, 'TASK', args)
      return
    end

    skt:sendx(identity, tid, 'END')
  end)

  -- watchdog
  loop:add_interval(1000, function()
    local alive = 0
    for _, thread in ipairs(threads) do 
      if thread_alive(thread) then alive = alive + 1 end
    end
    if alive == 0 then loop:interrupt() end
  end)

  loop:add_interval(100, function(ev) cache_src() end)

  for i = 1, N do 
    local thread = loop:add_thread(code)
    thread:start(true, true)
    threads[#threads + 1] = thread
  end

  loop:start()

  loop:destroy()

  for _, t in ipairs(threads) do t:join() end

  if src_err or snk_err then
    return nil, src_err or snk_err
  end

  return true
end

---
-- @tparam[number] be begin index
-- @tparam[number] en end index
-- @tparam[number?] step step
-- @tparam[string] code
-- @tparam[callable?] snk sink
-- @tparam[number?] N thread count
-- @tparam[number?] C cache size
local function For(be, en, step, code, snk, N, C)
  if type(step) ~= 'number' then -- no step
    step, code, snk, N, C = 1, step, code, snk, N
  end

  if type(snk) == 'number' then -- no sink
    N, C = snk, N
  end

  assert(type(be)   == "number")
  assert(type(en)   == "number")
  assert(type(step) == "number")
  assert(type(code) == "string")

  local src = function()
    if be > en then return end
    local n = be
    be = be + step
    return n
  end

  return parallel_for_impl(code, src, snk, N, C)
end

---
-- @tparam[string] code
-- @tparam[callable?] snk sink
-- @tparam[number?] N thread count
-- @tparam[number?] C cache size
local function ForEach(it, code, snk, N, C)
  local src = it
  if type(it) == 'table' then
    local k, v
    src = function()
      k, v = next(it, k)
      return k, v
    end
  end

  if type(snk) == 'number' then -- no sink
    snk, N, C = nil, snk, N
  end

  assert(type(src)  == "function")
  assert(type(code) == "string")

  return parallel_for_impl(code, src, snk, N, C)
end

local Parallel = {} do
Parallel.__index = Parallel

function Parallel:new(N)
  local o = setmetatable({ 
    thread_count = N or 4;
  }, self)
  o.cache_size = o.thread_count * 2
  return o
end

---
-- @tparam[number] be begin index
-- @tparam[number] en end index
-- @tparam[number?] step step
-- @tparam[string] code
-- @tparam[callable?] snk sink
function Parallel:For(be, en, step, code, snk)
  return For(be, en, step, code, snk, self.thread_count, self.cache_size)
end

function Parallel:ForEach(src, code, snk)
  return ForEach(src, code, snk, self.thread_count, self.cache_size)
end

end

return {
  For = For;
  ForEach = ForEach;
  New = function(...) return Parallel:new(...) end
}
