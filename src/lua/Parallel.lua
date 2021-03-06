--- Basic parallel for loops
--
-- @module Parallel
--
-- @usage
-- local T = string.dump
--
-- Parallel.For(1, 100, T(function(thread_no)
--   FOR(function(i) print(thread_no .. " :" .. i) end)
-- end))
--
-- Parallel.For(1, 100, T(function(thread_no)
--   FOR(function(i) return thread_no, math.pow(i, 2), math.sqrt(i) end)
-- end), print)
--
-- Parallel.Invoke('print(1)','print(2)','print(3)')
--


local zmq      = require "lzmq"
local zloop    = require "lzmq.loop"
local zthreads = require "lzmq.threads"
local mp       = require "cmsgpack"
local zassert  = zmq.assert

local THREAD_STARTER = string.dump(function(ENDPOINT, code, ...)
  local zmq      = require "lzmq"
  local zthreads = require "lzmq.threads"
  local mp       = require "cmsgpack"
  local zassert  = zmq.assert

  local function arg_unpack(t)
    return mp.unpack(t)
  end

  local function arg_pack(...)
    if select("#", ...) == 0 then
      return ""
    end
    return mp.pack(...)
  end

  function FOR(do_work)
    local ctx      = zthreads.get_parent_ctx()

    local s, err   = ctx:socket{zmq.DEALER, connect = ENDPOINT, linger = 0}
    if not s then return nil, err end

    s:sendx('0', 'READY')
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

      local res, err = arg_pack( do_work(arg_unpack(args)) )
      s:sendx(tid, 'RESP', res)
    end

    ctx:destroy()
  end

  if code:sub(1,1) == '@' then
    return assert(loadfile(code:sub(2)))(...)
  end

  return assert(loadstring(code))(...)
end)

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

local function parallel_for_impl(ctx, code, src, snk, N, cache_size)
  assert(type(code) == "string")

  N = N or 4

  local src_err, snk_err

  local cache   = {} -- заранее рассчитанные данные для заданий
  local threads = {} -- рабочие потоки
  local reqs    = 0
  local ready   = 0

  local MAX_CACHE = cache_size or N

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

  local loop = zloop.new(1, ctx)

  local ENDPOINT = "inproc://parallel.main."

  local skt, err = loop:create_socket{zmq.ROUTER, linger = 0}
  zassert(skt, err)

  ENDPOINT = ENDPOINT .. string.format("%X", skt:fd())
  local ok, err = skt:bind(ENDPOINT)
  if not ok then skt:close() end
  zassert(ok, err)

  loop:add_socket(skt, function(skt)
    local identity, tid, cmd, args = skt:recvx()
    zassert(tid, cmd)

    if cmd == 'READY' then
      ready = ready + 1
    else
      assert(cmd == 'RESP')
      assert(reqs > 0)
      reqs = reqs - 1
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
      reqs = reqs + 1
      return
    end

    skt:sendx(identity, tid, 'END')

    if reqs == 0 then
      -- we have clone of context so ctx:destroy() 
      -- does not destroy real context and we need
      -- send `END` to all threads
      if (not ctx) or (ready == #threads) then loop:interrupt() end
    end
  end)

  -- watchdog
  loop:add_interval(1000, function()
    for _, thread in ipairs(threads) do
      if thread:alive() then return end
    end
    loop:interrupt()
  end)

  loop:add_interval(100, function(ev) cache_src() end)

  local err
  for i = 1, N do 
    local thread
    thread, err = zthreads.run(loop:context(), THREAD_STARTER, ENDPOINT, code, i)
    if thread and thread:start() then
      threads[#threads + 1] = thread
    end
  end
  if #threads == 0 then return nil, err end

  loop:start()

  loop:destroy()

  for _, t in ipairs(threads) do t:join() end

  if src_err or snk_err then
    return nil, src_err or snk_err
  end

  return true
end

local function make_n_iter(be, en, step)
  assert(type(be)   == "number")
  assert(type(en)   == "number")
  assert(type(step) == "number")

  return function()
    local n = be
    if n > en then return end
    be = be + step
    if n > en then return end
    return n
  end
end

local function wrap_iter(it)
  -- @fixme Use __call/__pairs metamethods?

  if type(it) == 'table' then
    local k, v
    return function()
      k, v = next(it, k)
      return k, v
    end
  end

  assert(it) -- `it` must be callable
  return it
end

--- Implement for loop
--
-- @tparam number beginIndex begin index (inclusive)
-- @tparam number endIndex end index (inclusive)
-- @tparam[opt] number step step
-- @tparam string body it may be Lua compiled or raw chunk
-- @tparam[opt] callable sink this function callect all returned values from each iteration
-- @tparam[opt] number N thread count
-- @tparam[opt] number C cache size
local function For(be, en, step, code, snk, N, C)
  if type(step) ~= 'number' then -- no step
    assert(C == nil)
    step, code, snk, N, C = 1, step, code, snk, N
  end

  if type(snk) == 'number' then -- no sink
    assert(C == nil)
    snk, N, C = nil, snk, N
  end

  return parallel_for_impl(nil, code, make_n_iter(be, en, step), snk, N, C)
end

--- Implement iterator loop
--
-- @tparam [table | iterator] iteration range
-- @tparam string body it may be Lua compiled or raw chunk
-- @tparam[opt] callable sink this function callect all returned values from each iteration
-- @tparam[opt] number N thread count
-- @tparam[opt] number C cache size
local function ForEach(it, code, snk, N, C)
  if type(snk) == 'number' then -- no sink
    assert(C == nil)
    snk, N, C = nil, snk, N
  end

  return parallel_for_impl(nil, code, wrap_iter(it), snk, N, C)
end

local function Invoke_impl(ctx, N, ...)
  local code = string.dump(function() FOR(function(_,src)
    if src:sub(1,1) == '@' then dofile((src:sub(2)))
    else assert((loadstring or load)(src))() end
  end) end)
  
  if type(N) == 'number' then
    return ForEach({...}, code, N)
  end
  return ForEach_impl(ctx, {N, ...}, code)
end

--- Implement parallel invokation of lua chunks
--
-- @tparam string,... body it may be Lua compiled or raw chunk
local function Invoke(...)
  return Invoke_impl(nil, ...)
end

--- Implement `Parallel` class
--
local Parallel = {} do
Parallel.__index = Parallel

function Parallel:new(N, C)
  local o = setmetatable({
    thread_count = N or 4;
    context = zassert(zmq.context());
  }, self)
  o.cache_size = C or o.thread_count * 2
  
  return o
end

function Parallel:For(be, en, step, code, snk)
  if type(step) ~= 'number' then -- no step
    assert(snk == nil)
    step, code, snk = 1, step, code
  end

  return parallel_for_impl(self.context, code, make_n_iter(be, en, step), snk, self.thread_count, self.cache_size)
end

function Parallel:ForEach(it, code, snk)
  return parallel_for_impl(self.context, code, wrap_iter(it), snk, self.thread_count, self.cache_size)
end

function Parallel:Invoke(...)
  return Invoke(self.thread_count, ...)
end

function Parallel:destroy(...)
  self.context:destroy()
  self.context = nil
  return true
end

end

return {
  For     = For;
  ForEach = ForEach;
  Invoke  = Invoke;
  New     = function(...) return Parallel:new(...) end
}
