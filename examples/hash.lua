local path     = require "path"
local Parallel = require "Parallel"

local MASK    = [[*.*]]
local THREADS = 4
local RECURSE = true

local HASH_FILE = string.dump(function()
  require "digest"
  local CHUNK_SIZE = 4096
  TASK(function(fname)
    local f, err = io.open(fname,"rb")
    if not f then return fname, nil, err end
    local d = md5.new()
    while true do
      local c = f:read(CHUNK_SIZE)
      if c==nil then break end
      d:update(c)
    end
    f:close()
    return fname, d:digest()
  end)
end)

local function WRITE_HASH(fname, hash, err)
  if hash then
    io.stdout:write( string.lower(hash), ' *', fname, '\n' )
  elseif fname then
    io.stderr:write(fname, ' - ', err, '\n')
  end
end

local FILES do

local function co_reader(fn)
  local sender, err = coroutine.create(function ()
    local writer = function (chunk)
      return coroutine.yield(chunk)
    end
    fn(writer)
  end)
  if not sender then return nil, err end

  local function reader()
    local ok, data = coroutine.resume(sender, true)
    if ok then return data end
    return nil, data
  end

  return reader
end

FILES = function(P)
  return co_reader(function(writer)
    path.each(P, function(...)writer(...)end, {recurse=RECURSE; skipdirs = true})
  end)
end

end

Parallel.ForEach(FILES(MASK), HASH_FILE, WRITE_HASH, THREADS)
