-- @todo implement for platform different from Windows

local Parallel = require "Parallel"

local NET = "192.168.123."
local THREADS = 10

function NextIP(NET, PACKETS)
  local CUR = 0
  return function()
    if CUR > 254 then return end
    CUR = CUR + 1
    return NET .. CUR, PACKETS
  end
end

function PingDone(host, n, err)
  if n then
    print(host, n)
  elseif err then
    print(host, 'ERROR', err)
  end
end

local PING_THREAD = string.dump(function()
  local function exec(...)
    local f, err = io.popen(...)
    if not f then return nil, err end
    local res = f:read("*a")
    f:close()
    return res
  end

  local function WinPing(strHost, numPing, packetSize)
    numPing    = tonumber(numPing) or 1
    packetSize = tonumber(packetSize) and ("-l " .. packetSize) or ""
    local strCmdLine = "ping" .. " -n " .. tostring(numPing)  .. " " .. strHost

    local Pattern = "([0-9]*[0-9]*[0-9]+%.[0-9]*[0-9]*[0-9]+%.[0-9]*[0-9]*[0-9]+%.[0-9]*[0-9]*[0-9]+%: .*TTL=)"

    local res, err = exec(strCmdLine)
    if not res then return strHost, nil, err end

    local retAlive = 0
    for str in string.gfind(res, "([^\n]*)[\n]")  do
      if string.find(str, Pattern) then
         retAlive = retAlive + 1
      end
    end
    return retAlive
  end

  FOR(function(ip, n) return ip, WinPing(ip, n) end)
end)

Parallel.ForEach(NextIP(NET), PING_THREAD, PingDone, THREADS)
