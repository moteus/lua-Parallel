lua-Parallel
============

##Status

I can change anything.

##Usage

```Lua
local T = string.dump

Parallel.For(1, 100, T(function(thread_no)
  TASK(function(i) print(thread_no .. " :" .. i) end)
end))
```

[Parallel hash calculation](/examples/hash.lua)
