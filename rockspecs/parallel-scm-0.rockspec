package = "Parallel"
version = "scm-0"

source = {
  url = "https://github.com/moteus/lua-Parallel/archive/master.zip",
  dir = "lua-Parallel-master",
}

description = {
  summary  = "Basic parallel loops",
  homepage = "https://github.com/moteus/lua-Parallel",
  license = "MIT/X11",
}

dependencies = {
  "lua >= 5.1, < 5.3",
  "lua-llthreads2", -- or "lua-llthreads2-compat"
  "lua-cmsgpack",
  -- "lzmq" or "lzmq-ffi"
}

build = {
  copy_directories = {"test", "examples"},

  type = "builtin",

  modules = {
    Parallel = "src/lua/Parallel.lua";
  }
}
