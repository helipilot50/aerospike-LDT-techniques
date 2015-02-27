--require("debugger")("127.0.0.1", 10001)
local llist = require('ldt/lib_llist');

local LDT_TOP = "ldt-top"
local LDT_TAIL = "ldt-tail"
local LDT_KEY = "key"
local LDT_VALUE = "value"

local function next(rec, counter)
  local currentValue = rec[counter] or 0
  local nextValue = currentValue + 1
  rec[counter] = nextValue
  return nextValue
end

local function makeMap(key, value)
  local valueMap = map()
  valueMap[LDT_KEY] = key
  valueMap[LDT_VALUE] = value
  return valueMap
end

local function makeKeyMap(key)
  local keyMap = map()
  keyMap[LDT_KEY] = key
  return keyMap
end

local function find(rec, bin, key)
  local keyMap = makeKeyMap(key)
  local result = llist.find(rec, bin, keyMap)
  local firstElement = result[1]
  info(tostring(firstElement))
  return firstElement[LDT_VALUE]
end

function add(rec, bin, item)
  local tail = next(rec, LDT_TAIL)
  local value = makeMap(tail, item)
  llist.add(rec, bin, value)
  aerospike:update(rec)
end

function remove(rec, bin)
  local top = rec[LDT_TOP] or 0
  local result = find(rec, bin, top)
  llist.remove(makeKeyMap(top))
  next(rec, LDT_TOP)
  aerospike:update(rec)
  return result
end

function peek(rec, bin)
  local top = rec[LDT_TOP] or 0
  local result = find(rec, bin, top-1)
  return result
end

function size(rec, bin)
  local result = 0
  if rec[bin]~= nil then
    result = llist.size(rec, bin)
  end
  return result
end

function isEmpty(rec, bin)
  local result = llist.size(rec, bin) == 0
  return result
end

function clear(rec, bin)
  llist.destroy(rec, bin)
end