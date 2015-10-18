local llist = require('ldt/lib_llist');

local LDT_TOP = "ldt-top"
local LDT_TAIL = "ldt-tail"
local LDT_KEY = "key"
local LDT_VALUE = "value"

local function next(rec, counter)
  local currentValue = rec[counter] or 0
  local nextValue = currentValue + 1
  rec[counter] = nextValue
  info("Next "..tostring(counter).." value "..tostring(rec[counter]))
  aerospike:update(rec)
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
  --info("Find Bin "..tostring(bin))
  local keyMap = makeKeyMap(key)
  local result = llist.find(rec, bin, keyMap)
  local firstElement = result[1]
  if firstElement == nil then 
    return nil
  end
  return firstElement[LDT_VALUE]
end

function add(rec, bin, item)
  local top = rec[LDT_TOP] or 0
  if top == 0 then
    next(rec, LDT_TOP)
  end
  local tail = next(rec, LDT_TAIL)
  local value = makeMap(tail, item)
  llist.add(rec, bin, value)
end

function remove(rec, bin)
  local top = rec[LDT_TOP] or 0
  if top == 0 then
    return nil
  end
  --info("Top "..tostring(top))
  local result = find(rec, bin, top)
  if result == nil  then
    return nil
  end
  
  llist.remove(rec, bin, makeKeyMap(top))
  next(rec, LDT_TOP)
  return result
end

function peek(rec, bin)
  local top = rec[LDT_TOP] or 0
  if top == 0 then
    return nil
  end
  local result = find(rec, bin, top)
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