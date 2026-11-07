-- Lua port of the five plugins' transform step, for comparison only.
--
-- Input is fixtures.lua: upstream responses ALREADY DECODED into tables, the
-- shape a Rust host would hand an embedded plugin after serde_json/quick-xml.
-- So this measures map + entity extraction only: no HTTP, no JSON/XML parse,
-- no JSON Schema check (Lua's stdlib has none of those; in a Rust port they
-- would be host code). Runs on Lua 5.4 and LuaJIT 2.1 (Lua 5.1 dialect).
--
--   lua transform.lua fixtures.lua once            one cycle, exit
--   lua transform.lua fixtures.lua bench [REPS]     timing JSON on stdout
--   lua transform.lua fixtures.lua dump             envelopes as JSON lines
--   lua transform.lua - extract < corpus.txt        extractor outputs per line

local floor = math.floor

-- ---------------------------------------------------------------- extractors
-- Python: re.compile(r"\$([A-Za-z]{1,5})\b"). Lua patterns have no \b or
-- {m,n}; take the whole letter run and reject it if longer than 5 or followed
-- by a word char. %a/%w are locale-dependent and ASCII-only in the C locale,
-- unlike Python's Unicode \b (see extract_diff.py for the divergences).
local function cashtags(text)
  local out = {}
  for run, after in string.gmatch(text, "%$(%a+)()") do
    local nxt = text:sub(after, after)
    if #run <= 5 and not nxt:match("[%w_]") then out[#out + 1] = run end
  end
  return out
end

local function dedupe_upper(list)
  local seen, out = {}, {}
  for _, t in ipairs(list) do
    local u = t:upper()
    if not seen[u] then seen[u] = true; out[#out + 1] = u end
  end
  return out
end

local function extract_entities(text) return dedupe_upper(cashtags(text)) end

local STOP = {}
for w in ([[A AN THE AND OR BUT IF OF AT BY FOR IN ON TO FROM WITH IS ARE WAS WERE BE
  BEEN BEING HAVE HAS HAD DO DOES DID CAN COULD SHOULD WOULD WILL SHALL MAY MIGHT MUST
  NOT NO SO ALL ANY SOME MORE MOST OTHER INTO OVER AFTER THEN NOW JUST ALSO HOW OUT UP
  DOWN BUY SELL HOLD NEWS TODAY WEEK YEAR]]):gmatch("%S+") do STOP[w] = true end

local STRIP = "[%$%.,;%(%)%[%]\"']"
local function strip(s)
  local i, j = 1, #s
  while i <= j and s:sub(i, i):match(STRIP) do i = i + 1 end
  while j >= i and s:sub(j, j):match(STRIP) do j = j - 1 end
  return s:sub(i, j)
end

local function extract_stocks(text)
  local tags = cashtags(text)
  if #tags > 0 then return dedupe_upper(tags) end
  local seen, out = {}, {}
  for token in text:gmatch("[^%s,;]+") do
    local c = strip(token):upper()
    if #c >= 1 and #c <= 5 and c:match("^%u+$") and not STOP[c] and not seen[c] then
      seen[c] = true; out[#out + 1] = c
    end
  end
  return out
end

-- Python: ^[A-Z][A-Z.\-]{0,9}$ on each whitespace token, uppercased
local function extract_symbol(text)
  local seen, out = {}, {}
  for token in text:gmatch("%S+") do
    local u = token:upper()
    if #u <= 10 and u:match("^%u[%u%.%-]*$") and not seen[u] then
      seen[u] = true; out[#out + 1] = u
    end
  end
  return out
end

-- core: cli.entity_text + cli.normalize_entities
local function entity_text(fields, payload)
  local parts = {}
  for _, f in ipairs(fields) do
    local v = payload[f]
    if v ~= nil then parts[#parts + 1] = tostring(v) end
  end
  return table.concat(parts, " ")
end

local function normalize(list)
  local seen, out = {}, {}
  for _, e in ipairs(list) do
    if type(e) == "string" then
      e = e:match("^%s*(.-)%s*$")
      if e ~= "" and not seen[e] then seen[e] = true; out[#out + 1] = e end
    end
  end
  return out
end

-- ---------------------------------------------------------------- time
local function days_from_civil(y, m, d)
  if m <= 2 then y = y - 1 end
  local era = floor(y / 400)
  local yoe = y - era * 400
  local mp = m > 2 and m - 3 or m + 9
  local doy = floor((153 * mp + 2) / 5) + d - 1
  local doe = yoe * 365 + floor(yoe / 4) - floor(yoe / 100) + doy
  return era * 146097 + doe - 719468
end

local MON = {Jan=1, Feb=2, Mar=3, Apr=4, May=5, Jun=6, Jul=7, Aug=8, Sep=9, Oct=10,
             Nov=11, Dec=12}

local function now_ms() return os.time() * 1000 end

-- RFC 2822 subset: [Day, ] DD Mon YYYY HH:MM[:SS] (GMT|UT|Z|+hhmm|-hhmm).
-- email.utils.parsedate_to_datetime accepts more (obsolete zone names, 2-digit
-- years); anything else falls back to "now", as the adapters do on error.
local function rfc2822_ms(s)
  if not s then return now_ms() end
  local d, mon, y, H, M, S, z =
    s:match("(%d+)%s+(%a+)%s+(%d+)%s+(%d+):(%d+):?(%d*)%s*(%S*)")
  if not d or not MON[mon] then return now_ms() end
  local off = 0
  if z ~= "" and z ~= "GMT" and z ~= "UT" and z ~= "Z" then
    local sign, hh, mm = z:match("^([+-])(%d%d)(%d%d)$")
    if not sign then return now_ms() end
    off = (tonumber(hh) * 3600 + tonumber(mm) * 60) * (sign == "-" and -1 or 1)
  end
  local secs = days_from_civil(tonumber(y), MON[mon], tonumber(d)) * 86400
    + tonumber(H) * 3600 + tonumber(M) * 60 + (tonumber(S) or 0) - off
  return secs * 1000
end

local function iso_ms(s)
  local y, mo, d, H, M, S, frac, sign, oh, om =
    s:match("^(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)(%.?%d*)([Zz+-]?)(%d*):?(%d*)$")
  if not y then error("bad iso time " .. s) end
  local off = 0
  if sign == "+" or sign == "-" then
    off = (tonumber(oh) * 3600 + tonumber(om) * 60) * (sign == "-" and -1 or 1)
  end
  local secs = days_from_civil(tonumber(y), tonumber(mo), tonumber(d)) * 86400
    + tonumber(H) * 3600 + tonumber(M) * 60 + tonumber(S) - off
  local f = frac ~= "" and frac ~= "." and tonumber("0" .. frac) or 0
  return floor((secs + f) * 1000)
end

-- ---------------------------------------------------------------- maps
local function env(id, source, ts, payload)
  return {event_id = id, source = source, entities = {}, ts_ms = ts, payload = payload}
end

local function by_ts(a, b) return a.ts_ms < b.ts_ms end

local MAP = {}
function MAP.rss(items)
  local out = {}
  for _, i in ipairs(items) do
    out[#out + 1] = env("rss:" .. (i.guid or i.link), "rss", rfc2822_ms(i.pubDate),
                        {title = i.title, link = i.link})
  end
  return out
end
function MAP.hn(items)
  local out = {}
  for _, i in ipairs(items) do
    if i.type == "story" then
      out[#out + 1] = env("hn:" .. i.id, "hn", i.time * 1000,
                          {title = i.title, url = i.url, score = i.score})
    end
  end
  return out
end
function MAP.stocks(charts)
  local out = {}
  for _, c in ipairs(charts) do
    local meta = c.chart.result[1].meta
    local ts = floor(meta.regularMarketTime * 1000)
    out[#out + 1] = env("stocks:" .. meta.symbol .. ":" .. string.format("%d", ts), "stocks",
                        ts, {symbol = meta.symbol, price = meta.regularMarketPrice + 0.0,
                             currency = meta.currency or "USD"})
  end
  return out
end
function MAP.news(by_sym, symbols)
  local out = {}
  for _, sym in ipairs(symbols) do
    for _, i in ipairs(by_sym[sym]) do
      out[#out + 1] = env("news:" .. sym .. ":" .. (i.guid or i.link), "news",
                          rfc2822_ms(i.pubDate), {symbol = sym, title = i.title, link = i.link})
    end
  end
  table.sort(out, by_ts)  -- NB: table.sort is not stable; Python's sort is
  return out
end
function MAP.lobsters(stories)
  local out = {}
  for _, s in ipairs(stories) do
    local url = s.url
    if url == "" then url = nil end
    out[#out + 1] = env("lobsters:" .. s.short_id, "lobsters", iso_ms(s.created_at),
                        {title = s.title, url = url, score = s.score, tags = s.tags or {}})
  end
  table.sort(out, by_ts)
  return out
end

local PLUG = {  -- from_fields and extractor per plugin.toml / adapter.register
  hn = {{"title", "url"}, extract_entities}, lobsters = {{"title", "url"}, extract_entities},
  news = {{"symbol"}, extract_symbol}, rss = {{"title", "link"}, extract_entities},
  stocks = {{"symbol"}, extract_stocks},
}
local ORDER = {"hn", "lobsters", "news", "rss", "stocks"}

local function cycle(data, sink)
  local n = 0
  for _, name in ipairs(ORDER) do
    local fields, ex = PLUG[name][1], PLUG[name][2]
    for _, e in ipairs(MAP[name](data[name], data.symbols)) do
      e.entities = normalize(ex(entity_text(fields, e.payload)))
      n = n + 1
      if sink then sink(e) end
    end
  end
  return n
end

-- ---------------------------------------------------------------- json out
local function enc(v)
  local t = type(v)
  if t == "nil" then return "null" end
  if t == "boolean" then return tostring(v) end
  if t == "number" then
    if math.type and math.type(v) == "float" and v == floor(v) and math.abs(v) < 2^53 then
      return string.format("%.1f", v)  -- Lua 5.4 keeps the float subtype; LuaJIT has none
    end
    if v == floor(v) and math.abs(v) < 2^53 then return string.format("%d", v) end
    return string.format("%.17g", v)
  end
  if t == "string" then
    return '"' .. v:gsub('[%c"\\]', function(c)
      return string.format("\\u%04x", c:byte()) end) .. '"'
  end
  if #v > 0 or next(v) == nil then
    local p = {}
    for i = 1, #v do p[i] = enc(v[i]) end
    return "[" .. table.concat(p, ",") .. "]"
  end
  local keys = {}
  for k in pairs(v) do keys[#keys + 1] = k end
  table.sort(keys)
  local p = {}
  for _, k in ipairs(keys) do p[#p + 1] = enc(k) .. ":" .. enc(v[k]) end
  return "{" .. table.concat(p, ",") .. "}"
end

-- ---------------------------------------------------------------- module
-- Embedded (mlua-host): no `arg` global, so hand back the functions instead.
local M = {cycle = cycle, extract_entities = extract_entities,
           extract_stocks = extract_stocks, extract_symbol = extract_symbol}
function M.collect(data)
  local out = {}
  cycle(data, function(e) out[#out + 1] = e end)
  return out
end
if type(arg) ~= "table" then return M end

-- ---------------------------------------------------------------- main
local path, mode = arg[1], arg[2] or "once"

if mode == "extract" then
  for line in io.lines() do
    print(enc({extract_entities(line), extract_stocks(line), extract_symbol(line)}))
  end
  return
end

local data = dofile(path)

if mode == "once" then
  cycle(data)
  local f = io.open("/proc/self/status")  -- VmHWM for spawn.py
  if f then io.write(f:read("*a")); f:close() end
elseif mode == "dump" then
  cycle(data, function(e) print(enc(e)) end)
elseif mode == "bench" then
  local reps = tonumber(arg[3] or "300")
  local batch = 20
  collectgarbage("collect")
  local heap_before_kib = collectgarbage("count")
  for _ = 1, 10 do cycle(data) end
  local per = {}
  for r = 1, reps do
    local t0 = os.clock()
    for _ = 1, batch do cycle(data) end
    per[r] = (os.clock() - t0) / batch * 1e6
  end
  table.sort(per)
  local function q(p) return per[floor(p * (reps - 1)) + 1] end
  print(enc({
    runtime = _VERSION .. (jit and (" " .. jit.version) or ""),
    events_per_cycle = cycle(data),
    clock = "os.clock (process CPU time), batches of 20 cycles",
    map_only_extract_from_decoded = {median_us = q(0.5), min_us = per[1], p90_us = q(0.9),
                                     reps = reps},
    heap_kib_after_fixture_load = heap_before_kib,
    heap_kib_after_bench_uncollected = collectgarbage("count"),
  }))
end
