//! Embedded-Lua cost and behaviour probe for the plugin debate.
//!
//! Loads tools/plugin-bench/transform.lua as a module into an mlua state and
//! measures what a Rust producer host would pay per poll *after* HTTP and
//! JSON/XML decoding: serde_json::Value -> Lua tables, the Lua map+extract
//! call, Lua tables -> serde_json::Value. Then probes failure isolation:
//! Lua errors, memory limits, and instruction-hook interruption of a runaway
//! loop. Prints one JSON object. Microbenchmark only: no network, no Kafka.
//!
//! usage: plugin-bench-mlua-host TRANSFORM_LUA FIXTURES_JSON [REPS] [DUMP_JSONL]

use mlua::{HookTriggers, Lua, LuaSerdeExt, Table, Value, VmState};
use serde_json::{Value as J, json};
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn median(mut xs: Vec<f64>) -> serde_json::Value {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |p: f64| xs[(p * (xs.len() - 1) as f64) as usize];
    json!({"median_us": q(0.5), "min_us": xs[0], "p90_us": q(0.9), "reps": xs.len()})
}

fn vm_kib(field: &str) -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with(field))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn new_state(src: &str) -> mlua::Result<(Lua, Table)> {
    let lua = Lua::new(); // safe subset: no debug/ffi, no C modules
    let m: Table = lua.load(src).set_name("transform.lua").eval()?;
    Ok((lua, m))
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let src = std::fs::read_to_string(&a[1]).expect("transform.lua");
    let fx: J = serde_json::from_str(&std::fs::read_to_string(&a[2]).expect("fixtures.json"))
        .expect("fixtures json");
    let reps: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(300);
    let rss_start = vm_kib("VmRSS:");

    // 1. state creation + module load
    let mut t = Vec::new();
    let mut mem_fresh = 0;
    for _ in 0..200 {
        let t0 = Instant::now();
        let (lua, _m) = new_state(&src).unwrap();
        t.push(t0.elapsed().as_secs_f64() * 1e6);
        mem_fresh = lua.used_memory();
    }
    let state_new = median(t);

    // 2. per-cycle work, split into phases
    let (lua, m) = new_state(&src).unwrap();
    let collect: mlua::Function = m.get("collect").unwrap();
    let (mut tin, mut tcall, mut tout, mut ttotal) = (vec![], vec![], vec![], vec![]);
    let mut last = J::Null;
    for i in 0..(reps + 10) {
        let t0 = Instant::now();
        let v = lua.to_value(&fx).unwrap();
        let t1 = Instant::now();
        let r: Value = collect.call(v).unwrap();
        let t2 = Instant::now();
        let out: J = lua.from_value(r).unwrap();
        let t3 = Instant::now();
        if i >= 10 {
            tin.push((t1 - t0).as_secs_f64() * 1e6);
            tcall.push((t2 - t1).as_secs_f64() * 1e6);
            tout.push((t3 - t2).as_secs_f64() * 1e6);
            ttotal.push((t3 - t0).as_secs_f64() * 1e6);
        }
        last = out;
    }
    let envs = last.as_array().cloned().unwrap_or_default();
    let ent_obj = envs.iter().filter(|e| e["entities"].is_object()).count();
    let ent_arr = envs.iter().filter(|e| e["entities"].is_array()).count();
    let ts_int = envs.iter().filter(|e| e["ts_ms"].is_i64() || e["ts_ms"].is_u64()).count();
    let tags_obj = envs
        .iter()
        .filter(|e| e["payload"].get("tags").is_some_and(|t| t.is_object()))
        .count();
    if let Some(path) = a.get(4) {
        let lines: Vec<String> = envs.iter().map(|e| e.to_string()).collect();
        std::fs::write(path, lines.join("\n") + "\n").unwrap();
    }
    let used_after = lua.used_memory();
    let rss_one = vm_kib("VmRSS:");

    // five plugins -> five isolated states in one process
    let five: Vec<_> = (0..5).map(|_| new_state(&src).unwrap()).collect();
    let rss_six = vm_kib("VmRSS:");
    drop(five);

    // 3. Lua error isolation: error surfaces as Err, state stays usable
    let err = lua.load("error('plugin blew up')").exec();
    let still_ok: i64 = lua.load("return 1 + 1").eval().unwrap_or(-1);

    // 4. memory limit
    let (lim_lua, _) = new_state(&src).unwrap();
    let lim = lim_lua.set_memory_limit(4 << 20);
    let lim_hit = lim_lua
        .load("local t = {} for i = 1, 1e8 do t[i] = ('x'):rep(64) .. i end")
        .exec();

    // 5. runaway loop stopped by an instruction hook + deadline
    let runaway = |code: &'static str| -> serde_json::Value {
        let (tx, rx) = mpsc::channel();
        let src2 = src.clone();
        std::thread::spawn(move || {
            let (lua, _) = new_state(&src2).unwrap();
            let deadline = Instant::now() + Duration::from_millis(200);
            let hooked = lua.set_hook(HookTriggers::new().every_nth_instruction(1000), move |_, _| {
                if Instant::now() > deadline {
                    Err(mlua::Error::runtime("deadline"))
                } else {
                    Ok(VmState::Continue)
                }
            });
            let t0 = Instant::now();
            let r = lua.load(code).exec();
            let _ = tx.send((hooked.is_ok(), r.err().map(|e| e.to_string()), t0.elapsed()));
        });
        match rx.recv_timeout(Duration::from_secs(3)) {
            Ok((hooked, e, d)) => json!({"hook_installed": hooked, "stopped": true,
                "error": e.map(|s| s.lines().next().unwrap_or("").to_string()),
                "elapsed_ms": d.as_secs_f64() * 1e3}),
            Err(_) => json!({"stopped": false, "note": "still running after 3 s; thread abandoned"}),
        }
    };
    let loop_plain = runaway("local x = 0 while true do x = x + 1 end");
    let loop_calls = runaway("local function f(x) return x + 1 end local x = 0 while true do x = f(x) end");

    let flavour = if cfg!(feature = "luajit") { "luajit" } else { "lua54" };
    let version: String = lua.load("return _VERSION .. (jit and (' ' .. jit.version) or '')")
        .eval().unwrap_or_default();
    println!("{}", json!({
        "flavour": flavour, "runtime": version, "mlua": "0.12.2",
        "state_new_plus_module_load": state_new,
        "fresh_state_used_memory_bytes": mem_fresh,
        "cycle_to_lua": median(tin), "cycle_lua_call": median(tcall),
        "cycle_from_lua": median(tout), "cycle_total": median(ttotal),
        "events_per_cycle": envs.len(),
        "envelopes_entities_as_array": ent_arr, "envelopes_entities_as_object": ent_obj,
        "envelopes_ts_ms_json_integer": ts_int, "lobsters_tags_as_object": tags_obj,
        "used_memory_after_bench_bytes": used_after,
        "rss_kib": {"start": rss_start, "after_1_state_bench": rss_one,
                    "after_6_states": rss_six, "hwm": vm_kib("VmHWM:")},
        "lua_error": {"err": err.err().map(|e| e.to_string().lines().next().unwrap_or("").to_string()),
                      "state_usable_after": still_ok == 2},
        "memory_limit": {"set": lim.map(|_| "ok".to_string()).unwrap_or_else(|e| e.to_string()),
                         "alloc_past_limit": lim_hit.err().map(|e| e.to_string()
                             .lines().next().unwrap_or("").to_string())},
        "runaway_tight_loop": loop_plain, "runaway_loop_with_calls": loop_calls,
    }));
    std::process::exit(0); // do not wait on an abandoned runaway thread
}
