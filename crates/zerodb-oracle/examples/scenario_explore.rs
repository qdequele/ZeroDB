//! Run random scenarios and print each distinct divergence once.
//!
//! `cargo run --release -p zerodb-oracle --example scenario_explore -- [cases] [seed]`

use zerodb_oracle::scenario::{decode_scenario, minimize, run_scenario, Scenario};

fn main() {
    let mut args = std::env::args().skip(1);
    let cases: u64 = args.next().map_or(2000, |a| a.parse().unwrap());
    let mut x: u64 = args
        .next()
        .map_or(0x9e37_79b9_7f4a_7c15, |a| a.parse().unwrap());
    let mut seen = std::collections::BTreeMap::<String, (usize, String)>::new();
    let mut first = std::collections::BTreeMap::<String, Scenario>::new();
    let t0 = std::time::Instant::now();
    for i in 0..cases {
        if i % 200 == 0 {
            eprintln!("{i} cases, {:?}, {} signatures", t0.elapsed(), seen.len());
        }
        let mut data = vec![0u8; 1200];
        for b in &mut data {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        let Ok(s) = decode_scenario(&data) else {
            continue;
        };
        if let Err(d) = run_scenario(&s) {
            let kind = match &s {
                Scenario::Ordered(c) => format!("ordered/{:?}", c.cmp),
                Scenario::Snapshots(_) => "snapshots".into(),
                Scenario::Reopen(_) => "reopen".into(),
                Scenario::MapFull(_) => "mapfull".into(),
            };
            let sig = format!(
                "{kind}: {} | {} vs {}",
                d.what.split(['(', ' ']).next().unwrap_or(""),
                short(&d.lmdb),
                short(&d.zerodb)
            );
            let len = format!("{s:?}").len();
            first.entry(sig.clone()).or_insert_with(|| s.clone());
            let e = seen.entry(sig).or_insert((usize::MAX, String::new()));
            if len < e.0 {
                *e = (len, format!("{d}{s:#?}"));
            }
        }
    }
    for (sig, (_, d)) in &seen {
        println!("=== {sig}\n{d}");
    }
    if std::env::var_os("MINIMIZE").is_some() {
        for (sig, s) in &first {
            let m = minimize(s);
            println!(
                "##### minimized {sig}\n{}\n{m:?}",
                run_scenario(&m).unwrap_err()
            );
        }
    }
    println!("{} distinct divergence signatures", seen.len());
}

fn short(s: &str) -> String {
    s.chars().take(40).collect()
}
