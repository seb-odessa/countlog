use csv::ReaderBuilder;
use csv::StringRecord;
use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::iter::FromIterator;

type Value = (usize, usize);
fn main() {
    if let Some(ref path) = env::args().nth(1) {
        let limit = env::args()
            .nth(2)
            .and_then(|s| s.parse::<f64>().ok()).unwrap_or_default();

        let second = env::args()
            .nth(3)
            .and_then(|s| s.parse::<u32>().ok());



        let mut stats: BTreeMap<String, Value> = BTreeMap::new();

        let mut csv: Vec<u8> = Vec::new();
        let mut file = File::open(&path).expect("Unable to open file");
        let _ = file.read_to_end(&mut csv).expect("Unable to read {path}");

        // let csv = std::fs::read_to_string(file).expect("Should have been able to read the file");

        println!("ok");
        let mut rdr = ReaderBuilder::new()
            .delimiter(b',')
            .flexible(true)
            .has_headers(true)
            .double_quote(true)
            .quoting(true)
            .from_reader(csv.as_slice());

        for result in rdr.records() {
            match result {
                Ok(record) => handle(&mut stats, record, second),
                Err(err) => println!("{err}"),
            }
        }

        let mut v = Vec::from_iter(stats);
        v.sort_by(|&(_, a), &(_, b)| b.cmp(&a));
        let total = v.iter().fold(0, |acc, value| acc + value.1 .0);
        let mut total_percent = 0.0;
        println!(
            "{:>5} | {:>10} | {:>5} | {:>5} | {:>20}",
            "№", "Bytes", "Percent", "Count", "Place"
        );
        for (i, (key, value)) in v.iter().enumerate() {
            let amount = value.0;
            let count = value.1;
            let percent = 100.0 * amount as f64 / total as f64;
            total_percent += percent;
            if limit > percent {
                break;
            }
            println!("{i:>5} | {amount:>10} | {percent:>6.2}% | {count:>5} | {key}");
        }
        println!("Total {total} bytes in {} records", v.len());
        println!("Total {total_percent:.2} percent.");
    }
}

fn handle(stats: &mut BTreeMap<String, Value>, records: StringRecord, second: Option<u32>) {
    let fields: Vec<&str> = records.iter().collect();
    if 13 != fields.len() {
        return;
    }

    if let Some(second) = second {
        if second != fields[2].parse::<f32>().unwrap_or_default().floor() as u32 {
            return;
        }
    }

    let capacity = fields[12].len();
    let mut payload = fields[12].split_whitespace();
    let mut key = payload.nth(1).expect("Key not found");
    if key.starts_with("mt:") {
        key = payload.nth(2).expect("Key not found");
    }

    stats
        .entry(key.to_string())
        .and_modify(|value| {
            value.0 += capacity;
            value.1 += 1
        })
        .or_insert((capacity, 1));
}
