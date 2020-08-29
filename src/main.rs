mod map_reduce;

use anyhow::anyhow;
use getopts::Options;
use std::env;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

type Result<T> = std::result::Result<T, anyhow::Error>;

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "join_tables=debug");
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.reqopt(
        "d",
        "donors",
        "path to csv file with donors",
        "/Users/someone/Donors.csv",
    );
    opts.reqopt(
        "n",
        "donations",
        "path to csv file with donations",
        "/Users/someone/Donations.csv",
    );
    opts.optflag("h", "help", "print this help menu");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            log::error!("{}", f.to_string());
            print_usage(&program, opts);
            return;
        }
    };
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let donors_file_path = matches.opt_str("d").unwrap();
    let donations_file_path = matches.opt_str("n").unwrap();

    match map_reduce::run(donors_file_path, donations_file_path).await {
        Ok(_) => log::info!("map_reduce successfully returned: {}", 25),
        Err(err) => log::error!("map_reduce failed, reason: {}", err),
    }
}

// Donations
// Project ID,Donation ID,Donor ID,Donation Included Optional Donation,Donation Amount,Donor Cart Sequence
//
//
// Donors
// Donor ID,Don City,Donor State,Donor Is Teacher,Donor Zip
//
// `select `Donor State`, sum(`Donation Amount`) from donors, donations where donations.`Donor ID` = donors.`Donor ID` group by `Donor State``
//
//
// state | amount

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    log::info!("{}", opts.usage(&brief));
}
