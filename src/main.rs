mod map_reduce;

use getopts::Options;
use std::env;
use std::time::Instant;

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
    opts.optopt("c", "task-size", "donors in map reduce task", "100000");
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
    let donors_in_task = matches
        .opt_str("c")
        .unwrap_or("100000".to_owned())
        .parse::<usize>()
        .unwrap();

    let start_time = Instant::now();
    match map_reduce::run(donors_file_path, donations_file_path, donors_in_task).await {
        Ok(res) => log::info!(
            "map_reduce successfully finished, time elapsed: {:.3?}, result: {:?}",
            start_time.elapsed(),
            res
        ),
        Err(err) => log::error!("map_reduce failed, reason: {}", err),
    }
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    log::info!("{}", opts.usage(&brief));
}
