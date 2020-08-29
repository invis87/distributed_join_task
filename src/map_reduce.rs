use anyhow::anyhow;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::time::Instant;
use tokio::sync::mpsc;

use crate::Result;

pub async fn run<T: AsRef<str>>(donors_path: T, donations_path: T) -> Result<()> {
    log::info!("Start map_reduce task");
    let start_time = Instant::now();

    let (mut tx, mut rx) = mpsc::unbounded_channel();
    let donors_in_task = 10_000; //TODO: move to arguments
    let donors = File::open(donors_path.as_ref())?;
    let mut donors_lines = BufReader::new(donors).lines();

    let donors_head = donors_lines
        .next()
        .ok_or(anyhow!("no lines in donors file"))??;
    let (donor_id_index, donor_state_index) = get_donors_indexes(donors_head)?;

    let mut tasks_size = 0;
    let mut donors_buffer = Vec::with_capacity(donors_in_task);
    for donor_line in donors_lines {
        let donor_info = DonorInfo::from_line(donor_id_index, donor_state_index, donor_line?);
        donors_buffer.push(donor_info);

        if donors_buffer.len() == donors_in_task {
            tasks_size += 1;
            donors_buffer.clear();

            let tx_clone = tx.clone();
            tokio::spawn(async move {
                match tx_clone.send(22) {
                    Ok(_) => {}
                    Err(err) => log::error!("fail to send result to channel, reason: {}", err),
                };
            });
        }
    }
    log::debug!("creating tasks took: {:.3?}", start_time.elapsed());

    let mut result = 0;
    for _ in 0..tasks_size {
        let task_response = rx.recv().await;
        if let Some(task_result) = task_response {
            result += task_result;
        } else {
            log::error!("for some reason task return None result");
        }
    }

    log::info!("tasks size: {}", tasks_size);
    log::info!("Result: {}", result);

    Ok(())
}

fn get_donors_indexes(header: String) -> Result<(usize, usize)> {
    let donor_id = "Donor ID";
    let donor_state = "Donor State";
    let mut donor_id_index = None;
    let mut donor_state_index = None;
    for (i, header) in header.split(',').enumerate() {
        if header == donor_id {
            donor_id_index = Some(i);
        }

        if header == donor_state {
            donor_state_index = Some(i);
        }
    }

    if let Some(donor_id_index) = donor_id_index {
        if let Some(donor_state_index) = donor_state_index {
            return Ok((donor_id_index, donor_state_index));
        }
    }

    Err(anyhow!(
        "fail to find '{}' or '{}' in donors header",
        donor_id,
        donor_state
    ))
}

struct DonorInfo {
    id: String,
    state: String,
}

impl DonorInfo {
    fn from_line(id_index: usize, state_index: usize, line: String) -> Self {
        let columns: Vec<&str> = line.split(',').collect();
        DonorInfo {
            id: columns[id_index].to_string(),
            state: columns[state_index].to_string(),
        }
    }
}
