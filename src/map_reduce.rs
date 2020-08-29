use anyhow::anyhow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::time::Instant;
use tokio::sync::mpsc;

use crate::Result;

pub async fn run<T: AsRef<str>>(
    donors_path: T,
    donations_path: T,
    donors_in_task: usize,
) -> Result<DonationsByState> {
    log::info!("Start map_reduce task");
    let start_time = Instant::now();

    let (tx, mut rx) = mpsc::unbounded_channel();
    let donors_file = File::open(donors_path.as_ref())?;
    let mut donors_lines = BufReader::new(donors_file).lines();

    let donors_head = donors_lines
        .next()
        .ok_or(anyhow!("no lines in donors file"))??;
    let (donor_id_index, donor_state_index) = get_indexes("Donor ID", "Donor State", donors_head)?;

    let mut tasks_size = 0;
    let mut donors_buffer: HashMap<String, String> = HashMap::with_capacity(donors_in_task);
    for donor_line in donors_lines {
        let donor_info = DonorInfo::from_line(donor_id_index, donor_state_index, donor_line?);
        donors_buffer.insert(donor_info.id, donor_info.state);

        if donors_buffer.len() == donors_in_task {
            tasks_size += 1;

            let tx_clone = tx.clone();
            let donations_path_clone = donations_path.as_ref().to_string();
            let moved_buffer =
                std::mem::replace(&mut donors_buffer, HashMap::with_capacity(donors_in_task));
            tokio::spawn(async move {
                match map_task(moved_buffer, donations_path_clone) {
                    Ok(map_result) => {
                        match tx_clone.send(map_result) {
                            Ok(_) => {}
                            Err(err) => {
                                log::error!("fail to send result to channel, reason: {}", err)
                            }
                        };
                    }
                    Err(err) => log::error!("map_task failed, reason: {}", err),
                }
            });
        }
    }
    log::debug!("creating tasks took: {:.3?}", start_time.elapsed());

    let mut result = DonationsByState::new();
    for task_index in 0..tasks_size {
        let task_response = rx.recv().await;
        if let Some(task_result) = task_response {
            log::debug!("receive 'map' task result #{}", task_index,);
            result += task_result;
        } else {
            log::error!("for some reason task return None result");
        }
    }

    Ok(result)
}

fn map_task(
    donor_id_to_state: HashMap<String, String>,
    donations_file_path: String,
) -> Result<DonationsByState> {
    let donations_file = File::open(donations_file_path)?;
    let mut donations_lines = BufReader::new(donations_file).lines();

    let donations_head = donations_lines
        .next()
        .ok_or(anyhow!("no lines in donations file"))??;
    let (donor_id_index, donation_amount_index) =
        get_indexes("Donor ID", "Donation Amount", donations_head)?;
    let mut donations_by_state: HashMap<String, f32> = HashMap::with_capacity(51);
    for donation_line in donations_lines {
        if let Some(donation_info) =
            DonationInfo::from_line(donor_id_index, donation_amount_index, donation_line?)
        {
            if let Some(state) = donor_id_to_state.get(&donation_info.donor_id) {
                if let Some(donation) = donations_by_state.get_mut(state) {
                    *donation += donation_info.amount;
                } else {
                    donations_by_state.insert(state.clone(), donation_info.amount);
                }
            }
        }
    }

    Ok(DonationsByState {
        data: donations_by_state,
    })
}

#[derive(Debug)]
pub struct DonationsByState {
    data: HashMap<String, f32>,
}

impl DonationsByState {
    fn new() -> Self {
        DonationsByState {
            data: HashMap::with_capacity(51),
        }
    }
}

impl std::ops::AddAssign for DonationsByState {
    fn add_assign(&mut self, other: Self) {
        for (k, v) in other.data.iter() {
            if let Some(amount) = self.data.get_mut(k) {
                //assume overflow is impossible
                *amount += v;
            } else {
                self.data.insert(k.clone(), *v);
            }
        }
    }
}

fn get_indexes(
    first_col_name: &str,
    second_col_name: &str,
    header: String,
) -> Result<(usize, usize)> {
    let mut first_index = None;
    let mut second_index = None;
    for (i, header) in header.split(',').enumerate() {
        if header == first_col_name {
            first_index = Some(i);
        }

        if header == second_col_name {
            second_index = Some(i);
        }
    }

    if let Some(first_index) = first_index {
        if let Some(second_index) = second_index {
            return Ok((first_index, second_index));
        }
    }

    Err(anyhow!(
        "fail to find '{}' or '{}' in donors header",
        first_col_name,
        second_col_name
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

struct DonationInfo {
    donor_id: String,
    amount: f32,
}

impl DonationInfo {
    fn from_line(id_index: usize, amount_index: usize, line: String) -> Option<Self> {
        let columns: Vec<&str> = line.split(',').collect();
        let amount_str = columns[amount_index].to_string();
        let amount = amount_str.parse::<f32>();

        match amount {
            Ok(amount) => Some(DonationInfo {
                donor_id: columns[id_index].to_string(),
                amount,
            }),
            Err(_) => None,
        }
    }
}
