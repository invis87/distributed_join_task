# Handmade distributed join

## Input

Two tables:
1. [donations](https://www.kaggle.com/hanselhansel/donorschoose?select=Donations.csv)
2. [donors](https://www.kaggle.com/hanselhansel/donorschoose?select=Donors.csv)

## Output

Result for query:
```sql
SELECT `Donor State`, sum(`Donation Amount`) FROM donors, donations WHERE
donations.`Donor ID` = donors.`Donor ID` GROUP BY `Donor State`
```

## Command options

- `-d | --donors` path to donors csv file
- `-n | --donations` path to donations csv file
- `-c | --task-size` number of donors to process by individual task. Each task read `Donations.csv` file, so the smaller this number are the more times this big file will be read.

## Run

- donwload `Donations.csv` & `Donors.csv`
- `cargo build --release` in project dir
- `{path_to_release_dir}/join_tables -d {path_to_Donors.csv} -n {path_to_Donations.csv} --task-size 100000`
