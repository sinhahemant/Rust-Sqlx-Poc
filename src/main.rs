mod db;
mod models;
mod schema;

use polars::prelude::*;
use std::error::Error;
use crate::models::YourModel;
use std::io::BufReader;
use std::fs::File;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    // Create database connection
    let db = db::Database::new().await?;

    // Ensure table schema exists
    schema::create_table(db.get_pool()).await?;

    // Read data from CSV for bulk insert
    let start1 = Instant::now();
    let file_path = "input.csv";
    let new_models = read_csv_to_new_models(file_path)?;
    db::Database::insert_data(&db.pool, new_models).await?;
    let end1 = Instant::now();

    let duration1 = end1.duration_since(start1);

    println!("Bulk insert completed: {:?}", duration1);

    // Read data from CSV for bulk update
    let start2 = Instant::now();
    let update_file_path = "update.csv";
    let updated_models = read_csv_to_new_models(update_file_path)?;
    for updated_model in updated_models {
        db::Database::update_data(&db.pool, &updated_model).await?;
    }
    let end2 = Instant::now();

    let duration2 = end2.duration_since(start2);

    println!("Bulk update completed: {:?}", duration2);

    // Select data from the database
    let start3 = Instant::now();
    let selected_data = db::Database::select_data(&db.pool, "id", "0D40DC3C-A7F0-CF43-7323-D682496436DB-0001").await?;
    for data in selected_data {
        println!("selected data{:?}", data);
    }
    let end3 = Instant::now();

    let duration3 = end3.duration_since(start3);

    println!("Select completed: {:?}", duration3);

    let end = Instant::now();

    let duration = end.duration_since(start);

    println!("Total time elapsed: {:?}", duration);

    Ok(())
}

fn read_csv_to_new_models(file_path: &str) -> Result<Vec<YourModel>, Box<dyn std::error::Error>> {
    // Read CSV file and convert to NewYourModel
    let reader = BufReader::new(File::open(file_path).expect("Failed to open file"));
    let df = CsvReader::new(reader)
        .infer_schema(Some(100))
        .has_header(true)
        .finish()
        .unwrap();

    
    // Perform bulk insert
    let mut new_models = Vec::new();
    for row in 0..df.height() {
        let id = df.column("id").unwrap().str().unwrap().get(row).unwrap_or_else(|| "Id not found").to_string();
        let uid = df.column("uid").unwrap().str().unwrap().get(row).unwrap_or_else(|| "Uid not found").to_string();
        let adjdate = df.column("adjdate").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjdate not found").to_string();
        let adjtype = df.column("adjtype").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjtype not found").to_string();
        let remitter = df.column("remitter").unwrap().str().unwrap().get(row).unwrap_or_else(|| "remitter not found").to_string();
        let beneficiery = df.column("beneficiery").unwrap().str().unwrap().get(row).unwrap_or_else(|| "beneficiery not found").to_string();
        let response = df.column("response").unwrap().str().unwrap().get(row).unwrap_or_else(|| "response not found").to_string();
        let txndate = df.column("txndate").unwrap().str().unwrap().get(row).unwrap_or_else(|| "txndate not found").to_string();
        let txntime = df.column("txntime").unwrap().str().unwrap().get(row).unwrap_or_else(|| "txntime not found").to_string();
        let rrn = df.column("rrn").unwrap().str().unwrap().get(row).unwrap_or_else(|| "rrn not found").to_string();
        let terminalid = df.column("terminalid").unwrap().str().unwrap().get(row).unwrap_or_else(|| "terminalid not found").to_string();
        let ben_mobile_no = df.column("ben_mobile_no").unwrap().str().unwrap().get(row).unwrap_or_else(|| "ben_mobile_no not found").to_string();
        let rem_mobile_no = df.column("rem_mobile_no").unwrap().str().unwrap().get(row).unwrap_or_else(|| "rem_mobile_no not found").to_string();
        let chbdate = df.column("chbdate").unwrap().str().unwrap().get(row).unwrap_or_else(|| "chbdate not found").to_string();
        let chbref = df.column("chbref").unwrap().str().unwrap().get(row).unwrap_or_else(|| "chbref not found").to_string();
        let txnamount = df.column("txnamount").unwrap().str().unwrap().get(row).unwrap_or_else(|| "txnamount not found").to_string();
        let adjamount = df.column("adjamount").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjamount not found").to_string();
        let rem_payee_psp_fee = df.column("rem_payee_psp_fee").unwrap().str().unwrap().get(row).unwrap_or_else(|| "rem_payee_psp_fee not found").to_string();
        let ben_fee = df.column("ben_fee").unwrap().str().unwrap().get(row).unwrap_or_else(|| "ben_fee not found").to_string();
        let ben_fee_sw = df.column("ben_fee_sw").unwrap().str().unwrap().get(row).unwrap_or_else(|| "ben_fee_sw not found").to_string();
        let adjfee = df.column("adjfee").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjfee not found").to_string();
        let npcifee = df.column("npcifee").unwrap().str().unwrap().get(row).unwrap_or_else(|| "npcifee not found").to_string();
        let remfeetax = df.column("remfeetax").unwrap().str().unwrap().get(row).unwrap_or_else(|| "remfeetax not found").to_string();
        let benfeetax = df.column("benfeetax").unwrap().str().unwrap().get(row).unwrap_or_else(|| "benfeetax not found").to_string();
        let npcitax = df.column("npcitax").unwrap().str().unwrap().get(row).unwrap_or_else(|| "npcitax not found").to_string();
        let adjref = df.column("adjref").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjref not found").to_string();
        let bankadjref = df.column("bankadjref").unwrap().str().unwrap().get(row).unwrap_or_else(|| "bankadjref not found").to_string();
        let adjproof = df.column("adjproof").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjproof not found").to_string();
        let compensation_amount = df.column("compensation_amount").unwrap().str().unwrap().get(row).unwrap_or_else(|| "compensation_amount not found").to_string();
        let adjustment_raised_time = df.column("adjustment_raised_time").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjustment_raised_time not found").to_string();
        let no_of_days_for_penalty = df.column("no_of_days_for_penalty").unwrap().str().unwrap().get(row).unwrap_or_else(|| "no_of_days_for_penalty not found").to_string();
        let shdt73 = df.column("shdt73").unwrap().str().unwrap().get(row).unwrap_or_else(|| "shdt73 not found").to_string();
        let shdt74 = df.column("shdt74").unwrap().str().unwrap().get(row).unwrap_or_else(|| "shdt74 not found").to_string();
        let shdt75 = df.column("shdt75").unwrap().str().unwrap().get(row).unwrap_or_else(|| "shdt75 not found").to_string();
        let shdt76 = df.column("shdt76").unwrap().str().unwrap().get(row).unwrap_or_else(|| "shdt76 not found").to_string();
        let shdt77 = df.column("shdt77").unwrap().str().unwrap().get(row).unwrap_or_else(|| "shdt77 not found").to_string();
        let transaction_type = df.column("transaction_type").unwrap().str().unwrap().get(row).unwrap_or_else(|| "transaction_type not found").to_string();
        let transaction_indicator = df.column("transaction_indicator").unwrap().str().unwrap().get(row).unwrap_or_else(|| "transaction_indicator not found").to_string();
        let beneficiary_account_number = df.column("beneficiary_account_number").unwrap().str().unwrap().get(row).unwrap_or_else(|| "beneficiary_account_number not found").to_string();
        let remitter_account_number = df.column("remitter_account_number").unwrap().str().unwrap().get(row).unwrap_or_else(|| "remitter_account_number not found").to_string();
        let aadhar_number = df.column("aadhar_number").unwrap().str().unwrap().get(row).unwrap_or_else(|| "aadhar_number not found").to_string();
        let mobile_number = df.column("mobile_number").unwrap().str().unwrap().get(row).unwrap_or_else(|| "mobile_number not found").to_string();
        let payer_psp = df.column("payer_psp").unwrap().str().unwrap().get(row).unwrap_or_else(|| "payer_psp not found").to_string();
        let payee_psp = df.column("payee_psp").unwrap().str().unwrap().get(row).unwrap_or_else(|| "payee_psp not found").to_string();
        let upi_transaction_id = df.column("upi_transaction_id").unwrap().str().unwrap().get(row).unwrap_or_else(|| "upi_transaction_id not found").to_string();
        let virtual_address = df.column("virtual_address").unwrap().str().unwrap().get(row).unwrap_or_else(|| "virtual_address not found").to_string();
        let dispute_flag = df.column("dispute_flag").unwrap().str().unwrap().get(row).unwrap_or_else(|| "dispute_flag not found").to_string();
        let reason_code = df.column("reason_code").unwrap().str().unwrap().get(row).unwrap_or_else(|| "reason_code not found").to_string();
        let mcc = df.column("mcc").unwrap().str().unwrap().get(row).unwrap_or_else(|| "mcc not found").to_string();
        let originating_channel = df.column("originating_channel").unwrap().str().unwrap().get(row).unwrap_or_else(|| "originating_channel not found").to_string();

        new_models.push(YourModel::new(
            id,
            uid,
            adjdate,
            adjtype,
            remitter,
            beneficiery,
            response,
            txndate,
            txntime,
            rrn,
            terminalid,
            ben_mobile_no,
            rem_mobile_no,
            chbdate,
            chbref,
            txnamount,
            adjamount,
            rem_payee_psp_fee,
            ben_fee,
            ben_fee_sw,
            adjfee,
            npcifee,
            remfeetax,
            benfeetax,
            npcitax,
            adjref,
            bankadjref,
            adjproof,
            compensation_amount,
            adjustment_raised_time,
            no_of_days_for_penalty,
            shdt73,
            shdt74,
            shdt75,
            shdt76,
            shdt77,
            transaction_type,
            transaction_indicator,
            beneficiary_account_number,
            remitter_account_number,
            aadhar_number,
            mobile_number,
            payer_psp,
            payee_psp,
            upi_transaction_id,
            virtual_address,
            dispute_flag,
            reason_code,
            mcc,
            originating_channel,
        ));
    }
    Ok(new_models)
}