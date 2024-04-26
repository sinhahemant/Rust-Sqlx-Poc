use sqlx::PgPool;
use sqlx::Error;

pub async fn create_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query!(
        r#"
        CREATE TABLE IF NOT EXISTS my_table (
            id VARCHAR(255) PRIMARY KEY,
            uid VARCHAR(255),
            adjdate VARCHAR(255),
            adjtype VARCHAR(255),
            remitter VARCHAR(255),
            beneficiery VARCHAR(255),
            response VARCHAR(255),
            txndate VARCHAR(255),
            txntime VARCHAR(255),
            rrn VARCHAR(255),
            terminalid VARCHAR(255),
            ben_mobile_no VARCHAR(255),
            rem_mobile_no VARCHAR(255),
            chbdate VARCHAR(255),
            chbref VARCHAR(255),
            txnamount VARCHAR(255),
            adjamount VARCHAR(255),
            rem_payee_psp_fee VARCHAR(255),
            ben_fee VARCHAR(255),
            ben_fee_sw VARCHAR(255),
            adjfee VARCHAR(255),
            npcifee VARCHAR(255),
            remfeetax VARCHAR(255),
            benfeetax VARCHAR(255),
            npcitax VARCHAR(255),
            adjref VARCHAR(255),
            bankadjref VARCHAR(255),
            adjproof VARCHAR(255),
            compensation_amount VARCHAR(255),
            adjustment_raised_time VARCHAR(255),
            no_of_days_for_penalty VARCHAR(255),
            shdt73 VARCHAR(255),
            shdt74 VARCHAR(255),
            shdt75 VARCHAR(255),
            shdt76 VARCHAR(255),
            shdt77 VARCHAR(255),
            transaction_type VARCHAR(255),
            transaction_indicator VARCHAR(255),
            beneficiary_account_number VARCHAR(255),
            remitter_account_number VARCHAR(255),
            aadhar_number VARCHAR(255),
            mobile_number VARCHAR(255),
            payer_psp VARCHAR(255),
            payee_psp VARCHAR(255),
            upi_transaction_id VARCHAR(255),
            virtual_address VARCHAR(255),
            dispute_flag VARCHAR(255),
            reason_code VARCHAR(255),
            mcc VARCHAR(255),
            originating_channel VARCHAR(255)
        )
        "#
    )
    .execute(pool)
    .await?;
    Ok(())
}
