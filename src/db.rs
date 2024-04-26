use crate::models::{MyModel, YourModel};
use sqlx::postgres::PgPool;
use sqlx::{Error, FromRow, QueryBuilder, Row};

pub struct Database {
    pub pool: PgPool, // Change visibility to public
}

impl Database {
    pub async fn new() -> Result<Self, Error> {
        let database_url =
            dotenv::var("DATABASE_URL").expect("DATABASE_URL not found in .env file");
        // let opts: PgConnectOptions = database_url.parse()?;
        let pool = PgPool::connect(&database_url).await?;
        Ok(Database { pool })
    }

    // Add a public method to expose the pool
    pub fn get_pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn insert_data(pool: &PgPool, models: Vec<YourModel>) -> Result<(), sqlx::Error> {
        let batch_size = 1000;
        for chunk in models.chunks(batch_size) {
            let mut query_builder = QueryBuilder::new("INSERT INTO my_table (id, uid, adjdate, adjtype, remitter, beneficiery, response, txndate, txntime, rrn, terminalid, ben_mobile_no, rem_mobile_no, chbdate, chbref, txnamount, adjamount, rem_payee_psp_fee, ben_fee, ben_fee_sw, adjfee, npcifee, remfeetax, benfeetax, npcitax, adjref, bankadjref, adjproof, compensation_amount, adjustment_raised_time, no_of_days_for_penalty, shdt73, shdt74, shdt75, shdt76, shdt77, transaction_type, transaction_indicator, beneficiary_account_number, remitter_account_number, aadhar_number, mobile_number, payer_psp, payee_psp, upi_transaction_id, virtual_address, dispute_flag, reason_code, mcc, originating_channel) ");
            query_builder.push_values(chunk, |mut b, model| {
                b.push_bind(model.id.clone())
                    .push_bind(model.uid.clone())
                    .push_bind(model.adjdate.clone())
                    .push_bind(model.adjtype.clone())
                    .push_bind(model.remitter.clone())
                    .push_bind(model.beneficiery.clone())
                    .push_bind(model.response.clone())
                    .push_bind(model.txndate.clone())
                    .push_bind(model.txntime.clone())
                    .push_bind(model.rrn.clone())
                    .push_bind(model.terminalid.clone())
                    .push_bind(model.ben_mobile_no.clone())
                    .push_bind(model.rem_mobile_no.clone())
                    .push_bind(model.chbdate.clone())
                    .push_bind(model.chbref.clone())
                    .push_bind(model.txnamount.clone())
                    .push_bind(model.adjamount.clone())
                    .push_bind(model.rem_payee_psp_fee.clone())
                    .push_bind(model.ben_fee.clone())
                    .push_bind(model.ben_fee_sw.clone())
                    .push_bind(model.adjfee.clone())
                    .push_bind(model.npcifee.clone())
                    .push_bind(model.remfeetax.clone())
                    .push_bind(model.benfeetax.clone())
                    .push_bind(model.npcitax.clone())
                    .push_bind(model.adjref.clone())
                    .push_bind(model.bankadjref.clone())
                    .push_bind(model.adjproof.clone())
                    .push_bind(model.compensation_amount.clone())
                    .push_bind(model.adjustment_raised_time.clone())
                    .push_bind(model.no_of_days_for_penalty.clone())
                    .push_bind(model.shdt73.clone())
                    .push_bind(model.shdt74.clone())
                    .push_bind(model.shdt75.clone())
                    .push_bind(model.shdt76.clone())
                    .push_bind(model.shdt77.clone())
                    .push_bind(model.transaction_type.clone())
                    .push_bind(model.transaction_indicator.clone())
                    .push_bind(model.beneficiary_account_number.clone())
                    .push_bind(model.remitter_account_number.clone())
                    .push_bind(model.aadhar_number.clone())
                    .push_bind(model.mobile_number.clone())
                    .push_bind(model.payer_psp.clone())
                    .push_bind(model.payee_psp.clone())
                    .push_bind(model.upi_transaction_id.clone())
                    .push_bind(model.virtual_address.clone())
                    .push_bind(model.dispute_flag.clone())
                    .push_bind(model.reason_code.clone())
                    .push_bind(model.mcc.clone())
                    .push_bind(model.originating_channel.clone());
            });

            let query = query_builder.build();

            query.execute(pool).await?;
        }

        Ok(())
    }

    pub async fn update_data(pool: &PgPool, data: &YourModel) -> Result<(), Error> {
        // Implement update data logic
        let query = sqlx::query(
            "UPDATE my_table SET uid  = $2, adjdate  = $3, adjtype  = $4, remitter  = $5, beneficiery  = $6, response  = $7, txndate  = $8, txntime  = $9, rrn  = $10, terminalid  = $11, ben_mobile_no  = $12, rem_mobile_no  = $13, chbdate  = $14, chbref  = $15, txnamount  = $16, adjamount  = $17, rem_payee_psp_fee  = $18, ben_fee  = $19, ben_fee_sw  = $20, adjfee  = $21, npcifee  = $22, remfeetax  = $23, benfeetax  = $24, npcitax  = $25, adjref  = $26, bankadjref  = $27, adjproof  = $28, compensation_amount  = $29, adjustment_raised_time  = $30, no_of_days_for_penalty  = $31, shdt73  = $32, shdt74  = $33, shdt75  = $34, shdt76  = $35, shdt77  = $36, transaction_type  = $37, transaction_indicator  = $38, beneficiary_account_number  = $39, remitter_account_number  = $40, aadhar_number  = $41, mobile_number  = $42, payer_psp  = $43, payee_psp  = $44, upi_transaction_id  = $45, virtual_address  = $46, dispute_flag  = $47, reason_code  = $48, mcc  = $49, originating_channel = $50 WHERE id = $1",
        )
        .bind(&data.id)
        .bind(&data.uid)
        .bind(&data.adjdate)
        .bind(&data.adjtype)
        .bind(&data.remitter)
        .bind(&data.beneficiery)
        .bind(&data.response)
        .bind(&data.txndate)
        .bind(&data.txntime)
        .bind(&data.rrn)
        .bind(&data.terminalid)
        .bind(&data.ben_mobile_no)
        .bind(&data.rem_mobile_no)
        .bind(&data.chbdate)
        .bind(&data.chbref)
        .bind(&data.txnamount)
        .bind(&data.adjamount)
        .bind(&data.rem_payee_psp_fee)
        .bind(&data.ben_fee)
        .bind(&data.ben_fee_sw)
        .bind(&data.adjfee)
        .bind(&data.npcifee)
        .bind(&data.remfeetax)
        .bind(&data.benfeetax)
        .bind(&data.npcitax)
        .bind(&data.adjref)
        .bind(&data.bankadjref)
        .bind(&data.adjproof)
        .bind(&data.compensation_amount)
        .bind(&data.adjustment_raised_time)
        .bind(&data.no_of_days_for_penalty)
        .bind(&data.shdt73)
        .bind(&data.shdt74)
        .bind(&data.shdt75)
        .bind(&data.shdt76)
        .bind(&data.shdt77)
        .bind(&data.transaction_type)
        .bind(&data.transaction_indicator)
        .bind(&data.beneficiary_account_number)
        .bind(&data.remitter_account_number)
        .bind(&data.aadhar_number)
        .bind(&data.mobile_number)
        .bind(&data.payer_psp)
        .bind(&data.payee_psp)
        .bind(&data.upi_transaction_id)
        .bind(&data.virtual_address)
        .bind(&data.dispute_flag)
        .bind(&data.reason_code)
        .bind(&data.mcc)
        .bind(&data.originating_channel);

        query.execute(pool).await?;

        Ok(())
    }

    pub async fn select_data(
        pool: &PgPool,
        condition_column: &str,
        condition_value: &str,
    ) -> Result<Vec<MyModel>, Error> {
        // Implement select data logic
        let data = sqlx::query_as::<_, MyModel>(
            "SELECT id,uid,adjdate,adjtype,remitter,beneficiery,response,txndate,txntime,rrn,terminalid,ben_mobile_no,rem_mobile_no,chbdate,chbref,txnamount,adjamount,rem_payee_psp_fee,ben_fee,ben_fee_sw,adjfee,npcifee,remfeetax,benfeetax,npcitax,adjref,bankadjref,adjproof,compensation_amount,adjustment_raised_time,no_of_days_for_penalty,shdt73,shdt74,shdt75,shdt76,shdt77,transaction_type,transaction_indicator,beneficiary_account_number,remitter_account_number,aadhar_number,mobile_number,payer_psp,payee_psp,upi_transaction_id,virtual_address,dispute_flag,reason_code,mcc,originating_channel FROM my_table WHERE id = $1 limit 1",
        )
        // .bind(condition_column)
        .bind(condition_value)
        .fetch_one(pool).await?;
        Ok(vec![data])
    }
}

impl<'r> FromRow<'r, sqlx::postgres::PgRow> for MyModel {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            uid: row.try_get("uid")?,
            adjdate: row.try_get("adjdate")?,
            adjtype: row.try_get("adjtype")?,
            remitter: row.try_get("remitter")?,
            beneficiery: row.try_get("beneficiery")?,
            response: row.try_get("response")?,
            txndate: row.try_get("txndate")?,
            txntime: row.try_get("txntime")?,
            rrn: row.try_get("rrn")?,
            terminalid: row.try_get("terminalid")?,
            ben_mobile_no: row.try_get("ben_mobile_no")?,
            rem_mobile_no: row.try_get("rem_mobile_no")?,
            chbdate: row.try_get("chbdate")?,
            chbref: row.try_get("chbref")?,
            txnamount: row.try_get("txnamount")?,
            adjamount: row.try_get("adjamount")?,
            rem_payee_psp_fee: row.try_get("rem_payee_psp_fee")?,
            ben_fee: row.try_get("ben_fee")?,
            ben_fee_sw: row.try_get("ben_fee_sw")?,
            adjfee: row.try_get("adjfee")?,
            npcifee: row.try_get("npcifee")?,
            remfeetax: row.try_get("remfeetax")?,
            benfeetax: row.try_get("benfeetax")?,
            npcitax: row.try_get("npcitax")?,
            adjref: row.try_get("adjref")?,
            bankadjref: row.try_get("bankadjref")?,
            adjproof: row.try_get("adjproof")?,
            compensation_amount: row.try_get("compensation_amount")?,
            adjustment_raised_time: row.try_get("adjustment_raised_time")?,
            no_of_days_for_penalty: row.try_get("no_of_days_for_penalty")?,
            shdt73: row.try_get("shdt73")?,
            shdt74: row.try_get("shdt74")?,
            shdt75: row.try_get("shdt75")?,
            shdt76: row.try_get("shdt76")?,
            shdt77: row.try_get("shdt77")?,
            transaction_type: row.try_get("transaction_type")?,
            transaction_indicator: row.try_get("transaction_indicator")?,
            beneficiary_account_number: row.try_get("beneficiary_account_number")?,
            remitter_account_number: row.try_get("remitter_account_number")?,
            aadhar_number: row.try_get("aadhar_number")?,
            mobile_number: row.try_get("mobile_number")?,
            payer_psp: row.try_get("payer_psp")?,
            payee_psp: row.try_get("payee_psp")?,
            upi_transaction_id: row.try_get("upi_transaction_id")?,
            virtual_address: row.try_get("virtual_address")?,
            dispute_flag: row.try_get("dispute_flag")?,
            reason_code: row.try_get("reason_code")?,
            mcc: row.try_get("mcc")?,
            originating_channel: row.try_get("originating_channel")?,
            // Ensure you match the type of each field correctly
        })
    }
}
