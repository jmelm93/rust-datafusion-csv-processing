// datafusion examples: https://github.com/apache/arrow-datafusion/tree/master/datafusion-examples/examples
// datafusion docs: https://arrow.apache.org/datafusion/
use datafusion::prelude::*;
// use datafusion::prelude::Column;
use datafusion::arrow::datatypes::{
    // DataType, 
    // Field, 
    Schema
    
};

// use serde::{ Deserialize };
// use serde_json::to_string;

use std::sync::Arc;
use std::str;
use std::fs;
// use std::future::Future;
use std::ops::Deref;


type DFResult = Result<Arc<DataFrame>, datafusion::error::DataFusionError>;

struct FinalObject {
    schema: Schema,
    // columns: Vec<Column>,
    num_rows: usize,
    num_columns: usize,
}

// to allow debug logging for FinalObject
impl std::fmt::Debug for FinalObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // write!(f, "FinalObject {{ schema: {:?}, columns: {:?}, num_rows: {:?}, num_columns: {:?} }}",
        write!(f, "FinalObject {{ schema: {:?}, num_rows: {:?}, num_columns: {:?} }}",
                // self.schema,  self.columns, self.num_columns, self.num_rows)
                self.schema, self.num_columns, self.num_rows)
    }
}

fn create_or_delete_csv_file(path: String, content: Option<String>, operation: &str) {
    match operation {
        "create" => {
            match content {
                Some(c) => fs::write(path, c.as_bytes()).expect("Problem with writing file!"),
                None => println!("The content is None, no file will be created"),
            }
        }
        "delete" => {
            // Delete the csv file
            fs::remove_file(path).expect("Problem with deleting file!");
        }
        _ => println!("Invalid operation"),
    }
}


async fn read_csv_file_with_inferred_schema(file_name_string: String) -> DFResult {
    // create string csv data
    let csv_data_string = "heading,value\nbasic,1\ncsv,2\nhere,3".to_string();

    // Create a temporary file
    create_or_delete_csv_file(file_name_string.clone(), Some(csv_data_string), "create");

    // Create a session context
    let ctx = SessionContext::new();

    // Register a lazy DataFrame using the context
    let df = ctx.read_csv(file_name_string.clone(), CsvReadOptions::default()).await.expect("An error occurred while reading the CSV string");

    // return the dataframe
    Ok(Arc::new(df))
}

#[tokio::main]
async fn main() {

    let file_name_string = "temp_file.csv".to_string();

    let arc_csv_df = read_csv_file_with_inferred_schema(file_name_string.clone()).await.expect("An error occurred while reading the CSV string (funct: read_csv_file_with_inferred_schema)");

    // have to use ".clone()" each time I want to use this ref
    let deref_df = arc_csv_df.deref();

    // print to console
    deref_df.clone().show().await.expect("An error occurred while showing the CSV DataFrame");

    // collect to vec
    let data = deref_df.clone().collect().await.expect("An error occurred while collecting the CSV DataFrame");
    // println!("Data: {:?}", data);

    // get final values from recordbatch
    // https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
    // https://users.rust-lang.org/t/how-to-use-recordbatch-in-arrow-when-using-datafusion/70057/2
    // https://github.com/apache/arrow-rs/blob/6.5.0/arrow/src/util/pretty.rs
    let data_vec = data.to_vec();

    let mut header = Vec::new();
    // let mut rows = Vec::new();

    for record_batch in data_vec {
        // get data
        println!("record_batch.columns: : {:?}", record_batch.columns());
        for col in record_batch.columns() {
            for row in 0..col.len() {
                // println!("Cow: {:?}", col);
                // println!("Row: {:?}", row);
                // let value = col.as_any().downcast_ref::<StringArray>().unwrap().value(row);
                // rows.push(value);
            }
        }
        // get headers
        for field in record_batch.schema().fields() {
            header.push(field.name().to_string());
        }
    };

    println!("Header: {:?}", header);
    // println!("Rows: {:?}", rows);
    
    // Delete temp csv
    create_or_delete_csv_file(file_name_string.clone(), None, "delete");
}