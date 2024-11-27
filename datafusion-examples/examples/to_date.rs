// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::*;

/// This example demonstrates how to use the to_date series
/// of functions in the DataFrame API as well as via sql.
#[tokio::main]
async fn main() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            "2020-09-08T13:42:29Z",
            "2020-09-08T13:42:29.190855-05:00",
            "2020-08-09 12:13:29",
            "2020-01-02",
        ]))],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch).await?;
    let df = ctx.table("t").await?;

    // use to_date function to convert col 'a' to timestamp type using the default parsing
    let df = df.with_column("a", to_date(vec![col("a")]))?;

    let df = df.select_columns(&["a"])?;

    // print the results
    df.show().await?;

    Ok(())
}
