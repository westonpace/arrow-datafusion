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

use arrow::array::{RecordBatch, StringBuilder};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::TryStreamExt;
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
pub async fn main() {
    build_parquet();

    for reservation_mb in 0..100 {
        let reservation_bytes = reservation_mb * 1024 * 1024;

        let env = RuntimeEnvBuilder::new()
            .with_disk_manager(DiskManagerConfig::default())
            .with_memory_pool(Arc::new(FairSpillPool::new(100 * 1024 * 1024)))
            .build_arc()
            .unwrap();

        let state = SessionStateBuilder::new()
            .with_runtime_env(env)
            .with_config(
                SessionConfig::default()
                    .with_sort_spill_reservation_bytes(reservation_bytes),
            )
            .build();

        let ctx = SessionContext::from(state);

        ctx.register_parquet(
            "big_strings",
            "/tmp/big_strings.parquet",
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        let sql = "SELECT * FROM big_strings ORDER BY strings";
        let res = ctx
            .sql(sql)
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap()
            .try_for_each(|_| std::future::ready(Ok(())))
            .await;

        if res.is_ok() {
            println!("{}: OK", reservation_mb);
        } else {
            println!("{}: FAIL", reservation_mb);
        }
    }
}

fn build_parquet() {
    if std::fs::File::open("/tmp/big_strings.parquet").is_ok() {
        println!("Using existing file at /tmp/big_strings.parquet");
        return;
    }
    println!("Generating test file at /tmp/big_strings.parquet");
    let file = std::fs::File::create("/tmp/big_strings.parquet").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "strings",
        DataType::Utf8,
        false,
    )]));
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    for batch_idx in 0..100 {
        println!("Generating batch {} of 100", batch_idx);
        let mut string_array_builder =
            StringBuilder::with_capacity(1024 * 1024, 1024 * 1024 * 3 * 14);
        for i in 0..(1024 * 1024) {
            string_array_builder
                .append_value(format!("string-{}string-{}string-{}", i, i, i));
        }
        let array = Arc::new(string_array_builder.finish());
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
}
