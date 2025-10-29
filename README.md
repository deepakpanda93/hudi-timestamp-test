# hudi-timestamp-test
hudi-timestamp-test

| Step | Description                                                                 | Result                   |
| ---- | --------------------------------------------------------------------------- | ------------------------ |
| 1️⃣  | Generates Parquet input once (`batchId=1`)                                  | `/raw_parquet/`          |
| 2️⃣  | Runs DeltaStreamer with **Hudi 0.14.0** for 8 configs                       | creates 8 tables         |
| 3️⃣  | Runs DeltaStreamer again with **Hudi 1.1.0 (fixed)** on those same 8 tables | upgrades same tables     |
| 4️⃣  | Reads + compares schemas, timestamps                                        | writes 8 diff reports    |
| ✅    | Writes summary CSV                                                          | one line per test result |

