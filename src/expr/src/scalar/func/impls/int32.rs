// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

sqlfunc!(
    #[sqlname = "-"]
    fn neg_int32(a: i32) -> i32 {
        -a
    }
);

sqlfunc!(
    #[sqlname = "~"]
    fn bit_not_int32(a: i32) -> i32 {
        !a
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_int32(a: i32) -> i32 {
        a.abs()
    }
);
