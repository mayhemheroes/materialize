// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::strconv;

sqlfunc!(
    #[sqlname = "NOT"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(Not)]
    fn not(a: bool) -> bool {
        !a
    }
);

sqlfunc!(
    #[sqlname = "boolean_to_text"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastStringToBool)]
    fn cast_bool_to_string<'a>(a: bool) -> &'a str {
        match a {
            true => "true",
            false => "false",
        }
    }
);

sqlfunc!(
    #[sqlname = "boolean_to_nonstandard_text"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastStringToBool)]
    fn cast_bool_to_string_nonstandard<'a>(a: bool) -> &'a str {
        // N.B. this function differs from `cast_bool_to_string` because
        // the SQL specification requires `true` and `false` to be spelled out in
        // explicit casts, while PostgreSQL prefers its more concise `t` and `f`
        // representation in some contexts, for historical reasons.
        strconv::format_bool_static(a)
    }
);

sqlfunc!(
    #[sqlname = "boolean_to_integer"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastInt32ToBool)]
    fn cast_bool_to_int32(a: bool) -> i32 {
        match a {
            true => 1,
            false => 0,
        }
    }
);

sqlfunc!(
    #[sqlname = "boolean_to_bigint"]
    #[preserves_uniqueness = true]
    #[inverse = to_unary!(super::CastInt64ToBool)]
    fn cast_bool_to_int64(a: bool) -> i64 {
        match a {
            true => 1,
            false => 0,
        }
    }
);
