// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Internal utility proc-macros for Materialize.
//!
//! Note: This is separate from the `mz_ore` crate because `proc-macro` crates are only allowed
//! to export procedural macros and nothing else.

use proc_macro::TokenStream;

mod instrument;
mod test;

/// Materialize wrapper around the `#[tracing::insrument]` macro.
///
/// We wrap the `tracing::instrument` macro to skip tracing all arguments by default, this prevents
/// noisy or actively harmful things (e.g. the entire Catalog) from accidentally getting traced. If
/// you would like to include a function's argument in the traced span, you can use the
/// `fields(...)` syntax.
#[proc_macro_attribute]
pub fn instrument(attr: TokenStream, item: TokenStream) -> TokenStream {
    instrument::instrument_impl(attr, item)
}

/// Materialize wrapper around the `test` macro.
///
/// The wrapper automatically initializes our logging infrastructure.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    test::test_impl(attr, item)
}
