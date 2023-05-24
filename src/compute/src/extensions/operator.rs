// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::trace::{Batch, Trace, TraceReader};
use differential_dataflow::{Collection, Data, ExchangeData, Hashable};
use mz_repr::Row;
use mz_storage_client::types::errors::DataflowError;
use timely::container::columnation::Columnation;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::{Antichain, Timestamp};

use crate::logging::compute::ComputeEvent;
use crate::typedefs::{ErrSpine, ErrValSpine, RowKeySpine, RowSpine};

pub(crate) trait MzArrange<G: Scope, K, V, R: Semigroup>
where
    G::Timestamp: Lattice,
    K: Data,
    V: Data,
{
    /// Arranges a stream of `(Key, Val)` updates by `Key`. Accepts an empty instance of the trace type.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        K: ExchangeData + Hashable,
        V: ExchangeData,
        R: ExchangeData,
        Tr: Trace + TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize;

    /// Arranges a stream of `(Key, Val)` updates by `Key`. Accepts an empty instance of the trace type.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        R: ExchangeData,
        P: ParallelizationContract<G::Timestamp, ((K, V), G::Timestamp, R)>,
        Tr: Trace + TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize;
}

impl<G, K, V, R> MzArrange<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
    K: Data,
    V: Data,
    R: Semigroup,
{
    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        K: ExchangeData + Hashable,
        V: ExchangeData,
        R: ExchangeData,
        Tr: Trace + TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.arrange_named(name).log_arrangement_size()
    }

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        R: ExchangeData,
        P: ParallelizationContract<G::Timestamp, ((K, V), G::Timestamp, R)>,
        Tr: Trace + TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.arrange_core(pact, name).log_arrangement_size()
    }
}

impl<G, R> MzArrange<G, DataflowError, (), R> for Collection<G, DataflowError, R>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
    R: Semigroup,
{
    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        R: ExchangeData,
        Tr: Trace
            + TraceReader<Key = DataflowError, Val = (), Time = G::Timestamp, R = R>
            + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.arrange_named(name).log_arrangement_size()
    }

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        R: ExchangeData,
        P: ParallelizationContract<G::Timestamp, ((DataflowError, ()), G::Timestamp, R)>,
        Tr: Trace
            + TraceReader<Key = DataflowError, Val = (), Time = G::Timestamp, R = R>
            + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.arrange_core(pact, name).log_arrangement_size()
    }
}

// A type that can log its heap size.
pub(crate) trait ArrangementSize {
    fn log_arrangement_size(&self) -> Self;
}

/// Helper to compute the size of a vector in memory.
///
/// The function only considers the immediate allocation of the vector, but is oblivious of any
/// pointers to owned allocations.
#[inline]
fn vec_size<T>(data: &Vec<T>, mut callback: impl FnMut(usize, usize)) {
    let size_of_t = std::mem::size_of::<T>();
    callback(data.len() * size_of_t, data.capacity() * size_of_t);
}

/// Helper for [`ArrangementSize`] to install a common operator holding on to a trace.
fn log_arrangement_size_inner<G, Tr, L>(arranged: &Arranged<G, TraceAgent<Tr>>, mut logic: L)
where
    G: Scope,
    G::Timestamp: Timestamp + Lattice + Ord,
    Tr: TraceReader + 'static,
    Tr::Time: Timestamp + Lattice + Ord + Clone + 'static,
    L: FnMut(&TraceAgent<Tr>) -> (usize, usize, usize) + 'static,
{
    let scope = arranged.stream.scope();
    let Some(logger) = scope.log_register().get::<ComputeEvent>("materialize/compute") else {return};
    let mut trace = arranged.trace.clone();
    let operator = trace.operator().global_id;

    // We don't want to block compaction.
    trace.set_logical_compaction(Antichain::new().borrow());
    trace.set_physical_compaction(Antichain::new().borrow());

    let (mut old_size, mut old_capacity, mut old_allocations) = (0isize, 0isize, 0isize);

    let mut builder = OperatorBuilder::new("ArrangementSize".to_owned(), scope);
    let mut input = builder.new_input(&arranged.stream, Pipeline);
    let address = builder.operator_info().address;
    logger.log(ComputeEvent::ArrangementHeapSizeOperator { operator, address });
    builder.build(|_cap| {
        move |_frontiers| {
            input.for_each(|_time, _data| {});
            let (size, capacity, allocations) = logic(&trace);

            let size = size.try_into().expect("must fit");
            if size != old_size {
                logger.log(ComputeEvent::ArrangementHeapSize {
                    operator,
                    size: size - old_size,
                });
            }

            let capacity = capacity.try_into().expect("must fit");
            if capacity != old_capacity {
                logger.log(ComputeEvent::ArrangementHeapCapacity {
                    operator,
                    capacity: capacity - old_capacity,
                });
            }

            let allocations = allocations.try_into().expect("must fit");
            if allocations != old_allocations {
                logger.log(ComputeEvent::ArrangementHeapAllocations {
                    operator,
                    allocations: allocations - old_allocations,
                });
            }

            old_size = size;
            old_capacity = capacity;
            old_allocations = allocations;
        }
    });
}

impl<G, K, V, T, R> ArrangementSize for Arranged<G, TraceAgent<RowSpine<K, V, T, R>>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord,
    K: Data + Columnation,
    V: Data + Columnation,
    T: Lattice + Timestamp,
    R: Semigroup,
{
    fn log_arrangement_size(&self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                allocations += 1;
                size += siz;
                capacity += cap
            };
            trace.map_batches(|batch| {
                batch.layer.keys.heap_size(&mut callback);
                batch.layer.vals.keys.heap_size(&mut callback);
                vec_size(&batch.layer.offs, &mut callback);
                vec_size(&batch.layer.vals.offs, &mut callback);
                vec_size(&batch.layer.vals.vals.vals, &mut callback);
            });
            (size, capacity, allocations)
        });
        self.clone()
    }
}

impl<G, T, R> ArrangementSize for Arranged<G, TraceAgent<ErrValSpine<Row, T, R>>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord,
    T: Lattice + Timestamp,
    R: Semigroup,
{
    fn log_arrangement_size(&self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                allocations += 1;
                size += siz;
                capacity += cap
            };
            trace.map_batches(|batch| {
                vec_size(&batch.layer.keys, &mut callback);
                vec_size(&batch.layer.offs, &mut callback);
                vec_size(&batch.layer.vals.keys, &mut callback);
                vec_size(&batch.layer.vals.offs, &mut callback);
                vec_size(&batch.layer.vals.vals.vals, &mut callback);
            });
            (size, capacity, allocations)
        });
        self.clone()
    }
}

impl<G, T, R> ArrangementSize for Arranged<G, TraceAgent<ErrSpine<DataflowError, T, R>>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord,
    T: Lattice + Timestamp,
    R: Semigroup,
{
    fn log_arrangement_size(&self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                allocations += 1;
                size += siz;
                capacity += cap
            };
            trace.map_batches(|batch| {
                vec_size(&batch.layer.keys, &mut callback);
                vec_size(&batch.layer.offs, &mut callback);
                vec_size(&batch.layer.vals.vals, &mut callback);
            });
            (size, capacity, allocations)
        });
        self.clone()
    }
}

impl<G, K, T, R> ArrangementSize for Arranged<G, TraceAgent<RowKeySpine<K, T, R>>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord,
    K: Data + Columnation,
    T: Lattice + Timestamp,
    R: Semigroup,
{
    fn log_arrangement_size(&self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                allocations += 1;
                size += siz;
                capacity += cap
            };
            trace.map_batches(|batch| {
                batch.layer.keys.heap_size(&mut callback);
                vec_size(&batch.layer.offs, &mut callback);
                vec_size(&batch.layer.vals.vals, &mut callback);
            });
            (size, capacity, allocations)
        });
        self.clone()
    }
}

// TODO: `reduce_pair`, `consolidate_named_if`
/// Extension trait for the `reduce_core` differential dataflow method.
pub(crate) trait MzReduce<G: Scope, K: Data, V: Data, R: Semigroup>:
    ReduceCore<G, K, V, R>
where
    G::Timestamp: Lattice + Ord,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, T2>(&self, name: &str, mut logic: L) -> Arranged<G, TraceAgent<T2>>
    where
        T2: Trace + TraceReader<Key = K, Time = G::Timestamp> + 'static,
        T2::Val: Data,
        T2::R: Abelian,
        T2::Batch: Batch,
        L: FnMut(&K, &[(&V, R)], &mut Vec<(T2::Val, T2::R)>) + 'static,
        Arranged<G, TraceAgent<T2>>: ArrangementSize,
    {
        // Allow access to `reduce_core` since we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.reduce_core::<_, T2>(name, move |key, input, output, change| {
            if !input.is_empty() {
                logic(key, input, change);
            }
            change.extend(output.drain(..).map(|(x, d)| (x, d.negate())));
        })
        .log_arrangement_size()
    }
}

impl<G, K, V, T1, R> MzReduce<G, K, V, R> for Arranged<G, T1>
where
    G::Timestamp: Lattice + Ord,
    G: Scope,
    K: Data,
    V: Data,
    R: Semigroup,
    T1: TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + Clone + 'static,
{
}
