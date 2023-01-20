// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from an a persist shard.

use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;

use mz_persist_client::operators::shard_source::shard_source;
use mz_persist_types::codec_impls::UnitSchema;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Capability, OkErr};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::scheduling::Activator;
use tokio::sync::Mutex;

use mz_expr::MfpPlan;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::fetch::FetchedPart;
use mz_repr::{DatumVec, Diff, GlobalId, Row, Timestamp};

use crate::controller::CollectionMetadata;
use crate::types::errors::DataflowError;
use crate::types::sources::SourceData;

pub use mz_persist_client::operators::shard_source::FlowControl;

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
/// All updates at times greater or equal to `until` will be suppressed.
/// The `map_filter_project` argument, if supplied, may be partially applied,
/// and any un-applied part of the argument will be left behind in the argument.
///
/// Users of this function have the ability to apply flow control to the output
/// to limit the in-flight data (measured in bytes) it can emit. The flow control
/// input is a timely stream that communicates the frontier at which the data
/// emitted from by this source have been dropped.
///
/// **Note:** Because this function is reading batches from `persist`, it is working
/// at batch granularity. In practice, the source will be overshooting the target
/// flow control upper by an amount that is related to the size of batches.
///
/// If no flow control is desired an empty stream whose frontier immediately advances
/// to the empty antichain can be used. An easy easy of creating such stream is by
/// using [`timely::dataflow::operators::generic::operator::empty`].
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn persist_source<G>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<G>>,
    refuel: usize,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let (stream, token) = persist_source_core(
        scope,
        source_id,
        persist_clients,
        metadata,
        as_of,
        until,
        map_filter_project,
        flow_control,
        refuel,
    );
    let (ok_stream, err_stream) = stream.ok_err(|(d, t, r)| match d {
        Ok(row) => Ok((row, t, r)),
        Err(err) => Err((err, t, r)),
    });
    (ok_stream, err_stream, token)
}

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
#[allow(clippy::needless_borrow)]
pub fn persist_source_core<G>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<G>>,
    refuel: usize,
) -> (
    Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let name = source_id.to_string();
    let (fetched, token) = shard_source(
        scope,
        &name,
        persist_clients,
        metadata.persist_location,
        metadata.data_shard,
        as_of,
        until.clone(),
        flow_control,
        Arc::new(metadata.relation_desc),
        Arc::new(UnitSchema),
    );
    let rows = decode_and_mfp(&fetched, &name, until, map_filter_project, refuel);
    (rows, token)
}

/// Pending work to read from feteched parts
struct PendingWork {
    /// The time at which the work should happen.
    capability: Capability<Timestamp>,
    /// Pending fetched part.
    fetched_part: FetchedPart<SourceData, (), Timestamp, Diff>,
}

impl PendingWork {
    /// Perform roughly `fuel` work, reading from the fetched part, decoding, and sending outputs.
    fn do_work(
        &mut self,
        fuel: &mut usize,
        until: &Antichain<Timestamp>,
        map_filter_project: Option<&MfpPlan>,
        datum_vec: &mut DatumVec,
        row_builder: &mut Row,
        output: &mut OutputHandle<
            '_,
            Timestamp,
            (Result<Row, DataflowError>, Timestamp, Diff),
            timely::dataflow::channels::pushers::Tee<
                Timestamp,
                (Result<Row, DataflowError>, Timestamp, Diff),
            >,
        >,
    ) {
        let mut work = 0;
        let mut session = output.session(&self.capability);
        while let Some(((key, val), time, diff)) = self.fetched_part.next() {
            if until.less_equal(&time) {
                continue;
            }
            match (key, val) {
                (Ok(SourceData(Ok(row))), Ok(())) => {
                    if let Some(mfp) = map_filter_project {
                        let arena = mz_repr::RowArena::new();
                        let mut datums_local = datum_vec.borrow_with(&row);
                        for result in mfp.evaluate(
                            &mut datums_local,
                            &arena,
                            time,
                            diff,
                            |time| !until.less_equal(time),
                            row_builder,
                        ) {
                            match result {
                                Ok((row, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        session.give((Ok(row), time, diff));
                                        work += 1;
                                    }
                                }
                                Err((err, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        session.give((Err(err), time, diff));
                                        work += 1;
                                    }
                                }
                            }
                        }
                    } else {
                        session.give((Ok(row), time, diff));
                        work += 1;
                    }
                }
                (Ok(SourceData(Err(err))), Ok(())) => {
                    session.give((Err(err), time, diff));
                    work += 1;
                }
                // TODO(petrosagg): error handling
                (Err(_), Ok(_)) | (Ok(_), Err(_)) | (Err(_), Err(_)) => {
                    panic!("decoding failed")
                }
            }
            if work >= *fuel {
                *fuel = 0;
                return;
            }
        }
        *fuel -= work;
    }
}

pub fn decode_and_mfp<G>(
    fetched: &Stream<G, FetchedPart<SourceData, (), Timestamp, Diff>>,
    name: &str,
    until: Antichain<Timestamp>,
    mut map_filter_project: Option<&mut MfpPlan>,
    refuel: usize,
) -> Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let scope = fetched.scope();
    let mut builder = OperatorBuilder::new(
        format!("persist_source::decode_and_mfp({})", name),
        scope.clone(),
    );
    let operator_info = builder.operator_info();

    let mut fetched_input = builder.new_input(fetched, Pipeline);
    let (mut updates_output, updates_stream) = builder.new_output();

    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    // Extract the MFP if it exists; leave behind an identity MFP in that case.
    let map_filter_project = map_filter_project.as_mut().map(|mfp| mfp.take());

    builder.build(move |_caps| {
        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activations = scope.activations();
        let activator = Activator::new(&operator_info.address[..], activations);
        // Maintain a list of work to do
        let mut todo = std::collections::VecDeque::new();
        let mut buffer = Default::default();

        move |_frontier| {
            fetched_input.for_each(|time, data| {
                data.swap(&mut buffer);
                let capability = time.retain();
                for fetched_part in buffer.drain(..) {
                    todo.push_back(PendingWork {
                        capability: capability.clone(),
                        fetched_part,
                    })
                }
            });

            let mut fuel = refuel;
            let mut handle = updates_output.activate();
            while !todo.is_empty() && fuel > 0 {
                todo.front_mut().unwrap().do_work(
                    &mut fuel,
                    &until,
                    map_filter_project.as_ref(),
                    &mut datum_vec,
                    &mut row_builder,
                    &mut handle,
                );
                if fuel > 0 {
                    todo.pop_front();
                }
            }
            if !todo.is_empty() {
                activator.activate();
            }
        }
    });

    updates_stream
}
