// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [protobuf] representation of [Apache Arrow] arrays.
//!
//! # Motivation
//!
//! Persist can store a small amount of data inline at the consensus layer.
//! Because we are space constrained, we take particular care to store only the
//! data that is necessary. Other Arrow serialization formats, e.g. [Parquet]
//! or [Arrow IPC], include data that we don't need and would be wasteful to
//! store.
//!
//! [protobuf]: https://protobuf.dev/
//! [Apache Arrow]: https://arrow.apache.org/
//! [Parquet]: https://parquet.apache.org/docs/
//! [Arrow IPC]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{ArrowNativeType, DataType, Field, Fields};
use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};

#[allow(missing_docs)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/mz_persist_types.arrow.rs"));
}
pub use proto::ProtoArrayData;

impl RustType<ProtoArrayData> for arrow::array::ArrayData {
    fn into_proto(&self) -> ProtoArrayData {
        ProtoArrayData {
            data_type: Some(self.data_type().into_proto()),
            length: u64::cast_from(self.len()),
            offset: u64::cast_from(self.offset()),
            buffers: self.buffers().iter().map(|b| b.into_proto()).collect(),
            children: self.child_data().iter().map(|c| c.into_proto()).collect(),
            nulls: self.nulls().map(|n| n.inner().into_proto()),
        }
    }

    fn from_proto(proto: ProtoArrayData) -> Result<Self, TryFromProtoError> {
        let ProtoArrayData {
            data_type,
            length,
            offset,
            buffers,
            children,
            nulls,
        } = proto;
        let data_type = data_type.into_rust_if_some("data_type")?;
        let nulls = nulls
            .map(|n| n.into_rust())
            .transpose()?
            .map(NullBuffer::new);

        let mut builder = ArrayDataBuilder::new(data_type)
            .len(usize::cast_from(length))
            .offset(usize::cast_from(offset))
            .nulls(nulls);

        for b in buffers.into_iter().map(|b| b.into_rust()) {
            builder = builder.add_buffer(b?);
        }
        for c in children.into_iter().map(|c| c.into_rust()) {
            builder = builder.add_child_data(c?);
        }

        // Construct the builder which validates all inputs and aligns data.
        builder
            .build_aligned()
            .map_err(|e| TryFromProtoError::RowConversionError(e.to_string()))
    }
}

impl RustType<proto::DataType> for arrow::datatypes::DataType {
    fn into_proto(&self) -> proto::DataType {
        let kind = match self {
            DataType::Null => proto::data_type::Kind::Null(()),
            DataType::Boolean => proto::data_type::Kind::Boolean(()),
            DataType::UInt8 => proto::data_type::Kind::Uint8(()),
            DataType::UInt16 => proto::data_type::Kind::Uint16(()),
            DataType::UInt32 => proto::data_type::Kind::Uint32(()),
            DataType::UInt64 => proto::data_type::Kind::Uint64(()),
            DataType::Int8 => proto::data_type::Kind::Int8(()),
            DataType::Int16 => proto::data_type::Kind::Int16(()),
            DataType::Int32 => proto::data_type::Kind::Int32(()),
            DataType::Int64 => proto::data_type::Kind::Int64(()),
            DataType::Float32 => proto::data_type::Kind::Float32(()),
            DataType::Float64 => proto::data_type::Kind::Float64(()),
            DataType::Utf8 => proto::data_type::Kind::String(()),
            DataType::Binary => proto::data_type::Kind::Binary(()),
            DataType::FixedSizeBinary(size) => proto::data_type::Kind::FixedBinary(*size),
            DataType::List(inner) => proto::data_type::Kind::List(Box::new(inner.into_proto())),
            DataType::Map(inner, sorted) => {
                let map = proto::data_type::Map {
                    value: Some(Box::new(inner.into_proto())),
                    sorted: *sorted,
                };
                proto::data_type::Kind::Map(Box::new(map))
            }
            DataType::Struct(children) => {
                let children = children.into_iter().map(|f| f.into_proto()).collect();
                proto::data_type::Kind::Struct(proto::data_type::Struct { children })
            }
            other => unimplemented!("unsupported data type {other:?}"),
        };

        proto::DataType { kind: Some(kind) }
    }

    fn from_proto(proto: proto::DataType) -> Result<Self, TryFromProtoError> {
        let data_type = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("kind"))?;
        let data_type = match data_type {
            proto::data_type::Kind::Null(()) => DataType::Null,
            proto::data_type::Kind::Boolean(()) => DataType::Boolean,
            proto::data_type::Kind::Uint8(()) => DataType::UInt8,
            proto::data_type::Kind::Uint16(()) => DataType::UInt16,
            proto::data_type::Kind::Uint32(()) => DataType::UInt32,
            proto::data_type::Kind::Uint64(()) => DataType::UInt64,
            proto::data_type::Kind::Int8(()) => DataType::Int8,
            proto::data_type::Kind::Int16(()) => DataType::Int16,
            proto::data_type::Kind::Int32(()) => DataType::Int32,
            proto::data_type::Kind::Int64(()) => DataType::Int64,
            proto::data_type::Kind::Float32(()) => DataType::Float32,
            proto::data_type::Kind::Float64(()) => DataType::Float64,
            proto::data_type::Kind::String(()) => DataType::Utf8,
            proto::data_type::Kind::Binary(()) => DataType::Binary,
            proto::data_type::Kind::FixedBinary(size) => DataType::FixedSizeBinary(size),
            proto::data_type::Kind::List(inner) => DataType::List(Arc::new((*inner).into_rust()?)),
            proto::data_type::Kind::Map(inner) => {
                let value = inner
                    .value
                    .ok_or_else(|| TryFromProtoError::missing_field("map.value"))?;
                DataType::Map(Arc::new((*value).into_rust()?), inner.sorted)
            }
            proto::data_type::Kind::Struct(inner) => {
                let children: Vec<Field> = inner
                    .children
                    .into_iter()
                    .map(|c| c.into_rust())
                    .collect::<Result<_, _>>()?;
                DataType::Struct(Fields::from(children))
            }
        };

        Ok(data_type)
    }
}

impl RustType<proto::Field> for arrow::datatypes::Field {
    fn into_proto(&self) -> proto::Field {
        proto::Field {
            name: self.name().clone(),
            nullable: self.is_nullable(),
            data_type: Some(Box::new(self.data_type().into_proto())),
        }
    }

    fn from_proto(proto: proto::Field) -> Result<Self, TryFromProtoError> {
        let proto::Field {
            name,
            nullable,
            data_type,
        } = proto;
        let data_type =
            data_type.ok_or_else(|| TryFromProtoError::missing_field("field.data_type"))?;
        let data_type = (*data_type).into_rust()?;

        Ok(Field::new(name, data_type, nullable))
    }
}

impl RustType<proto::Buffer> for arrow::buffer::Buffer {
    fn into_proto(&self) -> proto::Buffer {
        // TODO(parkmycar): There is probably something better we can do here.
        proto::Buffer {
            data: bytes::Bytes::copy_from_slice(self.as_slice()),
        }
    }

    fn from_proto(proto: proto::Buffer) -> Result<Self, TryFromProtoError> {
        Ok(arrow::buffer::Buffer::from_bytes(proto.data.into()))
    }
}

impl RustType<proto::BooleanBuffer> for arrow::buffer::BooleanBuffer {
    fn into_proto(&self) -> proto::BooleanBuffer {
        proto::BooleanBuffer {
            buffer: Some(self.sliced().into_proto()),
            length: u64::cast_from(self.len()),
        }
    }

    fn from_proto(proto: proto::BooleanBuffer) -> Result<Self, TryFromProtoError> {
        let proto::BooleanBuffer { buffer, length } = proto;
        let buffer = buffer.into_rust_if_some("buffer")?;
        Ok(BooleanBuffer::new(buffer, 0, usize::cast_from(length)))
    }
}

/// Wraps a single arrow array, downcasted to a specific type.
#[derive(Clone, Debug)]
pub enum ArrayOrd {
    /// Wraps a `NullArray`.
    Null(NullArray),
    /// Wraps a `Bool` array.
    Bool(BooleanArray),
    /// Wraps a `Int8` array.
    Int8(Int8Array),
    /// Wraps a `Int16` array.
    Int16(Int16Array),
    /// Wraps a `Int32` array.
    Int32(Int32Array),
    /// Wraps a `Int64` array.
    Int64(Int64Array),
    /// Wraps a `UInt8` array.
    UInt8(UInt8Array),
    /// Wraps a `UInt16` array.
    UInt16(UInt16Array),
    /// Wraps a `UInt32` array.
    UInt32(UInt32Array),
    /// Wraps a `UInt64` array.
    UInt64(UInt64Array),
    /// Wraps a `Float32` array.
    Float32(Float32Array),
    /// Wraps a `Float64` array.
    Float64(Float64Array),
    /// Wraps a `String` array.
    String(StringArray),
    /// Wraps a `Binary` array.
    Binary(BinaryArray),
    /// Wraps a `FixedSizeBinary` array.
    FixedSizeBinary(FixedSizeBinaryArray),
    /// Wraps a `List` array.
    List(Option<NullBuffer>, OffsetBuffer<i32>, Box<ArrayOrd>),
    /// Wraps a `Struct` array.
    Struct(Option<NullBuffer>, Vec<ArrayOrd>),
}

impl ArrayOrd {
    /// Downcast the provided array to a specific type in our enum.
    pub fn new(array: &dyn Array) -> Self {
        match array.data_type() {
            DataType::Null => ArrayOrd::Null(NullArray::from(array.to_data())),
            DataType::Boolean => ArrayOrd::Bool(array.as_boolean().clone()),
            DataType::Int8 => ArrayOrd::Int8(array.as_primitive().clone()),
            DataType::Int16 => ArrayOrd::Int16(array.as_primitive().clone()),
            DataType::Int32 => ArrayOrd::Int32(array.as_primitive().clone()),
            DataType::Int64 => ArrayOrd::Int64(array.as_primitive().clone()),
            DataType::UInt8 => ArrayOrd::UInt8(array.as_primitive().clone()),
            DataType::UInt16 => ArrayOrd::UInt16(array.as_primitive().clone()),
            DataType::UInt32 => ArrayOrd::UInt32(array.as_primitive().clone()),
            DataType::UInt64 => ArrayOrd::UInt64(array.as_primitive().clone()),
            DataType::Float32 => ArrayOrd::Float32(array.as_primitive().clone()),
            DataType::Float64 => ArrayOrd::Float64(array.as_primitive().clone()),
            DataType::Binary => ArrayOrd::Binary(array.as_binary().clone()),
            DataType::Utf8 => ArrayOrd::String(array.as_string().clone()),
            DataType::FixedSizeBinary(_) => {
                ArrayOrd::FixedSizeBinary(array.as_fixed_size_binary().clone())
            }
            DataType::List(_) => {
                let list_array = array.as_list();
                ArrayOrd::List(
                    list_array.nulls().cloned(),
                    list_array.offsets().clone(),
                    Box::new(ArrayOrd::new(list_array.values())),
                )
            }
            DataType::Struct(_) => {
                let struct_array = array.as_struct();
                let nulls = array.nulls().cloned();
                let columns: Vec<_> = struct_array
                    .columns()
                    .iter()
                    .map(|a| ArrayOrd::new(a))
                    .collect();
                ArrayOrd::Struct(nulls, columns)
            }
            data_type => unimplemented!("array type {data_type:?} not yet supported"),
        }
    }

    /// Return a struct representing the value at a particular index in this array.
    pub fn at(&self, idx: usize) -> ArrayIdx {
        ArrayIdx { idx, array: self }
    }
}

/// A struct representing a particular entry in a particular array. Most useful for its `Ord`
/// implementation, which can compare entire rows across similarly-typed arrays.
#[derive(Clone, Copy, Debug)]
pub struct ArrayIdx<'a> {
    /// An index into a particular array.
    pub idx: usize,
    /// The particular array.
    pub array: &'a ArrayOrd,
}

impl<'a> Ord for ArrayIdx<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        #[inline]
        fn is_null(buffer: &Option<NullBuffer>, idx: usize) -> bool {
            buffer.as_ref().map_or(false, |b| b.is_null(idx))
        }
        #[inline]
        fn cmp<A: ArrayAccessor>(
            left: A,
            left_idx: usize,
            right: A,
            right_idx: usize,
            cmp: fn(&A::Item, &A::Item) -> Ordering,
        ) -> Ordering {
            // NB: nulls sort last, conveniently matching psql / mz_repr
            match (left.is_null(left_idx), right.is_null(right_idx)) {
                (false, true) => Ordering::Less,
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, false) => cmp(&left.value(left_idx), &right.value(right_idx)),
            }
        }
        match (&self.array, &other.array) {
            (ArrayOrd::Null(s), ArrayOrd::Null(o)) => {
                debug_assert!(
                    self.idx < s.len() && other.idx < o.len(),
                    "null array indices in bounds"
                );
                Ordering::Equal
            }
            // For arrays with "simple" value types, we fetch and compare the underlying values directly.
            (ArrayOrd::Bool(s), ArrayOrd::Bool(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Int8(s), ArrayOrd::Int8(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Int16(s), ArrayOrd::Int16(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Int32(s), ArrayOrd::Int32(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Int64(s), ArrayOrd::Int64(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::UInt8(s), ArrayOrd::UInt8(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::UInt16(s), ArrayOrd::UInt16(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::UInt32(s), ArrayOrd::UInt32(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::UInt64(s), ArrayOrd::UInt64(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Float32(s), ArrayOrd::Float32(o)) => {
                cmp(s, self.idx, o, other.idx, f32::total_cmp)
            }
            (ArrayOrd::Float64(s), ArrayOrd::Float64(o)) => {
                cmp(s, self.idx, o, other.idx, f64::total_cmp)
            }
            (ArrayOrd::String(s), ArrayOrd::String(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Binary(s), ArrayOrd::Binary(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::FixedSizeBinary(s), ArrayOrd::FixedSizeBinary(o)) => {
                cmp(s, self.idx, o, other.idx, Ord::cmp)
            }
            // For lists, we generate an iterator for each side that ranges over the correct
            // indices into the value buffer, then compare them lexicographically.
            (
                ArrayOrd::List(s_nulls, s_offset, s_values),
                ArrayOrd::List(o_nulls, o_offset, o_values),
            ) => {
                #[inline]
                fn range<'a>(
                    offsets: &OffsetBuffer<i32>,
                    values: &'a ArrayOrd,
                    idx: usize,
                ) -> impl Iterator<Item = ArrayIdx<'a>> {
                    let offsets = offsets.inner();
                    let from = offsets[idx].as_usize();
                    let to = offsets[idx + 1].as_usize();
                    (from..to).map(|i| values.at(i))
                }
                match (is_null(s_nulls, self.idx), is_null(o_nulls, other.idx)) {
                    (false, true) => Ordering::Less,
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, false) => range(s_offset, s_values, self.idx)
                        .cmp(range(o_offset, o_values, other.idx)),
                }
            }
            // For structs, we iterate over the same index in each field for each input,
            // comparing them lexicographically in order.
            (ArrayOrd::Struct(s_nulls, s_cols), ArrayOrd::Struct(o_nulls, o_cols)) => {
                match (is_null(s_nulls, self.idx), is_null(o_nulls, other.idx)) {
                    (false, true) => Ordering::Less,
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, false) => {
                        let s = s_cols.iter().map(|array| array.at(self.idx));
                        let o = o_cols.iter().map(|array| array.at(other.idx));
                        s.cmp(o)
                    }
                }
            }
            (_, _) => panic!("array types did not match"),
        }
    }
}

impl<'a> PartialOrd for ArrayIdx<'a> {
    fn partial_cmp(&self, other: &ArrayIdx) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PartialEq for ArrayIdx<'a> {
    fn eq(&self, other: &ArrayIdx) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'a> Eq for ArrayIdx<'a> {}
