//! Things pertaining to the DATA segment (mostly)
//!
//! Basic overview: DATA is arranged according to version-specific "layouts".
//! Each layout will enumerate all possible combinations for a given version,
//! which directly correspond to all valid combinations of $BYTEORD, $DATATYPE,
//! $PnB, $PnR, and $PnDATATYPE in the case of 3.2.
//!
//! Each layout may then be projected in a "reader" or "writer." Readers are
//! blank vectors waiting to accept data from disk. Writers are iterators that
//! read values from a dataframe and possibly convert them before writing.
//!
//! Now for the ugly bits.
//!
//! Layouts can first be classified by column width, where "fixed" layouts have
//! a single width per column and "delimited" layouts have a variable width. The
//! latter only corresponds to one layout: the case where $DATATYPE=A and all
//! $PnB=*. Values in such layouts will always be read as u64.
//!
//! Fixed layouts can further be classified by the type in each column:
//! 1) Single-type numeric layouts (aka "matrices")
//! 2) Fixed ASCII layouts
//! 3) Variable-width integer layouts
//! 4) Mixed layouts
//!
//! (1) is the simplest; each column is the same type which corresponds directly
//! with a native Rust type. This includes f32, f64, and uint ranging from 1 to
//! 8 bytes (including those that aren't powers of 2). Each type has a slightly
//! different reader/writer corresponding to distinct byte interpretations on
//! disk. (2) is similar in that the entire layout is one type; however, each
//! number is always read as u64 subject to the chars allowed by $PnB. (1)/(2)
//! are the only possibilities for FCS 2.0/3.0 since $BYTEORD restricts all $PnB
//! to the same width in the case of numeric $DATATYPE.
//!
//! (3) is a weird layout that almost nobody likely uses but is nonetheless
//! permitted starting with 3.1. Since $BYTEORD was changed to only mean
//! endian-ness, its relation to $PnB was severed. When DATATYPE=I, this means
//! $PnB may be changed freely, which allows different integer widths in each
//! column. In practice this makes the resulting data structure a dataframe (vs
//! a matrix).
//!
//! (4) was newly added to 3.2 by way of the PnDATATYPE keywords which now
//! allows the data layout to include any type. This obviously more complex but
//! is not computationally very different from (3).
//!
//! In addition to width, layouts may also be classified by whether $TOT is
//! known. In 2.0, $TOT is optional and may not be given. For delimited ASCII
//! layouts, not have $TOT means we need to parse until we reach the end of
//! DATA, hoping that all columns have the same length. For fixed layouts, we
//! can compute $TOT using $PnB and the length of DATA.

use crate::config::{
    AllowOptionalDropping, AllowTotMismatch, DisallowRangeTrunc, ReadLayoutConfig, ReaderConfig,
    StdTextReadConfig,
};
use crate::core::{
    AsScaleTransform, Measurements, ScaleTransform, TemporalsAndOpticals, VersionedMetaroot,
};
use crate::logging::{
    CommutativeResultIter as _, DeferredIter as _, DeferredSwitchableError,
    DeferredWarningAndError, DeferredWarningsAndError, ErrorGroup, ErrorsResult, GroupResult,
    IOErrorGroup, IOResult, ImpureError, LogResult, ResultExt as _, Success, SwitchableErrorResult,
    WarningOrErrorResult, WarningsAndErrorResult, WarningsAndErrorsResult,
    WarningsAndIOGroupResult, WarningsResult,
};
use crate::macros::{def_group, match_many_to_one};
use crate::nonempty::FCSNonEmpty;
use crate::segment::AnyDataSegment;
use crate::text::byteord::{
    BitsOrChars, ByteOrdToSizedError, Bytes, BytesError, Endian, HasByteOrd, NoByteOrd,
    NoByteOrd3_1, OrderedToEndianError, PrivBytes, SizedByteOrd, WidthToFixedError,
};
use crate::text::float_decimal::{DecimalToFloatError, FloatDecimal, HasFloatBounds};
use crate::text::index::{IndexFromOne, MeasIndex};
use crate::text::keywords::{
    AlphaNumType, ByteOrd2_0, ByteOrd3_1, DeprecatedDatatypeWarning, Gain, LookupDatatypeResult,
    NumType, Par, Range, RangeToIntError, Scale, Tot, Width,
};
use crate::text::lookup::{
    OptIndexedKey as _, OptIndexedKeyError, ReqIndexedKey as _, ReqIndexedKeyError, ReqKeyError,
    ReqMetarootKey as _,
};
use crate::text::named_vec::{NamedVec, NewNamedVecError};
use crate::text::optional::{Identity, KeywordPairMaybe as _, Nothing};
use crate::type_families::FunctorOnce as _;
use crate::validated::ascii_range::{AsciiRange, AsciiRangeFromKeywordsError, Chars};
use crate::validated::bitmask::{
    Bitmask, Bitmask08, Bitmask16, Bitmask24, Bitmask32, Bitmask40, Bitmask48, Bitmask56,
    Bitmask64, BitmaskLossError, BitmaskTruncationError,
};
use crate::validated::dataframe::{
    AllFCSCast, AnyFCSColumn, CastResult, FCSColIter, FCSColumn, FCSDataFrame, FCSDataType,
    LossError,
};
use crate::validated::keys::{IndexedKey as _, MeasHeader, NonStdKeywords, StdKeywords};

use ambassador::{Delegate, delegatable_trait};
use bigdecimal::BigDecimal;
use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use num_traits::PrimInt;
use thiserror::Error;

use std::convert::Infallible;
use std::fmt;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem;
use std::num::{NonZeroU8, ParseIntError};
use std::str;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
};

/// All possible byte layouts for the DATA segment in 2.0.
///
/// This is identical to 3.0 in every way except that the $TOT keyword in 2.0
/// is optional, which requires a different interface.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Option<Tot>>, generics = "'a")]
#[delegate(InterLayoutOps<Nothing<NumType>>)]
pub struct DataLayout2_0(pub AnyOrderedLayout<Option<Tot>>);

/// All possible byte layouts for the DATA segment in 2.0.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Identity<Tot>>, generics = "'a")]
#[delegate(InterLayoutOps<Nothing<NumType>>)]
pub struct DataLayout3_0(pub AnyOrderedLayout<Identity<Tot>>);

/// All possible byte layouts for the DATA segment in 3.1.
///
/// Unlike 2.0 and 3.0, the integer layout allows the column widths to be
/// different. This is a consequence of making BYTEORD only mean "big or little
/// endian" and have nothing to do with number of bytes.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Identity<Tot>>, generics = "'a")]
#[delegate(InterLayoutOps<Nothing<NumType>>)]
pub struct DataLayout3_1(pub NonMixedEndianLayout<Nothing<NumType>>);

/// All possible byte layouts for the DATA segment in 3.2.
///
/// In addition to the loosened integer layouts in 3.1, 3.2 additionally allows
/// each column to have a different type and size (hence "Mixed").
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Identity<Tot>>, generics = "'a")]
pub enum DataLayout3_2 {
    Mixed(MixedLayout),
    NonMixed(NonMixedEndianLayout<Option<NumType>>),
}

pub type MixedLayout = EndianLayout<NullMixedType, Option<NumType>>;

/// All possible byte layouts for the DATA segment in 2.0 and 3.0.
///
/// It is so named "Ordered" because the BYTEORD keyword represents any possible
/// byte ordering that may occur rather than simply little or big endian.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Tot>, generics = "'a, Tot")]
#[delegate(InterLayoutOps<Nothing<NumType>>)]
pub enum AnyOrderedLayout<T> {
    Ascii(AnyAsciiLayout<T, Nothing<NumType>, true>),
    Integer(AnyOrderedUintLayout<T>),
    F32(OrderedLayout<F32Range, T>),
    F64(OrderedLayout<F64Range, T>),
}

// TODO make an integer layout which has only one width, which will cover the
// vast majority of cases and make certain operations easier.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Identity<Tot>>, generics = "'a")]
#[delegate(InterLayoutOps<DT>, generics = "DT")]
pub enum NonMixedEndianLayout<D> {
    Ascii(AnyAsciiLayout<Identity<Tot>, D, false>),
    Integer(EndianLayout<AnyNullBitmask, D>),
    F32(EndianLayout<F32Range, D>),
    F64(EndianLayout<F64Range, D>),
}

pub type EndianLayout<C, D> = FixedLayout<C, Endian, Identity<Tot>, D>;

/// Byte layouts for ASCII data.
///
/// This may either be fixed (ie columns have the same number of characters)
/// or variable (ie columns have have different number of characters and are
/// separated by delimiters).
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Tot>, generics = "'a, Tot")]
#[delegate(InterLayoutOps<DT>, generics = "DT")]
pub enum AnyAsciiLayout<T, D, const ORD: bool> {
    Delimited(DelimAsciiLayout<T, D, ORD>),
    Fixed(FixedAsciiLayout<T, D, ORD>),
}

pub type FixedAsciiLayout<T, D, const ORD: bool> = FixedLayout<AsciiRange, NoByteOrd<ORD>, T, D>;

/// Byte layout for delimited ASCII.
#[derive(Clone, Default, PartialEq, new, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DelimAsciiLayout<T, D, const ORD: bool> {
    #[as_ref([u64])]
    ranges: Vec<u64>,
    #[cfg_attr(feature = "serde", serde(skip))]
    _tot_def: PhantomData<T>,
    #[cfg_attr(feature = "serde", serde(skip))]
    _meas_data_def: PhantomData<D>,
}

/// Byte layout where each column has a fixed width.
#[derive(Clone, AsRef, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FixedLayout<C, L, T, D> {
    columns: Vec<C>,
    #[as_ref(L)]
    byte_layout: L,
    #[cfg_attr(feature = "serde", serde(skip))]
    _tot_def: PhantomData<T>,
    #[cfg_attr(feature = "serde", serde(skip))]
    _meas_data_def: PhantomData<D>,
}

/// Byte layout for integers that may be in any byte order.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Tot>, generics = "'a, Tot")]
#[delegate(InterLayoutOps<Nothing<NumType>>)]
#[delegate(OrderedLayoutOps)]
pub enum AnyOrderedUintLayout<T> {
    // TODO the first two don't need to be ordered
    Uint08(OrderedLayout<Bitmask08, T>),
    Uint16(OrderedLayout<Bitmask16, T>),
    Uint24(OrderedLayout<Bitmask24, T>),
    Uint32(OrderedLayout<Bitmask32, T>),
    Uint40(OrderedLayout<Bitmask40, T>),
    Uint48(OrderedLayout<Bitmask48, T>),
    Uint56(OrderedLayout<Bitmask56, T>),
    Uint64(OrderedLayout<Bitmask64, T>),
}

pub type OrderedLayout<C, T> = FixedLayout<C, <C as HasNativeWidth>::Order, T, Nothing<NumType>>;

/// The type of a non-delimited column in the DATA segment for 3.2
#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum MixedType<A, U, F, D> {
    Ascii(A),
    Uint(U),
    F32(F),
    F64(D),
}

pub type NullMixedType = MixedType<AsciiRange, AnyNullBitmask, F32Range, F64Range>;

type ReaderMixedType = MixedType<
    ColumnReader<AsciiRange, u64, NoByteOrd3_1>,
    AnyReaderBitmask,
    ColumnReader<F32Range, f32, Endian>,
    ColumnReader<F64Range, f64, Endian>,
>;

type WriterMixedType<'a> = MixedType<
    ColumnWriter<'a, AsciiRange, u64, NoByteOrd3_1>,
    AnyWriterBitmask<'a>,
    ColumnWriter<'a, F32Range, f32, Endian>,
    ColumnWriter<'a, F64Range, f64, Endian>,
>;

/// A big or little-endian integer column of some size (1-8 bytes)
#[derive(Debug, PartialEq, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyBitmask<C08, C16, C24, C32, C40, C48, C56, C64> {
    Uint08(C08),
    Uint16(C16),
    Uint24(C24),
    Uint32(C32),
    Uint40(C40),
    Uint48(C48),
    Uint56(C56),
    Uint64(C64),
}

pub type AnyNullBitmask = AnyBitmask<
    Bitmask08,
    Bitmask16,
    Bitmask24,
    Bitmask32,
    Bitmask40,
    Bitmask48,
    Bitmask56,
    Bitmask64,
>;

type AnyReaderBitmask = AnyBitmask<
    UintColumnReader<Bitmask08>,
    UintColumnReader<Bitmask16>,
    UintColumnReader<Bitmask24>,
    UintColumnReader<Bitmask32>,
    UintColumnReader<Bitmask40>,
    UintColumnReader<Bitmask48>,
    UintColumnReader<Bitmask56>,
    UintColumnReader<Bitmask64>,
>;

type AnyWriterBitmask<'a> = AnyBitmask<
    UintColumnWriter<'a, Bitmask08>,
    UintColumnWriter<'a, Bitmask16>,
    UintColumnWriter<'a, Bitmask24>,
    UintColumnWriter<'a, Bitmask32>,
    UintColumnWriter<'a, Bitmask40>,
    UintColumnWriter<'a, Bitmask48>,
    UintColumnWriter<'a, Bitmask56>,
    UintColumnWriter<'a, Bitmask64>,
>;

type UintColumnReader<C> = ColumnReader<C, <C as HasNativeType>::Native, Endian>;

type UintColumnWriter<'a, C> = ColumnWriter<'a, C, <C as HasNativeType>::Native, Endian>;

/// The type of any floating point column in all versions
#[derive(PartialEq, Clone, new, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct FloatRange<T, const LEN: usize> {
    pub range: FloatDecimal<T>,
}

pub type F32Range = FloatRange<f32, 4>;
pub type F64Range = FloatRange<f64, 8>;

/// Instructions to read one column and store in a vector
struct ColumnReader<C, T, S> {
    column_type: C,
    data: Vec<T>,
    byte_layout: PhantomData<S>,
}

/// Instructions to write one column using an iterator
#[derive(new)]
struct ColumnWriter<'a, C, T, S> {
    column_type: C,
    data: AnySource<'a, T>,
    loss: Option<AnyLossError>,
    byte_layout: PhantomData<S>,
}

impl<C, T, S> ColumnWriter<'_, C, T, S> {
    fn into_err(self, i: MeasIndex) -> Option<IndexedLossError> {
        self.loss
            .map(|error| IndexedError::new(i, error))
            .map(IndexedLossError)
    }
}

/// A struct whose fields map 1-1 with keyword values in one data column
#[derive(new)]
pub struct ColumnLayoutValues<D> {
    width: Width,
    range: Range,
    datatype: D,
}

type ColumnLayoutValues2_0 = ColumnLayoutValues<Nothing<NumType>>;
type ColumnLayoutValues3_2 = ColumnLayoutValues<Option<NumType>>;

pub trait IsNumType: Sized {
    fn lookup_datatype(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> DeferredSwitchableError<Self, AllowOptionalDropping, OptIndexedKeyError<NumType>>;

    fn lookup_datatype_ro(
        kws: &StdKeywords,
        i: MeasIndex,
    ) -> DeferredWarningAndError<Self, OptIndexedKeyError<NumType>, OptIndexedKeyError<NumType>>;

    fn lookup_all(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        par: Par,
        conf: &StdTextReadConfig,
    ) -> LookupMeasLayoutResult<Self, LookupStdMeasLayoutError> {
        (0..par.0)
            .map(|i| Self::lookup_one(std, nonstd, i.into(), conf))
            .mappend_commutative()
    }

    fn lookup_ro_all(kws: &StdKeywords) -> LookupMeasLayoutResult<Self, LookupRawMeasLayoutError> {
        Par::get_metaroot_req(kws)
            .map_err(LookupRawMeasLayoutError::from)
            .into_log()
            .and_then_commutative(|par| {
                (0..par.0)
                    .map(|i| Self::lookup_one_ro(kws, i.into()))
                    .mappend_commutative()
            })
    }

    fn lookup_one(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> WarningsAndErrorsResult<
        ColumnLayoutValues<Self>,
        (),
        OptIndexedKeyError<NumType>,
        LookupStdMeasLayoutError,
    > {
        let w = Width::remove_meas_req(std, i)
            .map_err(LookupStdMeasLayoutError::from)
            .into_nowarn();
        let r = Range::remove_meas_req(std, i)
            .map_err(LookupStdMeasLayoutError::from)
            .into_nowarn();
        w.zip_commutative(r)
            .nowarn_into_warn()
            .and_then_commutative(|(width, range)| {
                Self::lookup_datatype(std, nonstd, i, conf)
                    .switchable_into_commutative()
                    .map_errors(LookupStdMeasLayoutError::from)
                    .into_semigroup()
                    .map_ok_value(|datatype| ColumnLayoutValues::new(width, range, datatype))
                    .set_err_value(())
            })
    }

    fn lookup_one_ro(
        kws: &StdKeywords,
        i: MeasIndex,
    ) -> WarningsAndErrorsResult<
        ColumnLayoutValues<Self>,
        (),
        OptIndexedKeyError<NumType>,
        LookupRawMeasLayoutError,
    > {
        let w = Width::get_meas_req(kws, i)
            .map_err(LookupRawMeasLayoutError::from)
            .into_nowarn();
        let r = Range::get_meas_req(kws, i)
            .map_err(LookupRawMeasLayoutError::from)
            .into_nowarn();
        w.zip_commutative(r)
            .nowarn_into_warn()
            .and_then_commutative(|(width, range)| {
                Self::lookup_datatype_ro(kws, i)
                    .repack::<_, _, Vec<_>>()
                    .map_ok_value(|datatype| ColumnLayoutValues::new(width, range, datatype))
                    .map_errors(LookupRawMeasLayoutError::from)
                    .set_err_value(())
            })
    }
}

/// Methods for a type which may or may not have $TOT
pub trait IsTot: Sized {
    fn with_tot<F, G, I, X>(input: I, tot: Self, tot_f: F, notot_f: G) -> X
    where
        F: FnOnce(I, Tot) -> X,
        G: FnOnce(I) -> X;

    fn check_tot(
        total_events: u64,
        tot: Self,
        flag: AllowTotMismatch,
    ) -> SwitchableErrorResult<(), (), AllowTotMismatch, TotEventMismatch> {
        Self::with_tot(
            (),
            tot,
            |(), t| Self::check_tot_inner(total_events, t, flag),
            |()| LogResult::new_switchable_ok((), flag),
        )
    }

    #[must_use]
    fn check_tot_inner(
        total_events: u64,
        tot: Tot,
        flag: AllowTotMismatch,
    ) -> SwitchableErrorResult<(), (), AllowTotMismatch, TotEventMismatch> {
        let count = usize::try_from(total_events)
            .expect("event count exceeded maximum platform pointer size");
        let i = TotEventMismatch { tot, total_events };
        LogResult::new_switchable_ok_if(tot.0 == count, (), (), i, flag)
    }
}

/// Standardized operations on layouts
#[delegatable_trait]
pub trait LayoutOps<'a, T>: Sized {
    fn ncols(&self) -> usize;

    fn nbytes(&self, df: &FCSDataFrame) -> u64;

    fn ranges(&self) -> Vec<Range>;

    fn datatype(&self) -> AlphaNumType;

    fn datatypes(&self) -> Vec<AlphaNumType>;

    fn byteord_keyword(&self) -> (String, String);

    fn req_keywords(&self) -> [(String, String); 2] {
        [self.datatype().pair(), self.byteord_keyword()]
    }

    fn req_meas_keywords(&self) -> Vec<[(String, String); 2]>;

    // TODO in theory this could return the thing we removed, but it doesn't
    // seem like we have a use for it now and it would likely make this trait
    // more more complex as we would need an associated type
    fn remove_nocheck(&mut self, index: MeasIndex);

    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut Vec<u8>,
        tot: T,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> WarningsAndIOGroupResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError, ()>
    where
        T: IsTot;

    fn check_writer(&self, df: &'a FCSDataFrame) -> ErrorsResult<(), (), IndexedLossError>;

    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &'a FCSDataFrame,
        skip_conv_check: bool,
    ) -> DeferredWarningsAndError<(), IndexedLossError, io::Error>;

    fn check_transforms_and_len<S, G>(&self, xforms: &[S]) -> Result<(), MeasLayoutMismatchError>
    where
        G: Default,
        S: CheckedScaleTransform,
        MeasLayoutMismatchError: From<ErrorGroup<S::Err, G>>,
    {
        let meas_n = xforms.len();
        let layout_n = self.ncols();
        if meas_n != layout_n {
            let e = MeasLayoutLengthsError { meas_n, layout_n };
            return Err(e.into());
        }
        self.check_transforms(xforms)?;
        Ok(())
    }

    // TODO this should be private
    fn check_transforms<S, G>(&self, xforms: &[S]) -> GroupResult<(), S::Err, G>
    where
        S: CheckedScaleTransform,
        G: Default,
    {
        // ASSUME measurements and layout columns are the same length
        let ds = self.datatypes();
        let es = ds
            .iter()
            .zip(xforms)
            .enumerate()
            // Only integers are allowed to have gain and log scaling, so
            // everything else should be a "noop" transform (ie a linear
            // transform with slope of 1.0). NOTE the standard itself is
            // vague about what should happen to ASCII values (presumably
            // since nobody cares) so here we just treat them like we treat
            // floating point types to keep the logic simple.
            .filter_map(|(i, (&datatype, s))| s.matches_datatype(datatype, i.into()).err());
        ErrorGroup::try_new(es)
    }

    fn truncate_df(
        &self,
        df: &'a FCSDataFrame,
        skip_conv_check: bool,
    ) -> WarningsResult<FCSDataFrame, IndexedLossError>;
}

#[delegatable_trait]
pub trait InterLayoutOps<D> {
    fn opt_meas_headers(&self) -> Vec<MeasHeader>;

    fn opt_meas_keywords(&self) -> Vec<Vec<(String, Option<String>)>>;

    // no need to check since this will be done after validating that the index
    // is within the measurement vector, which has its own check and should
    // always be the same length
    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> SwitchableErrorResult<(), (), DisallowRangeTrunc, InsertRangeError>;

    fn push(
        &mut self,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> SwitchableErrorResult<(), (), DisallowRangeTrunc, InsertRangeError>;

    fn clear(&mut self);
}

/// Standardized operations on layouts
#[delegatable_trait]
pub trait OrderedLayoutOps: Sized {
    fn byte_order(&self) -> ByteOrd2_0;

    fn endianness(&self) -> Option<Endian> {
        self.byte_order().try_into().ok()
    }
}

/// A version-specific data layout
pub trait VersionedDataLayout
where
    for<'a> Self: Sized + LayoutOps<'a, Self::Tot> + InterLayoutOps<Self::NumType>,
{
    type ByteLayout;
    type NumType: IsNumType;
    type Tot: IsTot;

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
        par: Par,
    ) -> LookupStdLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>;

    // TODO could make errors and api simpler if we pass par here like above
    fn lookup_ro<C>(kws: &StdKeywords, conf: &C) -> LookupRawLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig>;

    fn new_empty(datatype: AlphaNumType) -> Self;

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Self::NumType>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), NewMixedTypeWarning, NewDataLayoutError>;

    fn h_read_df<R: Read + Seek>(
        &self,
        h: &mut BufReader<R>,
        tot: Self::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> WarningsAndIOGroupResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError, ()> {
        // The only purpose of this buffer is to read ASCII since we don't
        // hardcode the buffer width into the type (unlike integers and floats).
        // It's passed down to each layer of the read stack to avoid making the
        // buffer argument generic, which would make this implementation much
        // more complex. Good enough to pass the buffer and only use it when
        // needed.
        let mut buf = vec![];
        // TODO why return default rather than fail?
        seg.as_u64().try_coords().map_or(
            LogResult::new_ok(FCSDataFrame::default()),
            |(begin, _)| {
                h.seek(SeekFrom::Start(begin))
                    .map_err(IOErrorGroup::from)
                    .into_log()
                    .nowarn_and_then(|_| self.h_read_df_inner(h, &mut buf, tot, seg, conf))
            },
        )
    }

    fn h_write_df<W>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        skip_conv_check: bool,
    ) -> WarningsAndErrorResult<(), (), IndexedLossError, io::Error>
    where
        W: Write,
    {
        // The dataframe should be encapsulated such that a) the column number
        // matches the number of measurements. If these are not true, the code
        // is wrong.
        let par = self.ncols();
        let ncols = df.ncols();
        debug_assert!(
            ncols == par,
            "dataframe columns ({ncols}) unequal to number of measurements ({par})"
        );
        self.h_write_df_inner(h, df, skip_conv_check)
    }

    fn check_measurement_vector<N, T, O: AsScaleTransform>(
        &self,
        meas: &Measurements<N, T, O>,
    ) -> Result<(), MeasLayoutMismatchError> {
        let xforms: Vec<_> = meas
            .iter_with(&|_, _| ScaleTransform::default(), &|_, m| {
                m.value.as_transform()
            })
            .collect();
        self.check_transforms_and_len(&xforms[..])
    }

    #[allow(clippy::type_complexity)]
    fn try_new_measurements<M: VersionedMetaroot>(
        &self,
        measurements: TemporalsAndOpticals<M>,
    ) -> Result<Measurements<M::Name, M::Temporal, M::Optical>, MeasurementsWithLayoutError>
    where
        M::Optical: AsScaleTransform,
    {
        let ms = NamedVec::try_new(measurements)?;
        self.check_measurement_vector(&ms)
            .map_err(MeasurementsWithLayoutError::from)?;
        Ok(ms)
    }
}

/// Convert layout to new FCS version
pub trait ConvertFromLayout<T>: Sized
where
    Self: VersionedDataLayout,
{
    fn convert_from_layout(value: T) -> LayoutConvertResult<Self>;
}

pub trait CheckedScaleTransform {
    type Err;

    fn matches_datatype(&self, datatype: AlphaNumType, i: MeasIndex) -> Result<(), Self::Err>;
}

pub trait HasNativeType: Sized {
    /// The native rust type
    type Native: Default + Copy;
}

/// A type which uses a defined number of bytes
pub trait HasNativeWidth: HasNativeType {
    /// The length of the type in an FCS file (may be less than native)
    const BYTES: Bytes;

    /// The length of the native Rust type
    const LEN: usize;

    /// The sized byte order to be used with this type
    type Order;
}

/// A column which has only one $DATATYPE
pub trait HasOneDatatype: Sized {
    const DATATYPE: AlphaNumType;
}

/// A column which has a $DATATYPE keyword
pub trait HasDatatype: Sized {
    fn datatype(&self) -> AlphaNumType;

    fn datatype_from_columns(cs: &[Self]) -> AlphaNumType;
}

trait FromRange: Sized {
    type Error;

    fn from_range(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<Self, DisallowRangeTrunc, Self::Error>;
}

/// A type which has a width that may vary
pub trait IsFixed {
    fn nbytes(&self) -> NonZeroU8;

    fn fixed_width(&self) -> BitsOrChars;

    fn range(&self) -> Range;

    fn req_meas_keywords(&self, i: MeasIndex) -> [(String, String); 2] {
        [
            Width::Fixed(self.fixed_width()).meas_pair(i),
            self.range().meas_pair(i),
        ]
    }
}

/// A column which may be transformed into a reader for a rust numeric type
trait ToNativeReader: HasNativeType {
    fn into_native_reader<S>(self, nrows: usize) -> ColumnReader<Self, Self::Native, S>
    where
        Self::Native: Default + Copy,
    {
        ColumnReader {
            column_type: self,
            data: vec![Self::Native::default(); nrows],
            byte_layout: PhantomData,
        }
    }
}

trait NativeReadable<S>: HasNativeType {
    fn h_read_native<R: Read>(
        &self,
        h: &mut BufReader<R>,
        byte_layout: S,
        buf: &mut Vec<u8>,
    ) -> IOResult<Self::Native, ReadDataframeError>;
}

/// A column which may be transformed into a writer for a rust numeric type
trait ToNativeWriter
where
    Self: HasNativeType,
{
    type Error;

    fn into_native_writer<'a, S>(
        self,
        c: &'a AnyFCSColumn,
    ) -> ColumnWriter<'a, Self, Self::Native, S>
    where
        Self::Native: Default + Copy + AllFCSCast,
        AnySource<'a, Self::Native>: From<FCSColIter<'a, u8, Self::Native>>
            + From<FCSColIter<'a, u16, Self::Native>>
            + From<FCSColIter<'a, u32, Self::Native>>
            + From<FCSColIter<'a, u64, Self::Native>>
            + From<FCSColIter<'a, f32, Self::Native>>
            + From<FCSColIter<'a, f64, Self::Native>>,
    {
        ColumnWriter::new(self, AnySource::new(c), None)
    }

    fn check_native_writer(&self, col: &AnyFCSColumn) -> Result<(), LossError<Self::Error>>
    where
        Self::Native: Default + Copy + AllFCSCast,
    {
        col.check_writer(|x| Self::check_other_loss(self, x))
    }

    fn check_other_loss(&self, x: Self::Native) -> Option<Self::Error>;
}

trait IntoReader<S> {
    type Target: Readable<S>;

    fn into_reader(self, nrows: usize) -> Self::Target;
}

trait Readable<S> {
    fn into_dataframe_column(self) -> AnyFCSColumn;

    fn h_read<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        row: usize,
        byte_layout: S,
        buf: &mut Vec<u8>,
    ) -> IOResult<(), ReadDataframeError>;
}

trait NativeWritable<S>: HasNativeType {
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<Self::Native>,
        byte_layout: S,
    ) -> io::Result<Option<AnyLossError>>;
}

trait IntoWriter<'a, S> {
    type Target: Writable<'a, S>;

    fn into_writer(self, col: &'a AnyFCSColumn) -> Self::Target;

    fn check_writer(&self, col: &'a AnyFCSColumn) -> Result<(), AnyLossError>;
}

trait Writable<'a, S> {
    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: S) -> io::Result<()>;

    fn truncate(self, skip_conv_check: bool) -> (AnyFCSColumn, Option<AnyLossError>);

    fn into_err(self, i: MeasIndex) -> Option<IndexedLossError>;
}

trait Castable: Sized + HasNativeType {
    fn with_cast(&self, x: CastResult<Self::Native>) -> (Self::Native, Option<AnyLossError>);
}

/// General methods for each numeric type.
///
/// This is mostly for converting to/from bytes with various endian-ness.
// TODO clean this up with https://github.com/rust-lang/rust/issues/76560 once
// it lands in a stable compiler, in theory there is no reason to put the length
// of the type as a parameter, but the current compiler is not smart enough
trait NumProps: Sized + Copy + Default {
    const LEN: usize;
    type BUF: AsRef<[u8]> + AsMut<[u8]> + Default;

    fn read_buf<R: Read>(h: &mut BufReader<R>) -> io::Result<Self::BUF>;

    fn from_big(buf: Self::BUF) -> Self;

    fn from_little(buf: Self::BUF) -> Self;

    fn from_endian(buf: Self::BUF, endian: Endian) -> Self {
        match endian {
            Endian::Big => Self::from_big(buf),
            Endian::Little => Self::from_little(buf),
        }
    }

    fn to_big(self) -> Self::BUF;

    fn to_little(self) -> Self::BUF;

    fn to_endian(self, endian: Endian) -> Self::BUF {
        match endian {
            Endian::Big => self.to_big(),
            Endian::Little => self.to_little(),
        }
    }
}

/// Methods for reading numbers which may be in arbitrary byte orders.
trait OrderedFromBytes<const OLEN: usize>: NumProps {
    fn h_read_from_ordered<R: Read>(h: &mut BufReader<R>, order: [u8; OLEN]) -> io::Result<Self> {
        let mut tmp = [0; OLEN];
        let mut buf = Self::BUF::default();
        h.read_exact(&mut tmp)?;
        for (i, j) in order.iter().enumerate() {
            buf.as_mut()[usize::from(*j)] = tmp[i];
        }
        Ok(Self::from_little(buf))
    }

    fn h_write_from_ordered<W: Write>(
        self,
        h: &mut BufWriter<W>,
        order: [u8; OLEN],
    ) -> io::Result<()> {
        let tmp = Self::to_little(self);
        let mut buf = [0; OLEN];
        for (i, j) in order.iter().enumerate() {
            buf[usize::from(*j)] = tmp.as_ref()[i];
        }
        h.write_all(tmp.as_ref())
    }
}

/// Methods for reading/writing integers (1-8 bytes) from FCS files.
trait IntFromBytes<const INTLEN: usize>: NumProps + OrderedFromBytes<INTLEN> {
    fn h_read_endian<R: Read>(h: &mut BufReader<R>, endian: Endian) -> io::Result<Self> {
        // Read data that is not a power-of-two bytes long. Start by reading n
        // bytes into a vector, which can take a varying size. Then copy this
        // into the power of 2 buffer which will go to one or the other end of
        // the buffer depending on endianness.
        let mut tmp = [0; INTLEN];
        let mut buf = Self::BUF::default();
        h.read_exact(&mut tmp)?;
        Ok(if endian == Endian::Big {
            let b = Self::LEN - INTLEN;
            buf.as_mut()[b..].copy_from_slice(&tmp);
            Self::from_big(buf)
        } else {
            buf.as_mut()[..INTLEN].copy_from_slice(&tmp);
            Self::from_little(buf)
        })
    }

    fn h_read_ordered<R: Read>(
        h: &mut BufReader<R>,
        byteord: SizedByteOrd<INTLEN>,
    ) -> io::Result<Self> {
        match byteord {
            SizedByteOrd::Endian(e) => Self::h_read_endian(h, e),
            SizedByteOrd::Order(order) => Self::h_read_from_ordered(h, order),
        }
    }

    fn h_write_endian<W: Write>(self, h: &mut BufWriter<W>, endian: Endian) -> io::Result<()> {
        let mut buf = [0; INTLEN];
        let (start, end, tmp) = if endian == Endian::Big {
            ((Self::LEN - INTLEN), Self::LEN, Self::to_big(self))
        } else {
            (0, INTLEN, Self::to_little(self))
        };
        buf[..].copy_from_slice(&tmp.as_ref()[start..end]);
        h.write_all(&buf)
    }

    fn h_write_ordered<W: Write>(
        self,
        h: &mut BufWriter<W>,
        byteord: SizedByteOrd<INTLEN>,
    ) -> io::Result<()> {
        match byteord {
            SizedByteOrd::Endian(e) => self.h_write_endian(h, e),
            SizedByteOrd::Order(o) => self.h_write_from_ordered(h, o),
        }
    }
}

/// Methods for reading/writing floats (32 and 64 bit) from FCS files.
trait FloatFromBytes<const LEN: usize>: NumProps + OrderedFromBytes<LEN> {
    fn h_read_endian<R: Read>(h: &mut BufReader<R>, endian: Endian) -> io::Result<Self> {
        let buf = Self::read_buf(h)?;
        Ok(Self::from_endian(buf, endian))
    }

    fn h_read_ordered<R: Read>(
        h: &mut BufReader<R>,
        byteord: SizedByteOrd<LEN>,
    ) -> io::Result<Self> {
        match byteord {
            SizedByteOrd::Endian(endian) => Self::h_read_endian(h, endian),
            SizedByteOrd::Order(order) => Self::h_read_from_ordered(h, order),
        }
    }

    fn h_write_endian<W: Write>(self, h: &mut BufWriter<W>, endian: Endian) -> io::Result<()> {
        let buf = Self::to_endian(self, endian);
        h.write_all(buf.as_ref())
    }

    fn h_write_ordered<W: Write>(
        self,
        h: &mut BufWriter<W>,
        byteord: SizedByteOrd<LEN>,
    ) -> io::Result<()> {
        match byteord {
            SizedByteOrd::Endian(endian) => self.h_write_endian(h, endian),
            SizedByteOrd::Order(order) => self.h_write_from_ordered(h, order),
        }
    }
}

macro_rules! match_any_uint {
    ($value:expr, $root:ident, $inner:ident, $action:block) => {
        match_many_to_one!(
            $value,
            $root,
            [
                Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64
            ],
            $inner,
            $action
        )
    };
}

macro_rules! match_any_mixed {
    ($value:expr, $inner:ident, $action:block) => {
        match_many_to_one!($value, MixedType, [Ascii, Uint, F32, F64], $inner, $action)
    };
}

macro_rules! impl_any_uint {
    ($var:ident, $bitmask:path) => {
        impl From<$bitmask> for AnyNullBitmask {
            fn from(value: $bitmask) -> Self {
                Self::$var(value)
            }
        }

        impl From<UintColumnReader<$bitmask>> for AnyReaderBitmask {
            fn from(value: UintColumnReader<$bitmask>) -> Self {
                Self::$var(value)
            }
        }

        impl<'a> From<UintColumnWriter<'a, $bitmask>> for AnyWriterBitmask<'a> {
            fn from(value: UintColumnWriter<'a, $bitmask>) -> Self {
                Self::$var(value)
            }
        }

        impl TryFrom<AnyNullBitmask> for $bitmask {
            type Error = UintToUintError;
            fn try_from(value: AnyNullBitmask) -> Result<Self, Self::Error> {
                let w = value.nbytes();
                if let AnyBitmask::$var(x) = value {
                    Ok(x)
                } else {
                    Err(UintToUintError::new(w, Self::BYTES.into()))
                }
            }
        }

        impl TryFrom<NullMixedType> for $bitmask {
            type Error = MixedToOrderedUintError;
            fn try_from(value: NullMixedType) -> Result<Self, Self::Error> {
                let w = value.nbytes();
                if let MixedType::Uint(x) = value {
                    if let AnyBitmask::$var(y) = x {
                        Ok(y)
                    } else {
                        Err(UintToUintError::new(w, Self::BYTES.into()).into())
                    }
                } else {
                    let dest_type = value.as_alpha_num_type();
                    Err(MixedToNonMixedError::new(dest_type, value).into())
                }
            }
        }
    };
}

impl_any_uint!(Uint08, Bitmask08);
impl_any_uint!(Uint16, Bitmask16);
impl_any_uint!(Uint24, Bitmask24);
impl_any_uint!(Uint32, Bitmask32);
impl_any_uint!(Uint40, Bitmask40);
impl_any_uint!(Uint48, Bitmask48);
impl_any_uint!(Uint56, Bitmask56);
impl_any_uint!(Uint64, Bitmask64);

impl From<AsciiRange> for NullMixedType {
    fn from(value: AsciiRange) -> Self {
        Self::Ascii(value)
    }
}

impl From<AnyNullBitmask> for NullMixedType {
    fn from(value: AnyNullBitmask) -> Self {
        Self::Uint(value)
    }
}

impl<T, const LEN: usize> From<Bitmask<T, LEN>> for NullMixedType
where
    AnyNullBitmask: From<Bitmask<T, LEN>>,
{
    fn from(value: Bitmask<T, LEN>) -> Self {
        Self::Uint(value.into())
    }
}

impl From<F32Range> for NullMixedType {
    fn from(value: F32Range) -> Self {
        Self::F32(value)
    }
}

impl From<F64Range> for NullMixedType {
    fn from(value: F64Range) -> Self {
        Self::F64(value)
    }
}

impl From<ColumnReader<AsciiRange, u64, NoByteOrd<false>>> for ReaderMixedType {
    fn from(value: ColumnReader<AsciiRange, u64, NoByteOrd<false>>) -> Self {
        Self::Ascii(value)
    }
}

impl From<AnyReaderBitmask> for ReaderMixedType {
    fn from(value: AnyReaderBitmask) -> Self {
        Self::Uint(value)
    }
}

impl From<ColumnReader<F32Range, f32, Endian>> for ReaderMixedType {
    fn from(value: ColumnReader<F32Range, f32, Endian>) -> Self {
        Self::F32(value)
    }
}

impl From<ColumnReader<F64Range, f64, Endian>> for ReaderMixedType {
    fn from(value: ColumnReader<F64Range, f64, Endian>) -> Self {
        Self::F64(value)
    }
}

impl<'a> From<ColumnWriter<'a, AsciiRange, u64, NoByteOrd<false>>> for WriterMixedType<'a> {
    fn from(value: ColumnWriter<'a, AsciiRange, u64, NoByteOrd<false>>) -> Self {
        Self::Ascii(value)
    }
}

impl<'a> From<AnyWriterBitmask<'a>> for WriterMixedType<'a> {
    fn from(value: AnyWriterBitmask<'a>) -> Self {
        Self::Uint(value)
    }
}

impl<'a> From<ColumnWriter<'a, F32Range, f32, Endian>> for WriterMixedType<'a> {
    fn from(value: ColumnWriter<'a, F32Range, f32, Endian>) -> Self {
        Self::F32(value)
    }
}

impl<'a> From<ColumnWriter<'a, F64Range, f64, Endian>> for WriterMixedType<'a> {
    fn from(value: ColumnWriter<'a, F64Range, f64, Endian>) -> Self {
        Self::F64(value)
    }
}

impl IsNumType for Nothing<NumType> {
    fn lookup_datatype(
        _: &mut StdKeywords,
        _: &mut NonStdKeywords,
        _: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> DeferredSwitchableError<Self, AllowOptionalDropping, OptIndexedKeyError<NumType>> {
        LogResult::new_switchable_ok(Self::default(), conf.allow_optional_dropping)
    }

    fn lookup_datatype_ro(
        _: &StdKeywords,
        _: MeasIndex,
    ) -> DeferredWarningAndError<Self, OptIndexedKeyError<NumType>, OptIndexedKeyError<NumType>>
    {
        LogResult::new_ok(Self::default())
    }
}

impl IsNumType for Option<NumType> {
    fn lookup_datatype(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> DeferredSwitchableError<Self, AllowOptionalDropping, OptIndexedKeyError<NumType>> {
        NumType::drop_meas_opt(std, nonstd, i, conf)
    }

    // TODO make these "get" functions "drop" (really ignore) errors based on config
    fn lookup_datatype_ro(
        kws: &StdKeywords,
        i: MeasIndex,
    ) -> DeferredWarningAndError<Self, OptIndexedKeyError<NumType>, OptIndexedKeyError<NumType>>
    {
        NumType::get_meas_opt(kws, i).into_succ()
    }
}

impl IsTot for Option<Tot> {
    fn with_tot<F, G, I, X>(input: I, tot: Self, tot_f: F, notot_f: G) -> X
    where
        F: FnOnce(I, Tot) -> X,
        G: FnOnce(I) -> X,
    {
        if let Some(t) = tot {
            tot_f(input, t)
        } else {
            notot_f(input)
        }
    }
}

impl IsTot for Identity<Tot> {
    fn with_tot<F, G, I, X>(input: I, tot: Self, tot_f: F, _: G) -> X
    where
        F: FnOnce(I, Tot) -> X,
        G: FnOnce(I) -> X,
    {
        tot_f(input, tot.0)
    }
}

impl From<&NullMixedType> for Range {
    fn from(value: &NullMixedType) -> Self {
        match_any_mixed!(value, x, { x.into() })
    }
}

impl From<&AnyNullBitmask> for Range {
    fn from(value: &AnyNullBitmask) -> Self {
        match_any_uint!(value, AnyNullBitmask, x, { x.into() })
    }
}

impl<T: Clone, const LEN: usize> From<&FloatRange<T, LEN>> for Range {
    fn from(value: &FloatRange<T, LEN>) -> Self {
        value.range.clone().into()
    }
}

macro_rules! mixed_to_inner {
    ($inner:ident, $var:ident) => {
        impl TryFrom<NullMixedType> for $inner {
            type Error = MixedToNonMixedError;
            fn try_from(value: NullMixedType) -> Result<Self, Self::Error> {
                let dest_type = value.as_alpha_num_type();
                if let MixedType::$var(x) = value {
                    Ok(x)
                } else {
                    Err(MixedToNonMixedError::new(dest_type, value))
                }
            }
        }
    };
}

mixed_to_inner!(AsciiRange, Ascii);
mixed_to_inner!(AnyNullBitmask, Uint);
mixed_to_inner!(F32Range, F32);
mixed_to_inner!(F64Range, F64);

impl<T, const LEN: usize> ToNativeReader for Bitmask<T, LEN> where Self: HasNativeType<Native = T> {}

impl<T, const LEN: usize> ToNativeReader for FloatRange<T, LEN> where Self: HasNativeType<Native = T>
{}

impl ToNativeReader for AsciiRange {}

impl<T, const LEN: usize> NativeReadable<Endian> for Bitmask<T, LEN>
where
    Self: HasNativeType<Native = T>,
    T: Ord + Copy + IntFromBytes<LEN>,
{
    fn h_read_native<R: Read>(
        &self,
        h: &mut BufReader<R>,
        byte_layout: Endian,
        _: &mut Vec<u8>,
    ) -> IOResult<T, ReadDataframeError> {
        Ok(T::h_read_endian(h, byte_layout)?)
    }
}

impl<T, const LEN: usize> NativeReadable<SizedByteOrd<LEN>> for Bitmask<T, LEN>
where
    Self: HasNativeType<Native = T>,
    T: Ord + Copy + IntFromBytes<LEN>,
{
    fn h_read_native<R: Read>(
        &self,
        h: &mut BufReader<R>,
        byte_layout: SizedByteOrd<LEN>,
        _: &mut Vec<u8>,
    ) -> IOResult<T, ReadDataframeError> {
        Ok(T::h_read_ordered(h, byte_layout)?)
    }
}

impl<T, const LEN: usize> NativeReadable<Endian> for FloatRange<T, LEN>
where
    Self: HasNativeType<Native = T>,
    T: Copy + FloatFromBytes<LEN>,
{
    fn h_read_native<R: Read>(
        &self,
        h: &mut BufReader<R>,
        byte_layout: Endian,
        _: &mut Vec<u8>,
    ) -> IOResult<T, ReadDataframeError> {
        Ok(T::h_read_endian(h, byte_layout)?)
    }
}

impl<T, const LEN: usize> NativeReadable<SizedByteOrd<LEN>> for FloatRange<T, LEN>
where
    Self: HasNativeType<Native = T>,
    T: Copy + FloatFromBytes<LEN>,
{
    fn h_read_native<R: Read>(
        &self,
        h: &mut BufReader<R>,
        byte_layout: SizedByteOrd<LEN>,
        _: &mut Vec<u8>,
    ) -> IOResult<T, ReadDataframeError> {
        Ok(T::h_read_ordered(h, byte_layout)?)
    }
}

impl<const ORD: bool> NativeReadable<NoByteOrd<ORD>> for AsciiRange {
    fn h_read_native<R: Read>(
        &self,
        h: &mut BufReader<R>,
        _: NoByteOrd<ORD>,
        buf: &mut Vec<u8>,
    ) -> IOResult<Self::Native, ReadDataframeError> {
        buf.clear();
        h.take(u8::from(self.chars()).into()).read_to_end(buf)?;
        ascii_to_uint(buf)
            .map_err(ReadFixedAsciiError::from)
            .map_err(ReadAsciiError::from)
            .map_err(ReadDataframeError::from)
            .map_err(ImpureError::Pure)
    }
}

impl<C, S> IntoReader<S> for C
where
    AnyFCSColumn: From<FCSColumn<C::Native>>,
    C: NativeReadable<S> + ToNativeReader,
{
    type Target = ColumnReader<C, C::Native, S>;

    fn into_reader(self, nrows: usize) -> Self::Target {
        self.into_native_reader(nrows)
    }
}

impl IntoReader<Endian> for AnyNullBitmask {
    type Target = AnyReaderBitmask;

    fn into_reader(self, nrows: usize) -> Self::Target {
        match_any_uint!(self, Self, c, { c.into_native_reader(nrows).into() })
    }
}

impl IntoReader<Endian> for NullMixedType {
    type Target = ReaderMixedType;

    fn into_reader(self, nrows: usize) -> Self::Target {
        match_any_mixed!(self, c, { c.into_reader(nrows).into() })
    }
}

impl<C, T, S> Readable<S> for ColumnReader<C, T, S>
where
    T: Copy + Default,
    C: NativeReadable<S> + HasNativeType<Native = T> + ToNativeReader,
    AnyFCSColumn: From<FCSColumn<T>>,
{
    fn into_dataframe_column(self) -> AnyFCSColumn {
        FCSColumn::from(self.data).into()
    }

    fn h_read<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        row: usize,
        byte_layout: S,
        buf: &mut Vec<u8>,
    ) -> IOResult<(), ReadDataframeError> {
        let x = self.column_type.h_read_native(h, byte_layout, buf)?;
        self.data[row] = x;
        Ok(())
    }
}

impl Readable<Endian> for ReaderMixedType {
    fn into_dataframe_column(self) -> AnyFCSColumn {
        match_any_mixed!(self, c, { c.into_dataframe_column() })
    }

    fn h_read<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        row: usize,
        byte_layout: Endian,
        buf: &mut Vec<u8>,
    ) -> IOResult<(), ReadDataframeError> {
        match self {
            Self::Ascii(c) => c.h_read(h, row, NoByteOrd, buf),
            Self::Uint(c) => c.h_read(h, row, byte_layout, buf),
            Self::F32(c) => c.h_read(h, row, byte_layout, buf),
            Self::F64(c) => c.h_read(h, row, byte_layout, buf),
        }
    }
}

impl Readable<Endian> for AnyReaderBitmask {
    fn into_dataframe_column(self) -> AnyFCSColumn {
        match_any_uint!(self, AnyBitmask, c, { c.into_dataframe_column() })
    }

    fn h_read<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        row: usize,
        byte_layout: Endian,
        buf: &mut Vec<u8>,
    ) -> IOResult<(), ReadDataframeError> {
        match_any_uint!(self, AnyBitmask, c, { c.h_read(h, row, byte_layout, buf) })
    }
}

impl<T, const LEN: usize> Castable for Bitmask<T, LEN>
where
    Self: HasNativeType<Native = T>,
    T: Copy + Ord,
    u64: From<T>,
{
    fn with_cast(&self, x: CastResult<T>) -> (T, Option<AnyLossError>) {
        let (trunc, y) = self.apply(x.new);
        let t = trunc
            .map(LossError::Other)
            .or(x.as_err().map(LossError::Cast))
            .map(AnyLossError::Int);
        (y, t)
    }
}

impl<T, const LEN: usize> Castable for FloatRange<T, LEN>
where
    Self: HasNativeType<Native = T>,
    T: Copy,
{
    fn with_cast(&self, x: CastResult<T>) -> (T, Option<AnyLossError>) {
        let t = x.as_err().map(LossError::Cast).map(AnyLossError::Float);
        (x.new, t)
    }
}

impl Castable for AsciiRange {
    fn with_cast(&self, x: CastResult<Self::Native>) -> (Self::Native, Option<AnyLossError>) {
        let t = x.as_err().map(LossError::Cast).map(AnyLossError::Ascii);
        (x.new, t)
    }
}

impl<T, const LEN: usize> NativeWritable<Endian> for Bitmask<T, LEN>
where
    Self: HasNativeType<Native = T>,
    T: Ord + Copy + IntFromBytes<LEN>,
    u64: From<T>,
{
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<T>,
        byte_layout: Endian,
    ) -> io::Result<Option<AnyLossError>> {
        let (y, trunc) = self.with_cast(x);
        y.h_write_endian(h, byte_layout)?;
        Ok(trunc)
    }
}

impl<T, const LEN: usize> NativeWritable<SizedByteOrd<LEN>> for Bitmask<T, LEN>
where
    Self: HasNativeType<Native = T>,
    T: Ord + Copy + IntFromBytes<LEN>,
    u64: From<T>,
{
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<T>,
        byte_layout: SizedByteOrd<LEN>,
    ) -> io::Result<Option<AnyLossError>> {
        let (y, trunc) = self.with_cast(x);
        y.h_write_ordered(h, byte_layout)?;
        Ok(trunc)
    }
}

impl<T, const LEN: usize> NativeWritable<Endian> for FloatRange<T, LEN>
where
    Self: HasNativeType<Native = T>,
    T: Copy + FloatFromBytes<LEN>,
{
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<T>,
        byte_layout: Endian,
    ) -> io::Result<Option<AnyLossError>> {
        let (y, trunc) = self.with_cast(x);
        y.h_write_endian(h, byte_layout)?;
        Ok(trunc)
    }
}

impl<T, const LEN: usize> NativeWritable<SizedByteOrd<LEN>> for FloatRange<T, LEN>
where
    Self: HasNativeType<Native = T>,
    T: Copy + FloatFromBytes<LEN>,
{
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<T>,
        byte_layout: SizedByteOrd<LEN>,
    ) -> io::Result<Option<AnyLossError>> {
        let (y, trunc) = self.with_cast(x);
        y.h_write_ordered(h, byte_layout)?;
        Ok(trunc)
    }
}

impl<const ORD: bool> NativeWritable<NoByteOrd<ORD>> for AsciiRange {
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<Self::Native>,
        _: NoByteOrd<ORD>,
    ) -> io::Result<Option<AnyLossError>> {
        let (value, trunc) = self.with_cast(x);
        let str_value = value.to_string();
        let width: usize = u8::from(self.chars()).into();
        let err = if str_value.len() > width {
            // if string is greater than allocated chars, only write a fraction
            // starting from the left
            let offset = str_value.len() - width;
            h.write_all(&str_value.as_bytes()[offset..])?;
            Some(LossError::Other(AsciiLossError(self.chars())))
        } else {
            // if string less than allocated chars, pad left side with zero before
            // writing number
            for _ in 0..(width - str_value.len()) {
                h.write_all(&[30])?;
            }
            h.write_all(str_value.as_bytes())?;
            None
        };
        Ok(err.map(AnyLossError::Ascii).or(trunc))
    }
}

impl<'a, C, S> IntoWriter<'a, S> for C
where
    C: ToNativeWriter,
    ColumnWriter<'a, C, C::Native, S>: Writable<'a, S>,
    C::Native: Default + Copy + AllFCSCast,
    AnySource<'a, C::Native>: From<FCSColIter<'a, u8, C::Native>>
        + From<FCSColIter<'a, u16, C::Native>>
        + From<FCSColIter<'a, u32, C::Native>>
        + From<FCSColIter<'a, u64, C::Native>>
        + From<FCSColIter<'a, f32, C::Native>>
        + From<FCSColIter<'a, f64, C::Native>>,
    AnyLossError: From<LossError<C::Error>>,
{
    type Target = ColumnWriter<'a, C, C::Native, S>;

    fn into_writer(self, col: &'a AnyFCSColumn) -> Self::Target {
        self.into_native_writer(col)
    }

    fn check_writer(&self, col: &'a AnyFCSColumn) -> Result<(), AnyLossError> {
        self.check_native_writer(col).map_err(Into::into)
    }
}

impl<'a> IntoWriter<'a, Endian> for AnyNullBitmask {
    type Target = AnyWriterBitmask<'a>;

    fn into_writer(self, col: &'a AnyFCSColumn) -> Self::Target {
        match_any_uint!(self, Self, c, { c.into_native_writer(col).into() })
    }

    fn check_writer(&self, col: &'a AnyFCSColumn) -> Result<(), AnyLossError> {
        match_any_uint!(self, Self, c, {
            c.check_native_writer(col).map_err(Into::into)
        })
    }
}

impl<'a> IntoWriter<'a, Endian> for NullMixedType {
    type Target = WriterMixedType<'a>;

    fn into_writer(self, col: &'a AnyFCSColumn) -> Self::Target {
        match_any_mixed!(self, c, { c.into_writer(col).into() })
    }

    fn check_writer(&self, col: &'a AnyFCSColumn) -> Result<(), AnyLossError> {
        match self {
            Self::Ascii(c) => IntoWriter::<NoByteOrd3_1>::check_writer(c, col),
            Self::Uint(c) => c.check_writer(col),
            Self::F32(c) => c.check_native_writer(col).map_err(Into::into),
            Self::F64(c) => c.check_native_writer(col).map_err(Into::into),
        }
    }
}

impl<'a, C, T, S> Writable<'a, S> for ColumnWriter<'a, C, T, S>
where
    C: NativeWritable<S> + HasNativeType<Native = T> + ToNativeWriter + Castable,
    AnyFCSColumn: From<FCSColumn<T>>,
{
    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: S) -> io::Result<()> {
        let x = self.data.next().unwrap();
        let loss = self.column_type.h_write(h, x, byte_layout)?;
        self.loss = mem::take(&mut self.loss).or(loss);
        Ok(())
    }

    fn truncate(self, skip_conv_check: bool) -> (AnyFCSColumn, Option<AnyLossError>) {
        let mut warn = None;
        // TODO not optimal at all
        let mut xs = vec![];
        for x in self.data {
            let (y, w) = self.column_type.with_cast(x);
            if skip_conv_check {
                warn = mem::take(&mut warn).or(w);
            }
            xs.push(y);
        }
        (FCSColumn::from(xs).into(), warn)
    }

    fn into_err(self, i: MeasIndex) -> Option<IndexedLossError> {
        self.into_err(i)
    }
}

impl<'a> Writable<'a, Endian> for WriterMixedType<'a> {
    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: Endian) -> io::Result<()> {
        match self {
            Self::Ascii(c) => c.h_write(h, NoByteOrd),
            Self::Uint(c) => c.h_write(h, byte_layout),
            Self::F32(c) => c.h_write(h, byte_layout),
            Self::F64(c) => c.h_write(h, byte_layout),
        }
    }

    fn truncate(self, skip_conv_check: bool) -> (AnyFCSColumn, Option<AnyLossError>) {
        match_any_mixed!(self, x, { x.truncate(skip_conv_check) })
    }

    fn into_err(self, i: MeasIndex) -> Option<IndexedLossError> {
        match_any_mixed!(self, x, { x.into_err(i) })
    }
}

impl<'a> Writable<'a, Endian> for AnyWriterBitmask<'a> {
    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: Endian) -> io::Result<()> {
        match_any_uint!(self, Self, c, { c.h_write(h, byte_layout) })
    }

    fn truncate(self, skip_conv_check: bool) -> (AnyFCSColumn, Option<AnyLossError>) {
        match_any_uint!(self, Self, x, { x.truncate(skip_conv_check) })
    }

    fn into_err(self, i: MeasIndex) -> Option<IndexedLossError> {
        match_any_uint!(self, Self, x, { x.into_err(i) })
    }
}

impl<T, const LEN: usize> ToNativeWriter for Bitmask<T, LEN>
where
    Self: HasNativeType<Native = T>,
    u64: From<T>,
    T: Ord + Copy,
{
    type Error = BitmaskLossError;

    fn check_other_loss(&self, x: T) -> Option<Self::Error> {
        (x > self.bitmask()).then(|| BitmaskLossError(u64::from(self.bitmask())))
    }
}

impl<T, const LEN: usize> ToNativeWriter for FloatRange<T, LEN>
where
    Self: HasNativeType<Native = T>,
{
    type Error = Infallible;

    fn check_other_loss(&self, _: T) -> Option<Self::Error> {
        None
    }
}

impl ToNativeWriter for AsciiRange {
    type Error = AsciiLossError;

    fn check_other_loss(&self, x: Self::Native) -> Option<Self::Error>
    where
        u64: From<Self::Native>,
    {
        (Chars::from_u64(x) > self.chars()).then(|| AsciiLossError(self.chars()))
    }
}

/// A wrapper for any of the 6 source types that can be written.
///
/// Each inner type is an iterator from a different source type which emit
/// the given target type.
enum AnySource<'a, TargetType> {
    FromU08(FCSColIter<'a, u8, TargetType>),
    FromU16(FCSColIter<'a, u16, TargetType>),
    FromU32(FCSColIter<'a, u32, TargetType>),
    FromU64(FCSColIter<'a, u64, TargetType>),
    FromF32(FCSColIter<'a, f32, TargetType>),
    FromF64(FCSColIter<'a, f64, TargetType>),
}

impl<'a, T> AnySource<'a, T> {
    fn new(c: &'a AnyFCSColumn) -> Self
    where
        T: AllFCSCast,
        Self: From<FCSColIter<'a, u8, T>>
            + From<FCSColIter<'a, u16, T>>
            + From<FCSColIter<'a, u32, T>>
            + From<FCSColIter<'a, u64, T>>
            + From<FCSColIter<'a, f32, T>>
            + From<FCSColIter<'a, f64, T>>,
    {
        match_many_to_one!(c, AnyFCSColumn, [U08, U16, U32, U64, F32, F64], xs, {
            FCSDataType::as_col_iter(xs).into()
        })
    }
}

impl<T> Iterator for AnySource<'_, T> {
    type Item = CastResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match_many_to_one!(
            self,
            Self,
            [FromU08, FromU16, FromU32, FromU64, FromF32, FromF64],
            c,
            { c.next() }
        )
    }
}

fn is_ascii_delim(x: u8) -> bool {
    // tab, newline, carriage return, space, or comma
    x == 9 || x == 10 || x == 13 || x == 32 || x == 44
}

impl<D> EndianLayout<AnyNullBitmask, D> {
    pub(crate) fn endian_uint_try_new(
        cs: Vec<ColumnLayoutValues<D>>,
        e: Endian,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<Self, (), IndexedBitmaskError, NewUintTypeError>
    where
        D: IsNumType,
    {
        Self::try_new(cs, e, |i, c| {
            AnyBitmask::from_width_and_range(c.width, c.range, i, flag).repack_errors()
        })
    }

    pub(crate) fn uint_try_into_ordered<T>(
        self,
    ) -> ErrorsResult<AnyOrderedUintLayout<T>, (), UintEndianToOrderedLayoutError> {
        if let Some(cs) = NonEmpty::from_vec(self.columns) {
            cs.head
                .try_into_one_size(cs.tail, self.byte_layout, 1)
                .map_errors(|(index, error)| IndexedError::new(index, error).into())
        } else {
            let b: SizedByteOrd<4> = self.byte_layout.into();
            LogResult::new_ok(FixedLayout::new(vec![], b).into())
        }
    }
}

impl<D> EndianLayout<NullMixedType, D> {
    pub(crate) fn try_into_ordered<T>(
        self,
    ) -> ErrorsResult<AnyOrderedLayout<T>, (), MixedToOrderedLayoutError> {
        macro_rules! from_columns {
            ($i:expr) => {
                $i.into_iter()
                    .enumerate()
                    .map(|(i, c)| {
                        c.try_into()
                            .map_err(|e| IndexedError::new(i + 1, e))
                            .map_err(MixedToNonMixedLayoutError)
                            .map_err(MixedToOrderedLayoutError::from)
                            .into_log()
                    })
                    .mappend_commutative()
            };
        }

        if let Some(ne_cols) = NonEmpty::from_vec(self.columns) {
            let c0 = ne_cols.head;
            let cs = ne_cols.tail;
            let endian = self.byte_layout;
            match c0 {
                MixedType::Uint(x) => x
                    .try_into_one_size(cs, endian, 1)
                    .map_ok_value(AnyOrderedLayout::from)
                    .map_errors(|(index, error)| error.into_col_error(index)),
                MixedType::Ascii(x) => from_columns!(cs)
                    .map_ok_value(|xs| FixedLayout::new1(x, xs, NoByteOrd))
                    .map_ok_value(AnyAsciiLayout::from)
                    .map_ok_value(AnyOrderedLayout::from),
                MixedType::F32(x) => from_columns!(cs)
                    .map_ok_value(|xs| FixedLayout::new1(x, xs, endian.into()))
                    .map_ok_value(AnyOrderedLayout::from),
                MixedType::F64(x) => from_columns!(cs)
                    .map_ok_value(|xs| FixedLayout::new1(x, xs, endian.into()))
                    .map_ok_value(AnyOrderedLayout::from),
            }
        } else {
            let b: SizedByteOrd<4> = self.byte_layout.into();
            LogResult::new_ok(FixedLayout::new(vec![], b).into())
        }
    }

    pub(crate) fn try_into_non_mixed(
        self,
    ) -> ErrorsResult<NonMixedEndianLayout<Nothing<NumType>>, (), MixedToNonMixedLayoutError> {
        if let Some(ne_cols) = NonEmpty::from_vec(self.columns) {
            macro_rules! from_iter {
                ($iter:expr, $head:expr, $byte_layout:expr) => {
                    $iter
                        .map(|(i, c)| c.try_into().map_err(|e| (i, e)).into_log())
                        .mappend_commutative()
                        .map_ok_value(|xs| FixedLayout::new1($head, xs, $byte_layout))
                        .map_ok_value(NonMixedEndianLayout::from)
                };
            }

            let c0 = ne_cols.head;
            let it = ne_cols.tail.into_iter().enumerate();
            let byte_layout = self.byte_layout;
            match c0 {
                MixedType::Ascii(x) => it
                    .map(|(i, c)| c.try_into().map_err(|e| (i, e)).into_log::<_, _, Vec<_>>())
                    .mappend_commutative()
                    .map_ok_value(|xs| FixedLayout::new1(x, xs, NoByteOrd))
                    .map_ok_value(|l| AnyAsciiLayout::Fixed(l).into()),
                MixedType::Uint(x) => from_iter!(it, x, byte_layout),
                MixedType::F32(x) => from_iter!(it, x, byte_layout),
                MixedType::F64(x) => from_iter!(it, x, byte_layout),
            }
            .map_errors(|(i, error)| IndexedError::new(i + 1, error).into())
        } else {
            let l = FixedLayout::new(vec![], self.byte_layout);
            LogResult::new_ok(NonMixedEndianLayout::Integer(l))
        }
    }
}

// NOTE num_traits has this but it doesn't have a nice way to init a default
// buffer, this will probably be easier and cleaner anyways once we can use
// const expressions
macro_rules! impl_num_props {
    ($size:expr, $t:ty) => {
        impl NumProps for $t {
            const LEN: usize = $size;
            type BUF = [u8; $size];

            fn read_buf<R: Read>(h: &mut BufReader<R>) -> io::Result<Self::BUF> {
                let mut buf = Self::BUF::default();
                h.read_exact(&mut buf)?;
                Ok(buf)
            }

            fn to_big(self) -> Self::BUF {
                <$t>::to_be_bytes(self)
            }

            fn to_little(self) -> Self::BUF {
                <$t>::to_le_bytes(self)
            }

            fn from_big(buf: Self::BUF) -> Self {
                <$t>::from_be_bytes(buf)
            }

            fn from_little(buf: Self::BUF) -> Self {
                <$t>::from_le_bytes(buf)
            }
        }
    };
}

impl_num_props!(1, u8);
impl_num_props!(2, u16);
impl_num_props!(4, u32);
impl_num_props!(8, u64);
impl_num_props!(4, f32);
impl_num_props!(8, f64);

impl OrderedFromBytes<1> for u8 {}
impl OrderedFromBytes<2> for u16 {}
impl OrderedFromBytes<3> for u32 {}
impl OrderedFromBytes<4> for u32 {}
impl OrderedFromBytes<5> for u64 {}
impl OrderedFromBytes<6> for u64 {}
impl OrderedFromBytes<7> for u64 {}
impl OrderedFromBytes<8> for u64 {}
impl OrderedFromBytes<4> for f32 {}
impl OrderedFromBytes<8> for f64 {}

impl FloatFromBytes<4> for f32 {}
impl FloatFromBytes<8> for f64 {}

impl IntFromBytes<1> for u8 {}
impl IntFromBytes<2> for u16 {}
impl IntFromBytes<3> for u32 {}
impl IntFromBytes<4> for u32 {}
impl IntFromBytes<5> for u64 {}
impl IntFromBytes<6> for u64 {}
impl IntFromBytes<7> for u64 {}
impl IntFromBytes<8> for u64 {}

impl<T, const LEN: usize> FloatRange<T, LEN> {
    /// Make new float range from $PnB and $PnR values.
    ///
    /// Will return an error if $PnB is the incorrect size.
    pub(crate) fn from_width_and_range(
        width: Width,
        range: Range,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorResult<Self, (), IndexedFloatRangeError, FloatWidthError>
    where
        FloatDecimal<T>: TryFrom<BigDecimal, Error = DecimalToFloatError>,
        T: HasFloatBounds,
    {
        PrivBytes::try_from(width)
            .map_err(|e| IndexedError::new(i, e))
            .map_err(IndexedWidthToBytesError)
            .into_log::<Vec<_>, Vec<_>, Nothing<_>>()
            .map_errors(FloatWidthError::from)
            .and_then_commutative(|bytes| {
                if usize::from(u8::from(bytes)) == LEN {
                    Self::from_range(range, flag)
                        .set_err_value(())
                        .map_switchable_errors(|e| IndexedError::new(i, e))
                        .map_switchable_errors(IndexedFloatRangeError)
                        .switchable_into_commutative()
                        .map_errors(FloatWidthError::from)
                        .repack_warnings()
                } else {
                    let e = FloatWidthError::from(WrongFloatWidth::new(bytes, LEN, i));
                    LogResult::new_err(e)
                }
            })
    }
}

impl NullMixedType {
    /// Make a new mixed range from $PnB and $PnR, and $PnDATATYPE values
    pub(crate) fn from_width_and_range(
        width: Width,
        range: Range,
        datatype: Option<NumType>,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<Self, (), NewMixedTypeWarning, NewMixedTypeError> {
        macro_rules! from {
            ($t:ident, $width:expr, $range:expr, $i:expr, $flag:expr) => {
                $t::from_width_and_range($width, $range, $i, $flag)
                    .map_ok_value(Self::from)
                    .map_commutative_warnings(NewMixedTypeWarning::from)
                    .map_errors(NewMixedTypeError::from)
                    .repack_errors()
            };
        }

        if let Some(dt) = datatype {
            match dt {
                NumType::Integer => from!(AnyBitmask, width, range, i, flag),
                NumType::Float => from!(F32Range, width, range, i, flag),
                NumType::Double => from!(F64Range, width, range, i, flag),
            }
        } else {
            from!(AsciiRange, width, range, i, flag)
        }
    }

    fn as_alpha_num_type(&self) -> AlphaNumType {
        match self {
            Self::Ascii(_) => AlphaNumType::Ascii,
            Self::Uint(_) => AlphaNumType::Integer,
            Self::F32(_) => AlphaNumType::Float,
            Self::F64(_) => AlphaNumType::Double,
        }
    }
}

impl From<u64> for AnyNullBitmask {
    /// Make a new bitmask from a u64.
    ///
    /// The width is determined by the magnitude of the range; the smallest
    /// possible will be used.
    fn from(value: u64) -> Self {
        // ASSUME these will never truncate because we check the width first
        match PrivBytes::from_u64(value) {
            PrivBytes::B1 => Self::Uint08(Bitmask::from_u64(value).0),
            PrivBytes::B2 => Self::Uint16(Bitmask::from_u64(value).0),
            PrivBytes::B3 => Self::Uint24(Bitmask::from_u64(value).0),
            PrivBytes::B4 => Self::Uint32(Bitmask::from_u64(value).0),
            PrivBytes::B5 => Self::Uint40(Bitmask::from_u64(value).0),
            PrivBytes::B6 => Self::Uint48(Bitmask::from_u64(value).0),
            PrivBytes::B7 => Self::Uint56(Bitmask::from_u64(value).0),
            PrivBytes::B8 => Self::Uint64(Bitmask::from_u64(value).0),
        }
    }
}

impl From<AnyNullBitmask> for u64 {
    /// Convert bitmask range (not bitmask itself) to u64.
    fn from(value: AnyNullBitmask) -> Self {
        match_any_uint!(value, AnyNullBitmask, x, { Self::from(x) })
    }
}

impl AnyNullBitmask {
    /// Make a new bitmask from $PnB and PnR values.
    ///
    /// Will return an error if $PnB (in bits) cannot be converted into a width
    /// in bytes.
    fn from_width_and_range(
        width: Width,
        range: Range,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorResult<Self, (), IndexedBitmaskError, NewUintTypeError> {
        width
            .try_into()
            .map_err(|e| IndexedError::new(i, e))
            .map_err(IndexedWidthToBytesError)
            .map_err(NewUintTypeError::from)
            .into_log()
            .and_then_commutative(|bytes| {
                Self::try_new(bytes, range, i, flag)
                    .set_err_value(())
                    .switchable_into_commutative()
                    .map_errors(NewUintTypeError::from)
                    .repack_warnings()
            })
    }

    /// Make a new bitmask with a given width (in bytes) using a float/int.
    fn try_new(
        width: PrivBytes,
        range: Range,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<Self, DisallowRangeTrunc, IndexedBitmaskError> {
        let ret = match width {
            PrivBytes::B1 => Bitmask08::from_range(range, flag).map_deferred_value(Into::into),
            PrivBytes::B2 => Bitmask16::from_range(range, flag).map_deferred_value(Into::into),
            PrivBytes::B3 => Bitmask24::from_range(range, flag).map_deferred_value(Into::into),
            PrivBytes::B4 => Bitmask32::from_range(range, flag).map_deferred_value(Into::into),
            PrivBytes::B5 => Bitmask40::from_range(range, flag).map_deferred_value(Into::into),
            PrivBytes::B6 => Bitmask48::from_range(range, flag).map_deferred_value(Into::into),
            PrivBytes::B7 => Bitmask56::from_range(range, flag).map_deferred_value(Into::into),
            PrivBytes::B8 => Bitmask64::from_range(range, flag).map_deferred_value(Into::into),
        };
        ret.map_switchable_errors(|e| e.into_indexed_err(i))
    }

    pub(crate) fn try_into_one_size<X, E, T>(
        self,
        tail: Vec<X>,
        endian: Endian,
        starting_index: usize,
    ) -> ErrorsResult<AnyOrderedUintLayout<T>, (), (MeasIndex, E)>
    where
        Bitmask08: TryFrom<X, Error = E>,
        Bitmask16: TryFrom<X, Error = E>,
        Bitmask24: TryFrom<X, Error = E>,
        Bitmask32: TryFrom<X, Error = E>,
        Bitmask40: TryFrom<X, Error = E>,
        Bitmask48: TryFrom<X, Error = E>,
        Bitmask56: TryFrom<X, Error = E>,
        Bitmask64: TryFrom<X, Error = E>,
    {
        match_any_uint!(self, Self, x, {
            Bitmask::try_from_many(tail, starting_index)
                .map_ok_value(|xs| FixedLayout::new1(x, xs, endian.into()).into())
        })
    }
}

fn ascii_to_uint(buf: &[u8]) -> Result<u64, AsciiToUintError> {
    if buf.is_ascii() {
        // SAFETY: we just checked that all bytes are ASCII
        let s = unsafe { str::from_utf8_unchecked(buf) };
        s.parse().map_err(AsciiToUintError::from)
    } else {
        Err(NotAsciiError(buf.to_vec()).into())
    }
}

impl From<ColumnLayoutValues3_2> for ColumnLayoutValues2_0 {
    fn from(value: ColumnLayoutValues3_2) -> Self {
        Self::new(value.width, value.range, Nothing::default())
    }
}

impl<T, D, const ORD: bool> LayoutOps<'_, T> for DelimAsciiLayout<T, D, ORD>
where
    T: IsTot,
    NoByteOrd<ORD>: HasByteOrd,
    <NoByteOrd<ORD> as HasByteOrd>::ByteOrd: fmt::Display,
{
    fn ncols(&self) -> usize {
        self.ranges.len()
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64 {
        df.ascii_nbytes()
    }

    fn ranges(&self) -> Vec<Range> {
        self.ranges.iter().map(|x| Range::from(*x)).collect()
    }

    fn datatype(&self) -> AlphaNumType {
        AlphaNumType::Ascii
    }

    fn datatypes(&self) -> Vec<AlphaNumType> {
        self.ranges.iter().map(|_| self.datatype()).collect()
    }

    fn byteord_keyword(&self) -> (String, String) {
        // NOTE BYTEORD is meaningless for delimited ASCII so use a dummy
        <NoByteOrd<ORD> as HasByteOrd>::ByteOrd::from(NoByteOrd).pair()
    }

    fn req_meas_keywords(&self) -> Vec<[(String, String); 2]> {
        self.ranges
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let x = Width::Variable.meas_pair(i);
                let y = Range((*r).into()).meas_pair(i);
                [x, y]
            })
            .collect()
    }

    fn remove_nocheck(&mut self, index: MeasIndex) {
        self.ranges.remove(index.into());
    }

    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        _: &mut Vec<u8>,
        tot: T,
        seg: AnyDataSegment,
        _: &ReaderConfig,
    ) -> WarningsAndIOGroupResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError, ()> {
        macro_rules! go {
            ($x:expr) => {
                $x.map_err(|e| {
                    e.fmap_once(ReadDelimAsciiError::from)
                        .fmap_once(ReadAsciiError::from)
                        .fmap_once(ReadDataframeError::from)
                })
            };
        }
        let rs = &self.ranges;
        let nbytes = usize::try_from(seg.len()).expect("DATA length > usize");
        let res = T::with_tot(
            h,
            tot,
            |h_, t| go!(h_read_delim_with_rows(rs, h_, t, nbytes)),
            |h_| go!(h_read_delim_without_rows(rs, h_, nbytes)),
        );
        res.map_err(IOErrorGroup::from).into_log()
    }

    fn check_writer(&self, df: &FCSDataFrame) -> ErrorsResult<(), (), IndexedLossError> {
        df.iter_columns()
            .enumerate()
            .map(|(i, c)| {
                c.check_writer::<_, _, u64>(|_| None)
                    .map_err(|error| IndexedError::new(i, AnyLossError::Int(error)))
                    .map_err(IndexedLossError)
                    .into_log()
            })
            .mappend_def_void()
    }

    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        skip_conv_check: bool,
    ) -> DeferredWarningsAndError<(), IndexedLossError, io::Error> {
        let ncols = df.ncols();
        let nrows = df.nrows();
        // ASSUME dataframe has correct number of columns
        let mut column_srcs: Vec<_> = df.iter_columns().map(AnySource::<'_, u64>::new).collect();
        let mut loss_ws = vec![None; column_srcs.len()];

        let mut go = || -> Result<(), io::Error> {
            for row in 0..nrows {
                for (col, xs) in column_srcs.iter_mut().enumerate() {
                    let x = xs.next().unwrap();
                    let s = x.new.to_string();
                    loss_ws[col] = mem::take(&mut loss_ws[col]).or(x.as_err());
                    let buf = s.as_bytes();
                    h.write_all(buf)?;
                    // write delimiter after all but last value
                    if !(row == nrows - 1 && col == ncols - 1) {
                        h.write_all(&[32])?; // 32 = space in ASCII
                    }
                }
            }
            Ok(())
        };

        let write_res = go().into_nowarn1();

        if skip_conv_check {
            write_res.nowarn_into_warn()
        } else {
            let cs: Vec<_> = loss_ws
                .into_iter()
                .enumerate()
                .filter_map(|(i, warn)| {
                    warn.map(LossError::Cast)
                        .map(AnyLossError::Ascii)
                        .map(|w| IndexedError::new(i, w))
                        .map(IndexedLossError)
                })
                .collect();
            write_res.set_commutative_warnings(cs)
        }
    }

    fn truncate_df(
        &self,
        df: &FCSDataFrame,
        skip_conv_check: bool,
    ) -> WarningsResult<FCSDataFrame, IndexedLossError> {
        let nrows = df.nrows();
        let (columns, warnings): (Vec<_>, Vec<_>) = df
            .iter_columns()
            .enumerate()
            .map(|(i, c)| {
                let mut w = None;
                let mut cs = vec![0; nrows];
                for x in AnySource::<'_, u64>::new(c) {
                    cs.push(x.new);
                    if !skip_conv_check {
                        w = mem::take(&mut w).or(x.as_err());
                    }
                }
                (
                    FCSColumn::from(cs).into(),
                    w.map(|x| IndexedError::new(i, AnyLossError::Ascii(LossError::Cast(x)))),
                )
            })
            .unzip();
        let ws: Vec<_> = warnings
            .into_iter()
            .flatten()
            .map(IndexedLossError)
            .collect();
        let ret = FCSDataFrame::try_new(columns).unwrap();
        Success::new_non_switchable(ret).set_warnings(ws)
    }
}

impl<T, D, const ORD: bool> InterLayoutOps<D> for DelimAsciiLayout<T, D, ORD> {
    fn opt_meas_headers(&self) -> Vec<MeasHeader> {
        vec![]
    }

    fn opt_meas_keywords(&self) -> Vec<Vec<(String, Option<String>)>> {
        self.ranges.iter().map(|_| vec![]).collect()
    }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> SwitchableErrorResult<(), (), DisallowRangeTrunc, InsertRangeError> {
        range
            .into_uint()
            .map_errors(InsertRangeError::from)
            .nowarn_into_switchable(flag)
            .map_ok_value(|r| self.ranges.insert(index.into(), r))
            .set_err_value(())
    }

    fn push(
        &mut self,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> SwitchableErrorResult<(), (), DisallowRangeTrunc, InsertRangeError> {
        range
            .into_uint()
            .map_errors(InsertRangeError::from)
            .nowarn_into_switchable(flag)
            .map_ok_value(|r| self.ranges.push(r))
            .set_err_value(())
    }

    fn clear(&mut self) {
        self.ranges.clear();
    }
}

fn h_read_delim_with_rows<R: Read>(
    ranges: &[u64],
    h: &mut BufReader<R>,
    tot: Tot,
    nbytes: usize,
) -> Result<FCSDataFrame, ImpureError<ReadDelimWithRowsAsciiError>> {
    let mut buf = Vec::new();
    let mut last_was_delim = false;
    let nrows = tot.0;
    let ncols = ranges.len();
    // TODO emit a real error here since this means something is probably
    // screwy with the file
    if (nrows == 0 || ncols == 0) && nbytes > 0 {
        return Ok(FCSDataFrame::default());
    }
    // Here we have $TOT so initialize vectors to required length
    let mut data = vec![vec![0; nrows]; ncols];
    // let mut data = self.0.columns;
    // let nrows = data.head.len();
    // let ncols = data.len();
    let mut row = 0;
    let mut col = 0;
    // Delimiters are tab, newline, carriage return, space, or comma. Any
    // consecutive delimiter counts as one, and delimiters can be mixed.
    for b in h.bytes().take(nbytes) {
        let byte = b?;
        // exit if we encounter more rows than expected.
        if row == nrows {
            let e = ReadDelimWithRowsAsciiError::RowsExceeded(RowsExceededError(nrows));
            return Err(ImpureError::Pure(e));
        }
        if is_ascii_delim(byte) {
            if !last_was_delim {
                last_was_delim = true;
                data[col][row] = ascii_to_uint(&buf)
                    .map_err(ReadDelimWithRowsAsciiError::Parse)
                    .map_err(ImpureError::Pure)?;
                buf.clear();
                if col == ncols - 1 {
                    col = 0;
                    row += 1;
                } else {
                    col += 1;
                }
            }
        } else {
            buf.push(byte);
            last_was_delim = false;
        }
    }
    if !(col == 0 && row == nrows) {
        let e = DelimIncompleteError { col, row, nrows };
        let ee = ImpureError::Pure(ReadDelimWithRowsAsciiError::Incomplete(e));
        return Err(ee);
    }
    // The spec isn't clear if the last value should be a delim or
    // not, so flush the buffer if it has anything in it since we
    // only try to parse if we hit a delim above.
    if !buf.is_empty() {
        data[col][row] = ascii_to_uint(&buf)
            .map_err(ReadDelimWithRowsAsciiError::Parse)
            .map_err(ImpureError::Pure)?;
    }
    let cs: Vec<_> = data
        .into_iter()
        .map(FCSColumn::from)
        .map(AnyFCSColumn::from)
        .collect();
    // ASSUME this will never fail because all columns should be the same
    // length
    Ok(FCSDataFrame::try_new(cs).unwrap())
}

fn h_read_delim_without_rows<R: Read>(
    ranges: &[u64],
    h: &mut BufReader<R>,
    nbytes: usize,
) -> Result<FCSDataFrame, ImpureError<ReadDelimAsciiWithoutRowsError>> {
    let mut buf = Vec::new();
    // Here we don't have $TOT so init to empty vectors
    let mut data: Vec<_> = ranges.iter().map(|_| vec![]).collect();
    let ncols = data.len();
    // TODO emit a real error here since this means something is probably
    // screwy with the file
    if ncols == 0 && nbytes > 0 {
        return Ok(FCSDataFrame::default());
    }
    let mut col = 0;
    let mut last_was_delim = false;
    let go = |data_: &mut Vec<Vec<u64>>, col_: usize, buf_: &[u8]| {
        ascii_to_uint(buf_)
            .map_err(ReadDelimAsciiWithoutRowsError::Parse)
            .map_err(ImpureError::Pure)
            .map(|x| data_[col_].push(x))
    };
    // Delimiters are tab, newline, carriage return, space, or comma. Any
    // consecutive delimiter counts as one, and delimiters can be mixed.
    // If we don't know the number of rows, the only choice is to push onto
    // the column vectors one at a time. This leads to the possibility that
    // the vectors may not be the same length in the end, in which case,
    // scream loudly and bail.
    for b in h.bytes().take(nbytes) {
        let byte = b?;
        if is_ascii_delim(byte) {
            if !last_was_delim {
                last_was_delim = true;
                buf.clear();
                go(&mut data, col, &buf)?;
                if col == ncols - 1 {
                    col = 0;
                } else {
                    col += 1;
                }
            }
        } else {
            buf.push(byte);
            last_was_delim = false;
        }
    }
    if data.iter().map(Vec::len).unique().count() > 1 {
        return Err(ImpureError::Pure(ReadDelimAsciiWithoutRowsError::Unequal));
    }
    // The spec isn't clear if the last value should be a delim or
    // not, so flush the buffer if it has anything in it since we
    // only try to parse if we hit a delim above.
    if !buf.is_empty() {
        go(&mut data, col, &buf)?;
    }
    let cs: Vec<_> = data
        .into_iter()
        .map(FCSColumn::from)
        .map(AnyFCSColumn::from)
        .collect();
    // ASSUME this will never fail because all columns should be the same
    // length
    Ok(FCSDataFrame::try_new(cs).unwrap())
}

impl<C, S: Default, T, D> Default for FixedLayout<C, S, T, D> {
    fn default() -> Self {
        Self::new(vec![], S::default())
    }
}

impl<'a, C, S, T, D> LayoutOps<'a, T> for FixedLayout<C, S, T, D>
where
    D: IsNumType,
    T: IsTot,
    C: Clone + IsFixed + HasDatatype + IntoReader<S> + IntoWriter<'a, S> + FromRange,
    S: Copy + HasByteOrd,
    S::ByteOrd: fmt::Display,
    for<'c> Range: From<&'c C>,
    <C as IntoReader<S>>::Target: Readable<S>,
    <C as IntoWriter<'a, S>>::Target: Writable<'a, S>,
    InsertRangeError: From<<C as FromRange>::Error>,
{
    fn ranges(&self) -> Vec<Range> {
        self.columns.iter().map(Into::into).collect()
    }

    fn ncols(&self) -> usize {
        self.columns.len()
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64 {
        let nrows = u64::try_from(df.nrows()).expect("rows in dataframe exceed 2^64");
        self.event_width() * nrows
    }

    fn datatype(&self) -> AlphaNumType {
        C::datatype_from_columns(&self.columns)
    }

    fn datatypes(&self) -> Vec<AlphaNumType> {
        self.columns.iter().map(HasDatatype::datatype).collect()
    }

    fn byteord_keyword(&self) -> (String, String) {
        S::ByteOrd::from(self.byte_layout).pair()
    }

    fn req_meas_keywords(&self) -> Vec<[(String, String); 2]> {
        self.columns
            .iter()
            .enumerate()
            .map(|(i, c)| c.req_meas_keywords(i.into()))
            .collect()
    }

    fn remove_nocheck(&mut self, index: MeasIndex) {
        self.columns.remove(index.into());
    }

    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut Vec<u8>,
        tot: T,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> WarningsAndIOGroupResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError, ()>
    where
        T: IsTot,
    {
        self.compute_nrows(seg, conf)
            .map_non_commutative_warnings(ReadDataframeWarning::from)
            .non_commutative_into_commutative()
            .map_errors(ReadDataframeError::from)
            .into_semigroup()
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|n| {
                T::check_tot(n, tot, conf.allow_tot_mismatch)
                    .switchable_into_commutative()
                    .map_commutative_warnings(ReadDataframeWarning::from)
                    .map_errors(ReadDataframeError::from)
                    .into_semigroup()
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .set_ok_value(n)
            })
            .and_then_commutative(|n| {
                let nn = usize::try_from(n).expect("nrows exceeds usize");
                self.h_read_unchecked_df(h, nn, buf)
                    .map_err(IOErrorGroup::from)
                    .into_log()
            })
    }

    fn check_writer(&self, df: &'a FCSDataFrame) -> ErrorsResult<(), (), IndexedLossError> {
        // ASSUME df has same number of columns as layout
        self.columns
            .iter()
            .zip(df.iter_columns())
            .enumerate()
            .map(|(i, (col_type, col_data))| {
                col_type
                    .check_writer(col_data)
                    .map_err(|error| IndexedError::new(i, error))
                    .map_err(IndexedLossError)
                    .into_log()
            })
            .mappend_def_void()
    }

    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &'a FCSDataFrame,
        skip_conv_check: bool,
    ) -> DeferredWarningsAndError<(), IndexedLossError, io::Error> {
        let nrows = df.nrows();
        // ASSUME df has same number of columns as layout
        let mut cs: Vec<_> = self
            .columns
            .iter()
            .zip(df.iter_columns())
            .map(|(col_type, col_data)| col_type.clone().into_writer(col_data))
            .collect();

        let mut go = || {
            for _ in 0..nrows {
                for c in &mut cs {
                    c.h_write(h, self.byte_layout)?;
                }
            }
            Ok(())
        };

        let write_res = go().into_nowarn1();

        // TODO perhaps a microoptization, if we don't need conversion warnings
        // might as well not check for them when writing each value in the first
        // place. This may be optimized away by the compiler in case this flag
        // is false, and if not it maybe doesn't make a different anyways since
        // its mostly just a conditional check which will be fast with branch
        // prediction. On the other hand, this is a very tight loop.
        if skip_conv_check {
            write_res.nowarn_into_warn()
        } else {
            let ws = cs
                .into_iter()
                .enumerate()
                .filter_map(|(i, c)| c.into_err(i.into()))
                .collect();
            write_res.set_commutative_warnings(ws)
        }
    }

    // TODO confusing that this returns a result when no trait impls for this
    // method can fail
    fn truncate_df(
        &self,
        df: &'a FCSDataFrame,
        skip_conv_check: bool,
    ) -> WarningsResult<FCSDataFrame, IndexedLossError> {
        // ASSUME df has same number of columns as layout
        let (new_columns, warnings): (Vec<_>, Vec<_>) = self
            .columns
            .iter()
            .zip(df.iter_columns())
            .map(|(col_type, col_data)| {
                col_type
                    .clone()
                    .into_writer(col_data)
                    .truncate(skip_conv_check)
            })
            .unzip();
        let ws: Vec<_> = warnings
            .into_iter()
            .enumerate()
            .filter_map(|(i, e)| e.map(|f| IndexedError::new(i, f)))
            .map(IndexedLossError)
            .collect();
        let ret = FCSDataFrame::try_new(new_columns).unwrap();
        Success::new_non_switchable(ret).set_warnings(ws)
    }
}

impl<'a, C, S, T, D> InterLayoutOps<D> for FixedLayout<C, S, T, D>
where
    T: IsTot,
    C: Clone + IsFixed + HasDatatype + IntoReader<S> + IntoWriter<'a, S> + FromRange,
    S: Copy + HasByteOrd,
    for<'c> Range: From<&'c C>,
    <C as IntoReader<S>>::Target: Readable<S>,
    <C as IntoWriter<'a, S>>::Target: Writable<'a, S>,
    InsertRangeError: From<<C as FromRange>::Error>,
{
    fn opt_meas_headers(&self) -> Vec<MeasHeader> {
        vec![]
    }

    fn opt_meas_keywords(&self) -> Vec<Vec<(String, Option<String>)>> {
        self.columns.iter().map(|_| vec![]).collect()
    }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> SwitchableErrorResult<(), (), DisallowRangeTrunc, InsertRangeError> {
        C::from_range(range, flag)
            .map_switchable_errors(InsertRangeError::from)
            .map_ok_value(|col| self.insert_column(index, col))
            .set_err_value(())
    }

    fn push(
        &mut self,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> SwitchableErrorResult<(), (), DisallowRangeTrunc, InsertRangeError> {
        C::from_range(range, flag)
            .map_switchable_errors(InsertRangeError::from)
            .map_ok_value(|col| self.push_column(col))
            .set_err_value(())
    }

    fn clear(&mut self) {
        self.columns.clear();
    }
}

impl<C, S, T, D> OrderedLayoutOps for FixedLayout<C, S, T, D>
where
    S: Copy,
    ByteOrd2_0: From<S>,
{
    fn byte_order(&self) -> ByteOrd2_0 {
        self.byte_layout.into()
    }
}

impl<C, S, T, D> FixedLayout<C, S, T, D> {
    pub fn new_empty(byte_layout: S) -> Self {
        Self::new(vec![], byte_layout)
    }

    pub fn columns(&self) -> &[C] {
        &self.columns[..]
    }

    pub fn widths(&self) -> Vec<BitsOrChars>
    where
        C: IsFixed,
    {
        self.columns.iter().map(IsFixed::fixed_width).collect()
    }

    fn new1(head: C, tail: Vec<C>, byte_layout: S) -> Self {
        Self::new(NonEmpty::from((head, tail)).into(), byte_layout)
    }

    fn try_new<F, P, W, E>(
        cs: Vec<ColumnLayoutValues<D>>,
        byte_layout: S,
        new_col_f: F,
    ) -> WarningsAndErrorsResult<Self, (), W, E>
    where
        D: IsNumType,
        F: Fn(MeasIndex, ColumnLayoutValues<D>) -> WarningsAndErrorsResult<C, P, W, E>,
    {
        cs.into_iter()
            .enumerate()
            .map(|(i, c)| new_col_f(i.into(), c).repack_errors())
            .mappend_commutative()
            .map_ok_value(|columns| Self::new(columns, byte_layout))
    }

    fn h_read_unchecked_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        buf: &mut Vec<u8>,
    ) -> IOResult<FCSDataFrame, ReadDataframeError>
    where
        S: Copy,
        C: IsFixed + Clone + IntoReader<S>,
        <C as IntoReader<S>>::Target: Readable<S>,
    {
        let mut col_readers: Vec<_> = self
            .columns
            .iter()
            .map(|c| c.clone().into_reader(nrows))
            .collect();
        for row in 0..nrows {
            for c in &mut col_readers {
                c.h_read(h, row, self.byte_layout, buf)?;
            }
        }
        let data = col_readers
            .into_iter()
            .map(Readable::into_dataframe_column)
            .collect();
        Ok(FCSDataFrame::try_new(data).unwrap())
    }

    fn insert_column(&mut self, index: MeasIndex, col: C) {
        self.columns.insert(index.into(), col);
    }

    fn push_column(&mut self, col: C) {
        self.columns.push(col);
    }

    fn columns_into<X>(self) -> FixedLayout<X, S, T, D>
    where
        X: From<C>,
    {
        FixedLayout::new(
            self.columns.into_iter().map(Into::into).collect(),
            self.byte_layout,
        )
    }

    fn byte_layout_into<X>(self) -> FixedLayout<C, X, T, D>
    where
        X: From<S>,
    {
        FixedLayout::new(self.columns, self.byte_layout.into())
    }

    fn byte_layout_try_into<X>(self) -> Result<FixedLayout<C, X, T, D>, X::Error>
    where
        X: TryFrom<S>,
    {
        self.byte_layout
            .try_into()
            .map(|byte_layout| FixedLayout::new(self.columns, byte_layout))
    }

    pub fn phantom_into<T1, D1>(self) -> FixedLayout<C, S, T1, D1> {
        FixedLayout::new(self.columns, self.byte_layout)
    }

    fn event_width(&self) -> u64
    where
        C: IsFixed,
    {
        self.columns
            .iter()
            .map(|c| u64::from(u8::from(c.nbytes())))
            .sum()
    }

    pub fn compute_nrows(
        &self,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> WarningOrErrorResult<u64, (), UnevenEventWidth, EventWidthError>
    where
        S: Clone,
        C: IsFixed,
    {
        let n = seg.len();
        // TODO is this always not zero?
        let w = self.event_width();
        if w == 0 {
            LogResult::new_err(EventWidthError::from(ZeroEventWidth::new(n)))
        } else {
            let total_events = n / w;
            let remainder = n % w;
            let is_ok = remainder == 0;
            let e = UnevenEventWidth::new(w, n, remainder);
            let flag = conf.allow_uneven_event_width;
            SwitchableErrorResult::new_switchable_ok_if(is_ok, total_events, (), e, flag)
                .switchable_into_non_commutative()
                .map_errors(EventWidthError::from)
        }
    }
}

impl<C> EndianLayout<C, Option<NumType>> {
    fn insert_mixed(
        mut self,
        index: MeasIndex,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<DataLayout3_2, DisallowRangeTrunc, InsertRangeError>
    where
        C: TryFrom<NullMixedType, Error = MixedToNonMixedError>,
        NullMixedType: From<C>,
        NonMixedEndianLayout<Option<NumType>>: From<Self>,
    {
        NullMixedType::from_range(range, flag).map_deferred_value(|col| match col.try_into() {
            Ok(c) => {
                self.insert_column(index, c);
                DataLayout3_2::NonMixed(self.into())
            }
            Err(e) => {
                let mut z = self.columns_into();
                z.insert_column(index, e.src);
                z.into()
            }
        })
    }

    fn push_mixed(
        mut self,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<DataLayout3_2, DisallowRangeTrunc, InsertRangeError>
    where
        C: TryFrom<NullMixedType, Error = MixedToNonMixedError>,
        NullMixedType: From<C>,
        NonMixedEndianLayout<Option<NumType>>: From<Self>,
    {
        NullMixedType::from_range(range, flag).map_deferred_value(|col| match col.try_into() {
            Ok(c) => {
                self.push_column(c);
                DataLayout3_2::NonMixed(self.into())
            }
            Err(e) => {
                let mut z = self.columns_into();
                z.push_column(e.src);
                z.into()
            }
        })
    }
}

macro_rules! def_native_wrapper {
    ($name:path, $native:ty, $size:expr, $native_size:expr, $bytes:ident) => {
        impl HasNativeType for $name {
            type Native = $native;
        }

        impl HasNativeWidth for $name {
            const BYTES: Bytes = Bytes(PrivBytes::$bytes);
            const LEN: usize = $native_size;
            type Order = SizedByteOrd<$size>;
        }
    };
}

def_native_wrapper!(Bitmask08, u8, 1, 1, B1);
def_native_wrapper!(Bitmask16, u16, 2, 2, B2);
def_native_wrapper!(Bitmask24, u32, 3, 4, B3);
def_native_wrapper!(Bitmask32, u32, 4, 4, B4);
def_native_wrapper!(Bitmask40, u64, 5, 8, B5);
def_native_wrapper!(Bitmask48, u64, 6, 8, B6);
def_native_wrapper!(Bitmask56, u64, 7, 8, B7);
def_native_wrapper!(Bitmask64, u64, 8, 8, B8);
def_native_wrapper!(F32Range, f32, 4, 4, B4);
def_native_wrapper!(F64Range, f64, 8, 8, B8);

impl HasNativeType for AsciiRange {
    type Native = u64;
}

impl HasOneDatatype for AsciiRange {
    const DATATYPE: AlphaNumType = AlphaNumType::Ascii;
}

impl<T, const LEN: usize> HasOneDatatype for Bitmask<T, LEN> {
    const DATATYPE: AlphaNumType = AlphaNumType::Integer;
}

impl HasOneDatatype for F32Range {
    const DATATYPE: AlphaNumType = AlphaNumType::Float;
}

impl HasOneDatatype for F64Range {
    const DATATYPE: AlphaNumType = AlphaNumType::Double;
}

impl HasOneDatatype for AnyNullBitmask {
    const DATATYPE: AlphaNumType = AlphaNumType::Integer;
}

impl<T: HasOneDatatype> HasDatatype for T {
    fn datatype(&self) -> AlphaNumType {
        T::DATATYPE
    }

    fn datatype_from_columns(_: &[Self]) -> AlphaNumType {
        T::DATATYPE
    }
}

impl HasDatatype for NullMixedType {
    fn datatype(&self) -> AlphaNumType {
        match self {
            Self::Ascii(_) => AlphaNumType::Ascii,
            Self::Uint(_) => AlphaNumType::Integer,
            Self::F32(_) => AlphaNumType::Float,
            Self::F64(_) => AlphaNumType::Double,
        }
    }

    fn datatype_from_columns(cs: &[Self]) -> AlphaNumType {
        // If any numeric types are none, then that means at least one column is
        // ASCII, which means that $DATATYPE needs to be "A" since $PnDATATYPE
        // cannot be "A". Otherwise, find majority type.
        if let Some(xs) = NonEmpty::collect(cs.iter()) {
            xs.as_ref()
                .try_map(|c| NumType::try_from(c.datatype()))
                .ok()
                .map_or(AlphaNumType::Ascii, |mut ds| {
                    ds.sort();
                    (*FCSNonEmpty::from(ds).mode().0).into()
                })
        } else {
            // NOTE this is a totally arbitrary default
            AlphaNumType::Integer
        }
    }
}

impl<T, const LEN: usize> FromRange for Bitmask<T, LEN>
where
    T: TryFrom<Range, Error = RangeToIntError<T>> + PrimInt,
    u64: From<T>,
{
    type Error = RangeToBitmaskError;

    fn from_range(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<Self, DisallowRangeTrunc, Self::Error> {
        // TODO there is probably a better place to do this subtraction
        (range - Range::from(1_u8))
            .into_uint()
            .map_error(RangeToBitmaskError::from)
            .and_then_replace(|x| Self::try_from_native(x).map_error(RangeToBitmaskError::from))
            .nowarn_into_switchable(flag)
    }
}

impl<T, const LEN: usize> FromRange for FloatRange<T, LEN>
where
    T: HasFloatBounds,
{
    type Error = DecimalToFloatError;

    fn from_range(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<Self, DisallowRangeTrunc, Self::Error> {
        range
            .into_float()
            .map_deferred_value(Self::new)
            .nowarn_into_switchable(flag)
    }
}

impl FromRange for AsciiRange {
    type Error = RangeToIntError<()>;

    /// Make new AsciiRange from a float or integer.
    ///
    /// The number of chars will be automatically selected as the minimum
    /// required to express the range.
    fn from_range(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<Self, DisallowRangeTrunc, Self::Error> {
        range
            .into_uint::<u64>()
            .map_deferred_value(Self::from)
            .nowarn_into_switchable(flag)
    }
}

impl FromRange for AnyNullBitmask {
    type Error = RangeToIntError<()>;

    /// make a new bitmask from a float or integer.
    ///
    /// The size will be determined by the input and will be kept as small as
    /// possible.
    fn from_range(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<Self, DisallowRangeTrunc, Self::Error> {
        // TODO there is probably a better place to do this subtraction
        (range - Range::from(1_u8))
            .into_uint()
            .map_deferred_value(|x: u64| Self::from(x))
            .nowarn_into_switchable(flag)
    }
}

impl FromRange for NullMixedType {
    type Error = InsertRangeError;

    /// Create a mixed type based on the range.
    ///
    /// If int is supplied, return one of the uint types depending on size. If
    /// float is supplied, return f64 if range extends beyond the bounds of f32,
    /// otherwise use f32 (note that precision is not taken into consideration).
    ///
    /// ASCII will never be returned. This method will never fail.
    fn from_range(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<Self, DisallowRangeTrunc, Self::Error> {
        if range.0.is_integer() {
            AnyBitmask::from_range(range, flag)
                .map_deferred_value(Self::Uint)
                .map_switchable_errors(InsertRangeError::from)
        } else {
            FloatDecimal::<f32>::try_from(range.0)
                .map_or_else(
                    |e| FloatDecimal::<f64>::try_from(e.src).map(|r| Self::F64(FloatRange::new(r))),
                    |r| Ok(Self::F32(FloatRange::new(r))),
                )
                .map_or_else(
                    |e| {
                        // TODO kinda not dry
                        let m = if e.over {
                            f64::max_decimal()
                        } else {
                            f64::min_decimal()
                        };
                        let f = Self::F64(FloatRange::new(m));
                        SwitchableErrorResult::new_deferred_switchable(f, e, flag)
                            .map_switchable_errors(InsertRangeError::from)
                    },
                    |x| SwitchableErrorResult::new_switchable_ok(x, flag),
                )
        }
    }
}

impl<T, const LEN: usize> IsFixed for Bitmask<T, LEN>
where
    Self: HasNativeWidth,
    u64: From<T>,
    T: Copy,
{
    fn nbytes(&self) -> NonZeroU8 {
        Self::BYTES.into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        BitsOrChars(Self::BYTES.into())
    }

    fn range(&self) -> Range {
        self.into()
    }
}

impl<T, const LEN: usize> IsFixed for FloatRange<T, LEN>
where
    Self: HasNativeWidth,
    T: Copy,
    f64: From<T>,
{
    fn nbytes(&self) -> NonZeroU8 {
        Self::BYTES.into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        BitsOrChars(Self::BYTES.into())
    }

    fn range(&self) -> Range {
        self.range.clone().into()
    }
}

impl IsFixed for AsciiRange {
    fn nbytes(&self) -> NonZeroU8 {
        self.chars().into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        BitsOrChars(self.chars().into())
    }

    fn range(&self) -> Range {
        Range(self.value().into())
    }
}

impl IsFixed for AnyNullBitmask {
    fn nbytes(&self) -> NonZeroU8 {
        match_any_uint!(self, Self, x, { x.nbytes() })
    }

    fn fixed_width(&self) -> BitsOrChars {
        match_any_uint!(self, Self, x, { x.fixed_width() })
    }

    fn range(&self) -> Range {
        match_any_uint!(self, Self, x, { x.range() })
    }
}

impl IsFixed for NullMixedType {
    fn nbytes(&self) -> NonZeroU8 {
        match_any_mixed!(self, x, { x.nbytes() })
    }

    fn fixed_width(&self) -> BitsOrChars {
        match_any_mixed!(self, x, { x.fixed_width() })
    }

    fn range(&self) -> Range {
        match_any_mixed!(self, x, { x.range() })
    }
}

macro_rules! source_from_iter {
    ($from:ident, $to:ident, $wrap:ident) => {
        impl<'a> From<FCSColIter<'a, $from, $to>> for AnySource<'a, $to> {
            fn from(value: FCSColIter<'a, $from, $to>) -> Self {
                Self::$wrap(value)
            }
        }
    };
}

source_from_iter!(u8, u8, FromU08);
source_from_iter!(u8, u16, FromU08);
source_from_iter!(u8, u32, FromU08);
source_from_iter!(u8, u64, FromU08);
source_from_iter!(u8, f32, FromU08);
source_from_iter!(u8, f64, FromU08);

source_from_iter!(u16, u8, FromU16);
source_from_iter!(u16, u16, FromU16);
source_from_iter!(u16, u32, FromU16);
source_from_iter!(u16, u64, FromU16);
source_from_iter!(u16, f32, FromU16);
source_from_iter!(u16, f64, FromU16);

source_from_iter!(u32, u8, FromU32);
source_from_iter!(u32, u16, FromU32);
source_from_iter!(u32, u32, FromU32);
source_from_iter!(u32, u64, FromU32);
source_from_iter!(u32, f32, FromU32);
source_from_iter!(u32, f64, FromU32);

source_from_iter!(u64, u8, FromU64);
source_from_iter!(u64, u16, FromU64);
source_from_iter!(u64, u32, FromU64);
source_from_iter!(u64, u64, FromU64);
source_from_iter!(u64, f32, FromU64);
source_from_iter!(u64, f64, FromU64);

source_from_iter!(f32, u8, FromF32);
source_from_iter!(f32, u16, FromF32);
source_from_iter!(f32, u32, FromF32);
source_from_iter!(f32, u64, FromF32);
source_from_iter!(f32, f32, FromF32);
source_from_iter!(f32, f64, FromF32);

source_from_iter!(f64, u8, FromF64);
source_from_iter!(f64, u16, FromF64);
source_from_iter!(f64, u32, FromF64);
source_from_iter!(f64, u64, FromF64);
source_from_iter!(f64, f32, FromF64);
source_from_iter!(f64, f64, FromF64);

impl<T> Default for AnyOrderedUintLayout<T> {
    fn default() -> Self {
        Self::Uint32(FixedLayout::default())
    }
}

impl<T> AnyOrderedUintLayout<T> {
    #[must_use]
    pub fn phantom_into<X>(self) -> AnyOrderedUintLayout<X> {
        match_any_uint!(self, Self, l, { l.phantom_into().into() })
    }

    fn into_endian<D>(self) -> Result<EndianLayout<AnyNullBitmask, D>, OrderedToEndianError> {
        match_any_uint!(self, Self, l, {
            l.phantom_into()
                .byte_layout_try_into()
                .map(FixedLayout::columns_into)
        })
    }

    fn try_new(
        cs: Vec<ColumnLayoutValues<Nothing<NumType>>>,
        bo: ByteOrd2_0,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), IndexedBitmaskError, NewFixedIntLayoutError> {
        let notrunc = conf.disallow_range_truncation;
        let real_bo = conf.integer_byteord_override.unwrap_or(bo);
        let n = real_bo.nbytes();

        // First, scan through the widths to make sure they are all fixed and
        // are all the same number of bytes as ByteOrd. Skip this step if we
        // are ignoring $PnB for width and simply using the length of $BYTEORD.
        let width_res = if conf.integer_widths_from_byteord {
            LogResult::new_ok(())
        } else {
            cs.iter()
                .map(|c| c.width)
                .enumerate()
                .map(|(i, c)| {
                    PrivBytes::try_from(c)
                        .map_err(|e| IndexedError::new(i, e))
                        .map_err(IndexedWidthToBytesError)
                        .map_err(SingleFixedWidthError::from)
                })
                .map(Result::into_log::<_, _, Vec<_>>)
                .mappend_commutative()
                .and_then_commutative(|widths| {
                    let ws = widths.into_iter().filter(|&w| w != n);
                    if let Some(mismatches) = NonEmpty::collect(ws) {
                        let e = WidthMismatchError::new(real_bo, mismatches);
                        LogResult::new_err(SingleFixedWidthError::from(e))
                    } else {
                        LogResult::new_ok(())
                    }
                })
        };

        // Second, make the layout, and force all columns to the correct type
        // based on ByteOrd. It is necessary to check the columns first because
        // the bitmask won't necessarily fail even if it is larger than the
        // target type.
        //
        // NOTE this step is independent of $PnB, so downstream control flow is
        // dictated by warnings/errors
        let layout_res =
            match_many_to_one!(real_bo, ByteOrd2_0, [O1, O2, O3, O4, O5, O6, O7, O8], o, {
                FixedLayout::try_new(cs, o, |i, c| {
                    Bitmask::from_range(c.range, notrunc)
                        .map_switchable_errors(|e| e.into_indexed_err(i))
                        .switchable_into_commutative()
                        .into_semigroup()
                })
                .set_err_value(())
                .map_errors(NewFixedIntLayoutError::from)
                .map_ok_value(Self::from)
            });

        width_res
            .nowarn_into_warn()
            .map_errors(NewFixedIntLayoutError::from)
            .zip_commutative(layout_res)
            .map_ok_value(|((), layout)| layout)
    }
}

impl<T, D, const ORD: bool> Default for AnyAsciiLayout<T, D, ORD> {
    fn default() -> Self {
        Self::Fixed(FixedLayout::default())
    }
}

impl<T, D, const ORD: bool> AnyAsciiLayout<T, D, ORD> {
    #[must_use]
    pub fn phantom_into<T1, D1, const ORD_1: bool>(self) -> AnyAsciiLayout<T1, D1, ORD_1> {
        match self {
            Self::Delimited(x) => DelimAsciiLayout::new(x.ranges).into(),
            Self::Fixed(x) => FixedLayout::new(x.columns, NoByteOrd).into(),
        }
    }

    pub(crate) fn try_new(
        cs: Vec<ColumnLayoutValues<D>>,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<Self, (), IndexedRangeToIntError, AsciiRangeFromKeywordsError>
    where
        D: IsNumType,
    {
        if cs.iter().all(|c| c.width == Width::Variable) {
            cs.into_iter()
                .enumerate()
                .map(|(i, c)| {
                    c.range
                        .into_uint::<u64>()
                        .nowarn_into_switchable(flag)
                        .map_switchable_errors(|e| IndexedError::new(i, e))
                        .map_switchable_errors(IndexedRangeToIntError)
                        .switchable_into_commutative()
                        .map_errors(AsciiRangeFromKeywordsError::from)
                        .repack()
                })
                .mappend_def()
                .map_ok_value(|ranges| DelimAsciiLayout::new(ranges).into())
                .map_err_value(|_| ())
        } else {
            FixedLayout::try_new(cs, NoByteOrd, |i, c| {
                AsciiRange::from_width_and_range(c.width, c.range, i, flag)
            })
            .map_ok_value(Self::from)
        }
    }

    fn new_fixed(columns: Vec<AsciiRange>) -> Self {
        Self::Fixed(FixedLayout::new(columns, NoByteOrd))
    }

    fn new_delim(ranges: Vec<u64>) -> Self {
        Self::Delimited(DelimAsciiLayout::new(ranges))
    }
}

impl<T, D, const ORD: bool> FixedAsciiLayout<T, D, ORD> {
    pub fn new_ascii_u64(ranges: Vec<u64>) -> Self {
        let rs = ranges.into_iter().map(AsciiRange::from).collect();
        Self::new_ascii(rs)
    }

    #[must_use]
    pub fn new_ascii(ranges: Vec<AsciiRange>) -> Self {
        Self::new(ranges, NoByteOrd)
    }
}

impl<T, const LEN: usize, TC> OrderedLayout<Bitmask<T, LEN>, TC>
where
    Bitmask<T, LEN>: HasNativeWidth<Order = SizedByteOrd<LEN>>,
{
    #[must_use]
    pub fn new_endian_uint(ranges: Vec<Bitmask<T, LEN>>, endian: Endian) -> Self {
        Self::new(ranges, SizedByteOrd::Endian(endian))
    }
}

impl<T, const LEN: usize, TC> OrderedLayout<FloatRange<T, LEN>, TC>
where
    FloatRange<T, LEN>: HasNativeWidth<Order = SizedByteOrd<LEN>>,
{
    #[must_use]
    pub fn new_endian_float(ranges: Vec<FloatRange<T, LEN>>, endian: Endian) -> Self {
        Self::new(ranges, SizedByteOrd::Endian(endian))
    }
}

impl VersionedDataLayout for DataLayout2_0 {
    type ByteLayout = ByteOrd2_0;
    type NumType = Nothing<NumType>;
    type Tot = Option<Tot>;

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
        par: Par,
    ) -> LookupStdLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        AnyOrderedLayout::lookup(std, nonstd, conf, par).map_ok_value(Self::from)
    }

    fn lookup_ro<C>(kws: &StdKeywords, conf: &C) -> LookupRawLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig>,
    {
        AnyOrderedLayout::lookup_ro(kws, conf).map_ok_value(Self::from)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        AnyOrderedLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Self::NumType>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), NewMixedTypeWarning, NewDataLayoutError> {
        AnyOrderedLayout::try_new(datatype, byteord, columns, conf).map_ok_value(Self::from)
    }
}

impl VersionedDataLayout for DataLayout3_0 {
    type ByteLayout = ByteOrd2_0;
    type NumType = Nothing<NumType>;
    type Tot = Identity<Tot>;

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
        par: Par,
    ) -> LookupStdLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        AnyOrderedLayout::lookup(std, nonstd, conf, par).map_ok_value(Into::into)
    }

    fn lookup_ro<C>(kws: &StdKeywords, conf: &C) -> LookupRawLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig>,
    {
        AnyOrderedLayout::lookup_ro(kws, conf).map_ok_value(Self::from)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        AnyOrderedLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Nothing<NumType>>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), NewMixedTypeWarning, NewDataLayoutError> {
        AnyOrderedLayout::try_new(datatype, byteord, columns, conf).map_ok_value(Self::from)
    }
}

impl VersionedDataLayout for DataLayout3_1 {
    type ByteLayout = Endian;
    type NumType = Nothing<NumType>;
    type Tot = Identity<Tot>;

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
        par: Par,
    ) -> LookupStdLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        NonMixedEndianLayout::lookup(std, nonstd, conf, par).map_ok_value(Self::from)
    }

    fn lookup_ro<C>(kws: &StdKeywords, conf: &C) -> LookupRawLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig>,
    {
        NonMixedEndianLayout::lookup_ro(kws, conf).map_ok_value(Self::from)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        NonMixedEndianLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Nothing<NumType>>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), NewMixedTypeWarning, NewDataLayoutError> {
        NonMixedEndianLayout::try_new(datatype, byteord, columns, conf).map_ok_value(Into::into)
    }
}

impl VersionedDataLayout for DataLayout3_2 {
    type ByteLayout = ByteOrd3_1;
    type NumType = Option<NumType>;
    type Tot = Identity<Tot>;

    // TODO each instance we use both keyword types can just be ValidKeywords
    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
        par: Par,
    ) -> LookupStdLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        let datatype = AlphaNumType::remove_req_check_ascii(std);
        let endian = ByteOrd3_1::remove_metaroot_req(std);
        let columns = Option::lookup_all(std, nonstd, par, conf.as_ref());
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    fn lookup_ro<C>(kws: &StdKeywords, conf: &C) -> LookupRawLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig>,
    {
        let datatype = AlphaNumType::get_req_check_ascii(kws);
        let endian = ByteOrd3_1::get_metaroot_req(kws);
        let columns = Option::<NumType>::lookup_ro_all(kws);
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        NonMixedEndianLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Option<NumType>>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), NewMixedTypeWarning, NewDataLayoutError> {
        let notrunc = conf.disallow_range_truncation;
        let unique_dt: Vec<_> = columns
            .iter()
            .map(|c| c.datatype.map_or(datatype, Into::into))
            .unique()
            .collect();
        match unique_dt[..] {
            // no columns, therefore undetermined datatype, use whatever the
            // default layout is
            //
            // ASSUME this matches with Self::new_empty above
            [] => LogResult::new_ok(NonMixedEndianLayout::new_empty1(datatype, byteord.0).into()),
            // has columns with one datatype, use nonmixed layout
            [dt] => {
                let ds = columns
                    .into_iter()
                    .map(|c| ColumnLayoutValues::new(c.width, c.range, Nothing::default()))
                    .collect();
                NonMixedEndianLayout::try_new(dt, byteord.0, ds, conf)
                    .map_ok_value(|x| Self::NonMixed(x.phantom_into::<Option<NumType>>()))
            }
            // has columns with 1+ datatypes, use mixed layout
            _ => {
                let go = |i: MeasIndex, c: ColumnLayoutValues3_2| {
                    MixedType::from_width_and_range(c.width, c.range, c.datatype, i, notrunc)
                };
                FixedLayout::try_new(columns, byteord.0, go)
                    .map_errors(NewDataLayoutError::from)
                    .map_ok_value(Self::from)
            }
        }
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        LogResult::new_ok(Self(value.0.phantom_into()))
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        value.into_ordered().map_ok_value(Into::into)
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        value.into_ordered().map_ok_value(Into::into)
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        LogResult::new_ok(Self(value.0.phantom_into()))
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        value.into_ordered().map_ok_value(Into::into)
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        value.into_ordered().map_ok_value(Into::into)
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        value.0.into_3_1()
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        value.0.into_3_1()
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        match value {
            DataLayout3_2::NonMixed(x) => LogResult::new_ok(Self(x.phantom_into())),
            DataLayout3_2::Mixed(x) => x
                .try_into_non_mixed()
                .map_ok_value(Self)
                .map_errors(LayoutConvertError::from),
        }
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        value.0.into_3_2()
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        value.0.into_3_2()
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        LogResult::new_ok(Self::NonMixed(value.0.phantom_into()))
    }
}

impl CheckedScaleTransform for Scale {
    type Err = ScaleMismatchError;

    fn matches_datatype(&self, datatype: AlphaNumType, i: MeasIndex) -> Result<(), Self::Err> {
        if datatype != AlphaNumType::Integer && matches!(self, Self::Log(_)) {
            return Err(ScaleMismatchError::new(i, datatype, *self));
        }
        Ok(())
    }
}

impl CheckedScaleTransform for ScaleTransform {
    type Err = ScaleTransformMismatchError;

    fn matches_datatype(&self, datatype: AlphaNumType, i: MeasIndex) -> Result<(), Self::Err> {
        if datatype != AlphaNumType::Integer && !self.is_noop() {
            return Err(ScaleTransformMismatchError::new(i, datatype, *self));
        }
        Ok(())
    }
}

impl InterLayoutOps<Option<NumType>> for DataLayout3_2 {
    fn opt_meas_headers(&self) -> Vec<MeasHeader> {
        vec![NumType::std_blank()]
    }

    fn opt_meas_keywords(&self) -> Vec<Vec<(String, Option<String>)>> {
        let dt = self.datatype();
        match self {
            Self::NonMixed(x) => (0..x.ncols())
                .map(|i| vec![(NumType::std(i).to_string(), None)])
                .collect(),
            Self::Mixed(x) => x
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let y: Option<NumType> = NumType::try_from(c.datatype())
                        .ok()
                        .and_then(|y| (AlphaNumType::from(y) != dt).then_some(y));
                    vec![y.meas_opt_pair(i)]
                })
                .collect(),
        }
    }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> SwitchableErrorResult<(), (), DisallowRangeTrunc, InsertRangeError> {
        match mem::replace(self, Self::mixed_dummy()) {
            // If layout is mixed, interpret range as a mixed type
            Self::Mixed(mut x) => x
                .insert_nocheck(index, range, flag)
                .set_deferred_value(Self::Mixed(x)),
            // If layout is non-mixed, interpret range as an ASCII range and
            // keep the layout as ASCII. Otherwise, interpret as a mixed range
            // and convert the layout to a mixed layout if the interpreted
            // result is different from the rest of the types in the layout.
            Self::NonMixed(x) => match x {
                NonMixedEndianLayout::Ascii(mut y) => y
                    .insert_nocheck(index, range, flag)
                    .set_deferred_value(Self::NonMixed(y.into())),
                NonMixedEndianLayout::Integer(y) => y.insert_mixed(index, range, flag),
                NonMixedEndianLayout::F32(y) => y.insert_mixed(index, range, flag),
                NonMixedEndianLayout::F64(y) => y.insert_mixed(index, range, flag),
            },
        }
        .map_deferred_value(|newself| {
            *self = newself;
        })
    }

    fn push(
        &mut self,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<(), DisallowRangeTrunc, InsertRangeError> {
        match mem::replace(self, Self::mixed_dummy()) {
            Self::Mixed(mut x) => x.push(range, flag).set_deferred_value(Self::Mixed(x)),
            Self::NonMixed(x) => match x {
                NonMixedEndianLayout::Ascii(mut y) => y
                    .push(range, flag)
                    .set_deferred_value(Self::NonMixed(y.into())),
                NonMixedEndianLayout::Integer(y) => y.push_mixed(range, flag),
                NonMixedEndianLayout::F32(y) => y.push_mixed(range, flag),
                NonMixedEndianLayout::F64(y) => y.push_mixed(range, flag),
            },
        }
        .map_deferred_value(|newself| {
            *self = newself;
        })
    }

    fn clear(&mut self) {
        *self = match mem::replace(self, Self::mixed_dummy()) {
            Self::Mixed(x) => NonMixedEndianLayout::new_empty(x.datatype()).into(),
            Self::NonMixed(mut x) => {
                x.clear();
                Self::NonMixed(x)
            }
        }
    }
}

impl DataLayout3_1 {
    pub(crate) fn into_ordered<T>(self) -> LayoutConvertResult<AnyOrderedLayout<T>> {
        self.0.into_ordered()
    }
}

impl DataLayout3_2 {
    pub(crate) fn into_ordered<T>(self) -> LayoutConvertResult<AnyOrderedLayout<T>> {
        match self {
            Self::NonMixed(x) => x.into_ordered(),
            Self::Mixed(x) => x.try_into_ordered().map_errors(LayoutConvertError::from),
        }
    }

    #[must_use]
    pub fn new_mixed(ranges: Vec<NullMixedType>, endian: Endian) -> Self {
        // Check if the mixed types are all the same, in which case we can use a
        // simpler layout. This clone thing is not ideal but it will only be
        // cloning big-decimals for floats and will use Copy for everything else
        // (not a huge deal).
        if let Some(xs) = NonEmpty::from_vec(ranges) {
            if let Ok(rs) = xs.as_ref().try_map(|x| AsciiRange::try_from(x.clone())) {
                NonMixedEndianLayout::new_ascii_fixed(rs.into()).into()
            } else if let Ok(rs) = xs.as_ref().try_map(|x| AnyNullBitmask::try_from(x.clone())) {
                NonMixedEndianLayout::new_uint(rs.into(), endian).into()
            } else if let Ok(rs) = xs.as_ref().try_map(|x| F32Range::try_from(x.clone())) {
                NonMixedEndianLayout::new_f32(rs.into(), endian).into()
            } else if let Ok(rs) = xs.as_ref().try_map(|x| F64Range::try_from(x.clone())) {
                NonMixedEndianLayout::new_f64(rs.into(), endian).into()
            } else {
                FixedLayout::new(xs.into(), endian).into()
            }
        } else {
            FixedLayout::new(vec![], endian).into()
        }
    }

    // pub fn datatypes(&self) -> NonEmpty<AlphaNumType> {
    //     match self {
    //         // somewhat hacky way of getting a nonempty in a type-safe way
    //         Self::NonMixed(x) => LayoutOps::ranges(x)
    //             .as_ref()
    //             .map(|_| LayoutOps::datatype(x)),
    //         Self::Mixed(x) => x.columns.as_ref().map(|y| y.datatype()),
    //     }
    // }

    /// A dummy layout, used to make [`std::mem::replace`] work; not meaninful.
    fn mixed_dummy() -> Self {
        NonMixedEndianLayout::from(AnyAsciiLayout::from(DelimAsciiLayout::new(vec![]))).into()
    }

    fn lookup_inner<C, E>(
        datatype: LookupDatatypeResult<AlphaNumType>,
        endian: Result<ByteOrd3_1, ReqKeyError<ByteOrd3_1>>,
        columns: LookupMeasLayoutResult<Option<NumType>, E>,
        conf: &C,
    ) -> LookupLayoutResult<Self, LookupLayoutError<E>>
    where
        C: AsRef<ReadLayoutConfig>,
    {
        let endian_ = endian.map_err(LookupLayoutError::from).into_log();
        let columns_ = columns
            .map_commutative_warnings(LookupLayoutWarning::from)
            .map_errors(LookupLayoutError::Meas);
        datatype
            .map_commutative_warnings(LookupLayoutWarning::from)
            .map_errors(LookupLayoutError::from)
            .into_semigroup()
            .zip3_commutative(endian_, columns_)
            .and_then_commutative(|(d, e, cs)| {
                Self::try_new(d, e, cs, conf.as_ref())
                    .map_commutative_warnings(LookupLayoutWarning::from)
                    .map_errors(LookupLayoutError::from)
            })
    }
}

impl<T> Default for AnyOrderedLayout<T> {
    fn default() -> Self {
        Self::Integer(AnyOrderedUintLayout::default())
    }
}

impl<T> AnyOrderedLayout<T> {
    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
        par: Par,
    ) -> LookupStdLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        let datatype = AlphaNumType::remove_metaroot_req(std);
        let byteord = ByteOrd2_0::remove_metaroot_req(std);
        let columns = Nothing::lookup_all(std, nonstd, par, conf.as_ref());
        Self::lookup_inner(datatype, byteord, columns, conf)
    }

    fn lookup_ro<C>(kws: &StdKeywords, conf: &C) -> LookupRawLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig>,
    {
        let datatype = AlphaNumType::get_metaroot_req(kws);
        let byteord = ByteOrd2_0::get_metaroot_req(kws);
        let columns = Nothing::<NumType>::lookup_ro_all(kws);
        Self::lookup_inner(datatype, byteord, columns, conf)
    }

    fn lookup_inner<C, E>(
        datatype: Result<AlphaNumType, ReqKeyError<AlphaNumType>>,
        byteord: Result<ByteOrd2_0, ReqKeyError<ByteOrd2_0>>,
        columns: LookupMeasLayoutResult<Nothing<NumType>, E>,
        conf: &C,
    ) -> LookupLayoutResult<Self, LookupLayoutError<E>>
    where
        C: AsRef<ReadLayoutConfig>,
    {
        let byteord_ = byteord.map_err(LookupLayoutError::from).into_log();
        let columns_ = columns
            .map_commutative_warnings(LookupLayoutWarning::from)
            .map_errors(LookupLayoutError::Meas);
        datatype
            .map_err(LookupLayoutError::from)
            .into_log()
            .zip3_commutative(byteord_, columns_)
            .and_then_commutative(|(d, e, cs)| {
                Self::try_new(d, e, cs, conf.as_ref())
                    .map_commutative_warnings(LookupLayoutWarning::from)
                    .map_errors(LookupLayoutError::from)
            })
    }

    #[must_use]
    pub fn new_ascii_fixed(ranges: Vec<AsciiRange>) -> Self {
        AnyAsciiLayout::new_fixed(ranges).into()
    }

    #[must_use]
    pub fn new_ascii_delim(ranges: Vec<u64>) -> Self {
        AnyAsciiLayout::new_delim(ranges).into()
    }

    #[must_use]
    pub fn new_uint<U, const LEN: usize>(
        columns: Vec<Bitmask<U, LEN>>,
        byte_layout: SizedByteOrd<LEN>,
    ) -> Self
    where
        AnyOrderedUintLayout<T>:
            From<FixedLayout<Bitmask<U, LEN>, SizedByteOrd<LEN>, T, Nothing<NumType>>>,
    {
        Self::Integer(FixedLayout::new(columns, byte_layout).into())
    }

    #[must_use]
    pub fn new_f32(ranges: Vec<F32Range>, byte_layout: SizedByteOrd<4>) -> Self {
        FixedLayout::new(ranges, byte_layout).into()
    }

    #[must_use]
    pub fn new_f64(ranges: Vec<F64Range>, byte_layout: SizedByteOrd<8>) -> Self {
        FixedLayout::new(ranges, byte_layout).into()
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        match datatype {
            AlphaNumType::Ascii => AnyAsciiLayout::default().into(),
            AlphaNumType::Integer => AnyOrderedUintLayout::default().into(),
            AlphaNumType::Float => Self::F32(FixedLayout::default()),
            AlphaNumType::Double => Self::F64(FixedLayout::default()),
        }
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: ByteOrd2_0,
        columns: Vec<ColumnLayoutValues2_0>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), NewMixedTypeWarning, NewDataLayoutError> {
        macro_rules! from {
            ($i:expr) => {
                $i.map_errors(NewDataLayoutError::from)
                    .map_commutative_warnings(NewMixedTypeWarning::from)
                    .map_ok_value(Self::from)
            };
        }

        macro_rules! go_float {
            ($t:ident, $notrunc:expr) => {
                byteord
                    .try_into()
                    .map_err(NewDataLayoutError::from)
                    .into_log()
                    .and_then_commutative(|b| {
                        from! {FixedLayout::try_new(columns, b, |i, c| {
                            $t::from_width_and_range(c.width, c.range, i, $notrunc)
                                .repack_errors()
                        })}
                    })
            };
        }

        let notrunc = conf.disallow_range_truncation;

        match datatype {
            AlphaNumType::Ascii => from!(AnyAsciiLayout::try_new(columns, notrunc)),
            AlphaNumType::Integer => from!(AnyOrderedUintLayout::try_new(columns, byteord, conf)),
            AlphaNumType::Float => go_float!(F32Range, notrunc),
            AlphaNumType::Double => go_float!(F64Range, notrunc),
        }
    }

    #[must_use]
    pub fn phantom_into<X>(self) -> AnyOrderedLayout<X> {
        match_many_to_one!(self, Self, [Ascii, Integer, F32, F64], x, {
            x.phantom_into().into()
        })
    }

    pub fn into_unmixed<D>(self) -> LayoutConvertResult<NonMixedEndianLayout<D>> {
        macro_rules! go_float {
            ($i:expr) => {
                $i.phantom_into()
                    .byte_layout_try_into()
                    .map(NonMixedEndianLayout::from)
                    .into_log::<_, _, Vec<_>>()
            };
        }
        let res = match self {
            Self::Ascii(x) => LogResult::new_ok(NonMixedEndianLayout::from(x.phantom_into())),
            Self::Integer(x) => x.into_endian().map(NonMixedEndianLayout::from).into_log(),
            Self::F32(x) => go_float!(x),
            Self::F64(x) => go_float!(x),
        };
        res.map_errors(LayoutConvertError::from)
    }

    pub(crate) fn into_3_1(self) -> LayoutConvertResult<DataLayout3_1> {
        self.into_unmixed().map_ok_value(Into::into)
    }

    pub(crate) fn into_3_2(self) -> LayoutConvertResult<DataLayout3_2> {
        self.into_unmixed().map_ok_value(DataLayout3_2::NonMixed)
    }
}

impl NonMixedEndianLayout<Nothing<NumType>> {
    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
        par: Par,
    ) -> LookupStdLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        let datatype = AlphaNumType::remove_req_check_ascii(std);
        let endian = ByteOrd3_1::remove_metaroot_req(std);
        let columns = Nothing::<NumType>::lookup_all(std, nonstd, par, conf.as_ref());
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    fn lookup_ro<C>(kws: &StdKeywords, conf: &C) -> LookupRawLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig>,
    {
        let datatype = AlphaNumType::get_req_check_ascii(kws);
        let endian = ByteOrd3_1::get_metaroot_req(kws);
        let columns = Nothing::<NumType>::lookup_ro_all(kws);
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    // TODO this is almost like the 3.2 version
    fn lookup_inner<C, E>(
        datatype: LookupDatatypeResult<AlphaNumType>,
        endian: Result<ByteOrd3_1, ReqKeyError<ByteOrd3_1>>,
        columns: LookupMeasLayoutResult<Nothing<NumType>, E>,
        conf: &C,
    ) -> LookupLayoutResult<Self, LookupLayoutError<E>>
    where
        C: AsRef<ReadLayoutConfig>,
    {
        let endian_ = endian.map_err(LookupLayoutError::from).into_log();
        let columns_ = columns
            .map_commutative_warnings(LookupLayoutWarning::from)
            .map_errors(LookupLayoutError::Meas);
        datatype
            .map_commutative_warnings(LookupLayoutWarning::from)
            .map_errors(LookupLayoutError::from)
            .into_semigroup()
            .zip3_commutative(endian_, columns_)
            .and_then_commutative(|(d, e, cs)| {
                Self::try_new(d, e.0, cs, conf.as_ref())
                    .map_commutative_warnings(LookupLayoutWarning::from)
                    .map_errors(LookupLayoutError::from)
            })
    }

    fn try_new(
        datatype: AlphaNumType,
        endian: Endian,
        columns: Vec<ColumnLayoutValues<Nothing<NumType>>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), NewMixedTypeWarning, NewDataLayoutError> {
        let notrunc = conf.disallow_range_truncation;

        let go_f32 = |i: MeasIndex, c: ColumnLayoutValues<_>| {
            F32Range::from_width_and_range(c.width, c.range, i, notrunc).repack_errors()
        };

        let go_f64 = |i: MeasIndex, c: ColumnLayoutValues<_>| {
            F64Range::from_width_and_range(c.width, c.range, i, notrunc).repack_errors()
        };

        macro_rules! from {
            ($x:expr) => {
                $x.map_errors(NewDataLayoutError::from)
                    .map_commutative_warnings(NewMixedTypeWarning::from)
                    .map_ok_value(Self::from)
            };
        }

        match datatype {
            AlphaNumType::Ascii => from!(AnyAsciiLayout::try_new(columns, notrunc)),
            AlphaNumType::Integer => {
                from!(FixedLayout::endian_uint_try_new(columns, endian, notrunc))
            }
            AlphaNumType::Float => from!(FixedLayout::try_new(columns, endian, go_f32)),
            AlphaNumType::Double => from!(FixedLayout::try_new(columns, endian, go_f64)),
        }
    }
}

impl<D> NonMixedEndianLayout<D> {
    fn new_empty(datatype: AlphaNumType) -> Self {
        Self::new_empty1(datatype, Endian::default())
    }

    fn new_empty1(datatype: AlphaNumType, endian: Endian) -> Self {
        match datatype {
            AlphaNumType::Ascii => AnyAsciiLayout::default().into(),
            AlphaNumType::Integer => Self::Integer(FixedLayout::new_empty(endian)),
            AlphaNumType::Float => Self::F32(FixedLayout::new_empty(endian)),
            AlphaNumType::Double => Self::F64(FixedLayout::new_empty(endian)),
        }
    }

    #[must_use]
    pub fn new_ascii_fixed(ranges: Vec<AsciiRange>) -> Self {
        AnyAsciiLayout::new_fixed(ranges).into()
    }

    #[must_use]
    pub fn new_ascii_delim(ranges: Vec<u64>) -> Self {
        AnyAsciiLayout::new_delim(ranges).into()
    }

    #[must_use]
    pub fn new_uint(columns: Vec<AnyNullBitmask>, endian: Endian) -> Self {
        FixedLayout::new(columns, endian).into()
    }

    #[must_use]
    pub fn new_f32(ranges: Vec<F32Range>, endian: Endian) -> Self {
        FixedLayout::new(ranges, endian).into()
    }

    #[must_use]
    pub fn new_f64(ranges: Vec<F64Range>, endian: Endian) -> Self {
        FixedLayout::new(ranges, endian).into()
    }

    pub(crate) fn into_ordered<T>(self) -> LayoutConvertResult<AnyOrderedLayout<T>> {
        match self {
            Self::Ascii(x) => LogResult::new_ok(x.phantom_into().into()),
            Self::Integer(x) => x
                .uint_try_into_ordered()
                .map_ok_value(Into::into)
                .map_errors(LayoutConvertError::from),
            Self::F32(x) => LogResult::new_ok(x.phantom_into().byte_layout_into().into()),
            Self::F64(x) => LogResult::new_ok(x.phantom_into().byte_layout_into().into()),
        }
    }

    #[must_use]
    pub fn phantom_into<D1>(self) -> NonMixedEndianLayout<D1> {
        match_many_to_one!(self, Self, [Ascii, Integer, F32, F64], x, {
            x.phantom_into().into()
        })
    }
}

/// Error when keywords cannot be used to make a new layout.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewDataLayoutError {
    /// $PnB and $PnR could not be used to make ASCII column
    Ascii(AsciiRangeFromKeywordsError),
    /// $PnB and $PnR could not be used to make integer column (2.0/3.0)
    FixedInt(NewFixedIntLayoutError),
    /// $PnB and $PnR could not be used to make integer column (3.1/3.2)
    VariableInt(NewUintTypeError),
    /// $PnB and $PnR could not be used to make float column
    Float(FloatWidthError),
    /// $PnB and $PnR could not be used to make mixed column (3.2)
    Mixed(NewMixedTypeError),
    /// $BYTEORD does not match width allowed via $DATATYPE for float layout (2.0/3.0)
    ByteOrd(ByteOrdToSizedError),
}

/// Error when $PnB or $PnR cannot be used for an ordered integer layout (2.0/3.0 only)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewFixedIntLayoutError {
    Width(SingleFixedWidthError),
    Column(IndexedBitmaskError),
}

/// Error when $PnB cannot be used for an ordered integer layout (2.0/3.0 only)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SingleFixedWidthError {
    Bytes(IndexedWidthToBytesError),
    Width(WidthMismatchError),
}

/// Error when $PnB does not match width implied by $BYTEORD (2.0/3.0 only)
#[derive(Debug, Error, new)]
// TODO use correct error type
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::DataLossError))]
pub struct WidthMismatchError {
    byteord: ByteOrd2_0,
    found: NonEmpty<PrivBytes>,
}

impl fmt::Display for WidthMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if self.found.tail.is_empty() {
            write!(
                f,
                "measurement width ({}) does not match byte order ({})",
                self.found.head, self.byteord,
            )
        } else {
            write!(
                f,
                "multiple measurement widths given ({}) for byte order [{}]",
                self.found.iter().join(", "),
                self.byteord,
            )
        }
    }
}

/// Error when using $PnB and $PnR to make a new mixed type column.
///
/// This only applies to FCS 3.2 and the value of $PnDATATYPE is implied by
/// the variant of this enum.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewMixedTypeError {
    Ascii(AsciiRangeFromKeywordsError),
    Uint(NewUintTypeError),
    Float(FloatWidthError),
}

/// Warning when failing to truncate $PnR for use in a 3.2 mixed type layout.
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewMixedTypeWarning {
    Ascii(IndexedRangeToIntError),
    Uint(IndexedBitmaskError),
    Float(IndexedFloatRangeError),
}

/// Error when parsing $PnR to be used in a float layout.
#[derive(From, Debug, Error)]
#[error(
    "could not use {k} in float layout because {e}",
    k = Range::std(_0.index),
    e = _0.error
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::DataLossError))]
pub struct IndexedFloatRangeError(IndexedError<DecimalToFloatError>);

/// Error when using $PnB or $PnR to make a new integer bitmask
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewUintTypeError {
    Bitmask(IndexedBitmaskError),
    Bytes(IndexedWidthToBytesError),
}

/// Error when converting $PnB (in bits) to bytes
#[derive(From, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::DataLossError))]
pub struct IndexedWidthToBytesError(IndexedError<WidthToFixedError<BytesError>>);

impl fmt::Display for IndexedWidthToBytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let k = Width::std(self.0.index);
        match &self.0.error {
            WidthToFixedError::Fixed(e) => {
                write!(f, "could not convert {k} to bytes because {e}")
            }
            WidthToFixedError::Variable(_) => {
                write!(f, "{k} is variable ('*') when fixed is needed")
            }
        }
    }
}

/// Error when using $PnB or $PnR for float layout.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum FloatWidthError {
    Bytes(IndexedWidthToBytesError),
    WrongWidth(WrongFloatWidth),
    Range(IndexedFloatRangeError),
}

#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum IndexedBitmaskError {
    ToInt(IndexedRangeToIntError),
    Trunc(IndexedBitmaskTruncationError),
}

#[derive(From, Display, Debug)]
pub enum RangeToBitmaskError {
    ToInt(RangeToIntError<()>),
    Trunc(BitmaskTruncationError),
}

impl RangeToBitmaskError {
    fn into_indexed_err(self, i: MeasIndex) -> IndexedBitmaskError {
        match self {
            Self::ToInt(e) => IndexedRangeToIntError(IndexedError::new(i, e)).into(),
            Self::Trunc(e) => IndexedBitmaskTruncationError(IndexedError::new(i, e)).into(),
        }
    }
}

// TODO these errors could be combined since truncation is a subset of the latter
#[derive(From, Debug, Error)]
#[error(
    "{pnr} ({r}) is larger than {b} bytes allowed by {pnb}",
    pnr = Range::std(_0.index),
    pnb = Width::std(_0.index),
    r = _0.error.value,
    b = _0.error.bytes,
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::DataLossError))]
pub struct IndexedBitmaskTruncationError(IndexedError<BitmaskTruncationError>);

/// Error when converting $PnR to integer bitmask.
///
/// An error will occur if $PnR either exceeds the number of bytes allowed via
/// $PnB or is a decimal.
#[derive(From, Debug, Error)]
#[error(
    "{k} could not be converted to integer because {e}",
    k = Range::std(_0.index),
    e = _0.error,
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::DataLossError))]
pub struct IndexedRangeToIntError(pub(crate) IndexedError<RangeToIntError<()>>);

/// Error when checking $PnB for float layouts.
///
/// All $PnB should be 32 or 64 depending on $DATATYPE for these layouts.
#[derive(Debug, Display, new)]
#[display(
    "expected {k} to be {expected} but got {width} when determining float type",
    k = Range::std(self.index),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct WrongFloatWidth {
    width: PrivBytes,
    expected: usize,
    index: MeasIndex,
}

/// Any error when computing even width for fixed-width layout
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum EventWidthError {
    Zero(ZeroEventWidth),
    Uneven(UnevenEventWidth),
}

/// Error when fixed-width layout does not evenly divide the length of DATA.
#[derive(Error, Debug, new)]
#[error(
    "Events are {event_width} bytes wide, but this does not evenly divide \
     DATA segment which is {nbytes} bytes long (remainder of {remainder})"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct UnevenEventWidth {
    event_width: u64,
    nbytes: u64,
    remainder: u64,
}

/// Error when fixed layout is empty which precludes computing event number.
#[derive(Error, Debug, new)]
#[error("DATA segment is {event_width} bytes but event width is zero")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct ZeroEventWidth {
    event_width: u64,
}

/// Error when value is truncated when writing DATA with index
#[derive(From, Debug, Error)]
#[error("{e} in column {i}", e = _0.error, i = _0.index)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::DataLossError))]
pub struct IndexedLossError(IndexedError<AnyLossError>);

/// Error when value is truncated when writing DATA
#[derive(From, Display, Debug)]
pub(crate) enum AnyLossError {
    Int(LossError<BitmaskLossError>),
    Float(LossError<Infallible>),
    Ascii(LossError<AsciiLossError>),
}

/// Error when ASCII value is truncated to fewer chars when writing DATA
#[derive(Clone, Copy, Debug, Error)]
#[error("ASCII data truncated to {0} chars")]
pub(crate) struct AsciiLossError(Chars);

type LookupStdLayoutResult<T> = LookupLayoutResult<T, LookupStdLayoutError>;

type LookupRawLayoutResult<T> = LookupLayoutResult<T, LookupRawLayoutError>;

type LookupLayoutResult<T, E> = WarningsAndErrorsResult<T, (), LookupLayoutWarning, E>;

pub type LookupStdLayoutError = LookupLayoutError<LookupStdMeasLayoutError>;
pub type LookupRawLayoutError = LookupLayoutError<LookupRawMeasLayoutError>;

/// Error when looking up layout keywords.
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<pyo3::PyErr>))]
pub enum LookupLayoutError<E> {
    New(#[from] NewDataLayoutError),
    AlphaNumType(#[from] ReqKeyError<AlphaNumType>),
    ByteOrd2_0(#[from] ReqKeyError<ByteOrd2_0>),
    ByteOrd3_1(#[from] ReqKeyError<ByteOrd3_1>),
    Meas(E),
}

/// Warning when looking up layout keywords.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupLayoutWarning {
    New(NewMixedTypeWarning),
    Datatype(DeprecatedDatatypeWarning),
    Meas(OptIndexedKeyError<NumType>),
}

type LookupMeasLayoutResult<T, E> =
    WarningsAndErrorsResult<Vec<ColumnLayoutValues<T>>, (), OptIndexedKeyError<NumType>, E>;

/// Error when looking up measurement layout keywords in standard mode.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupStdMeasLayoutError {
    Width(ReqIndexedKeyError<Width>),
    Range(ReqIndexedKeyError<Range>),
    NumType(OptIndexedKeyError<NumType>),
}

/// Error when looking up measurement layout keywords in raw mode.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupRawMeasLayoutError {
    Par(ReqKeyError<Par>),
    Width(ReqIndexedKeyError<Width>),
    Range(ReqIndexedKeyError<Range>),
    NumType(OptIndexedKeyError<NumType>),
}

/// Error when reading DATA segment
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDataframeError {
    Ascii(ReadAsciiError),
    Width(EventWidthError),
    TotMismatch(TotEventMismatch),
}

/// Warning when reading DATA segment
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDataframeWarning {
    Uneven(UnevenEventWidth),
    Tot(TotEventMismatch),
}

/// Error when reading any ASCII layout (fixed or delimited)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadAsciiError {
    Delim(ReadDelimAsciiError),
    Fixed(ReadFixedAsciiError),
}

/// Error when reading fixed ASCII layout
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadFixedAsciiError {
    Uneven(UnevenEventWidth),
    Tot(TotEventMismatch),
    ToUint(AsciiToUintError),
}

// TODO this is probably redundant
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::EventDataError))]
pub enum AsciiToUintError {
    NotAscii(NotAsciiError),
    Int(ParseIntError),
}

/// Error when encountering characters when parsing DATA as ASCII
#[derive(Debug, Display)]
#[display("bytestring is not valid ASCII: {_0:?}")]
pub struct NotAsciiError(Vec<u8>);

/// Error when $TOT mismatches with number of computed events for DATA.
///
/// This is only applicable to fixed width layouts because their width is used
/// to compute the number of events in DATA.
#[derive(Error, Debug)]
#[error(
    "$TOT field is {tot} but number of events that \
     evenly fit into DATA is {total_events}"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct TotEventMismatch {
    tot: Tot,
    total_events: u64,
}

/// Error when reading delimited ASCII layout (with or without $TOT)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDelimAsciiError {
    Rows(ReadDelimWithRowsAsciiError),
    NoRows(ReadDelimAsciiWithoutRowsError),
}

/// Error when reading delimited ASCII layout with $TOT.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::EventDataError))]
pub enum ReadDelimWithRowsAsciiError {
    RowsExceeded(RowsExceededError),
    Incomplete(DelimIncompleteError),
    Parse(AsciiToUintError),
}

/// Error when reading delimited ASCII layout where DATA is exhausted.
///
/// This happens if $TOT is greater than the true number of values in DATA.
#[derive(Debug, Error)]
#[error("Exceeded expected number of rows: {0}")]
pub struct RowsExceededError(usize);

/// Error when reading delimited ASCII layout where parsing ends unexpectedly.
///
/// This happens if $TOT is less than the true number of values in DATA.
#[derive(Debug, Error)]
#[error(
    "Parsing ended in column {col} and row {row}, \
     where expected number of rows is {nrows}"
)]
pub struct DelimIncompleteError {
    col: usize,
    row: usize,
    nrows: usize,
}

/// Error when reading a delimited ASCII layout without $TOT
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::EventDataError))]
pub enum ReadDelimAsciiWithoutRowsError {
    #[error("{0}")]
    Parse(AsciiToUintError),
    #[error(
        "parsing delimited ASCII without $TOT \
         resulted in columns with unequal length"
    )]
    Unequal,
}

pub(crate) type LayoutConvertResult<L> = ErrorsResult<L, (), LayoutConvertError>;

/// Error when converting between layout versions.
///
/// Some conversions are infallible:
/// * ASCII layouts are interchangeable between any version
/// * all non-mixed 3.2 layouts are interchangeable with 3.1 layouts
/// * all 2.0 layouts are interchangeable with 3.0 layouts
/// * 3.1/3.2 float layouts perfectly downgrade to 2.0/3.0 float layouts
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LayoutConvertError {
    /// Any 2.0/3.0 non-ASCII layout to 3.1/3.2
    OrderToEndian(OrderedToEndianError),
    /// 3.1/3.2 integer layout to 2.0/3.0 integer layout
    Width(UintEndianToOrderedLayoutError),
    /// 3.2 mixed layout to a 2.0/3.0 ordered uint layout
    MixedToOrdered(MixedToOrderedLayoutError),
    /// 3.2 mixed layout to a 3.1/3.2 non-mixed layout.
    MixedToNonMixed(MixedToNonMixedLayoutError),
}

/// Error when converting a 3.1/3.2 int layout to a 2.0/3.0 int layout.
///
/// This arises due to 3.1+ layouts being allowed to support any width and
/// 2.0/3.0 layouts only supporting one width due to the $BYTEORD constraint.
#[derive(From, Debug, Error)]
#[error(
    "{b} and {r} encoding {from}-byte integers are incompatible with {to}-byte integer layout",
    from = _0.error.from,
    to = _0.error.to,
    b = Width::std(_0.index),
    r = Range::std(_0.index),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionException))]
pub struct UintEndianToOrderedLayoutError(IndexedError<UintToUintError>);

/// Error when converting a 3.2 mixed layout to a 3.1/3.2 non-mixed layout.
///
/// This will fail due to type mismatches (A, I, F, or D), since the width for
/// integer layouts is allowed to vary.
#[derive(From, Debug, Error)]
#[error(
    "{b} and {r} when {p}='{from}' are incompatible in layout with $DATATYPE='{to}'",
    from = _0.error.src.as_alpha_num_type(),
    to = _0.error.dest_type,
    p = NumType::std(_0.index),
    b = Width::std(_0.index),
    r = Range::std(_0.index),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionException))]
pub struct MixedToNonMixedLayoutError(IndexedError<MixedToNonMixedError>);

/// Error when converting a 3.2 mixed layout to a 2.0/3.0 ordered uint layout.
///
/// This can fail either because of a type mismatch (ie Float vs Integer) or
/// because the width is incorrect if the mixed layout has integer columns.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MixedToOrderedLayoutError {
    Integer(UintEndianToOrderedLayoutError),
    Other(MixedToNonMixedLayoutError),
}

/// MixedToOrderedLayoutError without the index.
///
/// Used for TryFrom impl's where the index is not known
#[derive(From)]
pub enum MixedToOrderedUintError {
    Integer(UintToUintError),
    Other(MixedToNonMixedError),
}

impl MixedToOrderedUintError {
    fn into_col_error(self, i: MeasIndex) -> MixedToOrderedLayoutError {
        match self {
            Self::Integer(e) => UintEndianToOrderedLayoutError(IndexedError::new(i, e)).into(),
            Self::Other(e) => MixedToNonMixedLayoutError(IndexedError::new(i, e)).into(),
        }
    }
}

/// Error when converting between bitmasks of different byte-widths.
#[derive(Debug, new)]
pub struct UintToUintError {
    from: NonZeroU8,
    to: u8,
}

/// Error when $PnDATATYPE of a column does not match $DATATYPE in a new layout.
#[derive(Debug, new)]
pub struct MixedToNonMixedError {
    dest_type: AlphaNumType,
    src: NullMixedType,
}

/// Error when attempting to insert a new range into a layout.
#[derive(From, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalException))]
pub enum InsertRangeError {
    #[error("could not insert range into ASCII layout because {0}")]
    Ascii(RangeToIntError<()>),
    #[error("could not insert range into integer layout because {0}")]
    Int(RangeToBitmaskError),
    #[error("could not insert range into float layout because {0}")]
    Float(DecimalToFloatError),
}

/// Error when layout and measurement vector do not match.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MeasLayoutMismatchError {
    Lengths(MeasLayoutLengthsError),
    Scale(ScaleMismatchErrors),
    ScaleTransform(ScaleTransformMismatchErrors),
}

/// Error when measurement vector and layout have different lengths.
#[derive(Debug, Error)]
#[error("measurement number ({meas_n}) does not match layout column number ({layout_n})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalException))]
pub struct MeasLayoutLengthsError {
    meas_n: usize,
    layout_n: usize,
}

pub type ScaleMismatchErrors = ErrorGroup<ScaleMismatchError, ScaleMismatchSummary>;

def_group!(
    ScaleMismatchSummary,
    "mismatch between scale and column datatypes"
);

pub type ScaleTransformMismatchErrors =
    ErrorGroup<ScaleTransformMismatchError, ScaleTransformMismatchSummary>;

def_group!(
    ScaleTransformMismatchSummary,
    "mismatch between scale transforms and column datatypes"
);

/// Error when attempting to make a new measurement vector given a layout.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MeasurementsWithLayoutError {
    New(NewNamedVecError),
    Layout(MeasLayoutMismatchError),
}

/// Error when $PnE does not match the datatype in its corresponding column (2.0)
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalException))]
pub struct ScaleMismatchError {
    index: MeasIndex,
    datatype: AlphaNumType,
    scale: Scale,
}

impl fmt::Display for ScaleMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let i = self.index;
        let ekey = Scale::std(i);
        let dt = self.datatype;
        let eval = self.scale;
        write!(
            f,
            "only integer columns may have non-linear scale, \
             column is '{dt}' where {ekey} is '{eval}'"
        )
    }
}

/// Error when $PnE/$PnG do not match the datatype in the corresponding column (3.0+)
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalException))]
pub struct ScaleTransformMismatchError {
    index: MeasIndex,
    datatype: AlphaNumType,
    scale: ScaleTransform,
}

impl fmt::Display for ScaleTransformMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let i = self.index;
        let ekey = Scale::std(i);
        let gkey = Gain::std(i);
        let dt = self.datatype;
        let (eval, g): (Scale, Option<Gain>) = self.scale.into();
        let gval = g.map_or("not set".into(), |s| format!("'{s}'"));
        write!(
            f,
            "only integer columns may have non-unitary scale transforms, \
             column is '{dt}' where {ekey} is '{eval}' and {gkey} is {gval}"
        )
    }
}

#[cfg(feature = "serde")]
pub(crate) fn req_meas_headers() -> [MeasHeader; 2] {
    [Width::std_blank(), Range::std_blank()]
}

#[derive(new, Debug)]
pub(crate) struct IndexedError<E> {
    #[new(into)]
    pub index: IndexFromOne,
    pub error: E,
}

#[cfg(feature = "python")]
mod python {
    use crate::text::float_decimal::{FloatDecimal, HasFloatBounds};
    use crate::text::keywords::AlphaNumType;
    use crate::validated::ascii_range::AsciiRange;

    use super::{AnyNullBitmask, FloatRange, NullMixedType};

    use bigdecimal::BigDecimal;
    use pyo3::conversion::FromPyObjectBound;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;
    use std::fmt;

    impl<'py, T, const LEN: usize> FromPyObject<'py> for FloatRange<T, LEN>
    where
        for<'a> T: FromPyObjectBound<'a, 'py> + HasFloatBounds,
        FloatDecimal<T>: TryFrom<BigDecimal>,
        <FloatDecimal<T> as TryFrom<BigDecimal>>::Error: fmt::Display,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x = ob.extract::<BigDecimal>()?;
            FloatDecimal::try_from(x)
                .map(Self::new)
                // this is a ParseBigDecimalError
                .map_err(|e| PyValueError::new_err(e.to_string()))
        }
    }

    impl<'py, T, const LEN: usize> IntoPyObject<'py> for FloatRange<T, LEN> {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            BigDecimal::from(self.range).into_pyobject(py)
        }
    }

    impl<'py> FromPyObject<'py> for NullMixedType {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (datatype, value): (AlphaNumType, Bound<'py, PyAny>) = ob.extract()?;
            match datatype {
                AlphaNumType::Float => {
                    let x = value.extract::<f32>()?;
                    let y = FloatDecimal::try_from(x)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    Ok(FloatRange::new(y).into())
                }
                AlphaNumType::Double => {
                    let x = value.extract::<f64>()?;
                    let y = FloatDecimal::try_from(x)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    Ok(FloatRange::new(y).into())
                }
                AlphaNumType::Integer => Ok(AnyNullBitmask::from(value.extract::<u64>()?).into()),
                AlphaNumType::Ascii => Ok(AsciiRange::from(value.extract::<u64>()?).into()),
            }
        }
    }

    impl<'py> IntoPyObject<'py> for NullMixedType {
        type Target = PyTuple;
        type Output = Bound<'py, PyTuple>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Ascii(x) => ("A", x.value()).into_pyobject(py),
                Self::Uint(x) => ("I", u64::from(x)).into_pyobject(py),
                Self::F32(x) => ("F", BigDecimal::from(x.range)).into_pyobject(py),
                Self::F64(x) => ("D", BigDecimal::from(x.range)).into_pyobject(py),
            }
        }
    }
}
