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
    AllowTotMismatch, DisallowRangeTrunc, ReadLayoutConfig, ReaderConfig, StdTextReadConfig,
};
use crate::core::{AsScaleTransform, LayoutConvertResult, Measurements, ScaleTransform};
use crate::logging::{
    CmtResultIter as _, DeferredErrors, DeferredFungibleErrors, DeferredIter as _,
    DeferredWarningAndError, DeferredWarningsAndError, ErrorsResult, FungibleErrorResult,
    FungibleErrorsResult, IOResult, IOWarningsAndErrorsResult, ImpureError, LogResult,
    ResultExt as _, Success, WarningOrErrorResult, WarningsAndErrorsResult, WarningsResult,
};
use crate::macros::match_many_to_one;
use crate::nonempty::FCSNonEmpty;
use crate::segment::{
    AnyDataSegment, DataSegmentId, ReqSegmentWithDefaultError, ReqSegmentWithDefaultWarning,
    SegmentMismatchWarning,
};

use crate::text::byteord::{
    BitsOrChars, ByteOrd2_0, ByteOrd3_1, ByteOrdToSizedEndianError, ByteOrdToSizedError, Bytes,
    Endian, HasByteOrd, NoByteOrd, NoByteOrd3_1, OrderedToEndianError, SizedByteOrd, Width,
    WidthToBytesError,
};
use crate::text::float_decimal::{DecimalToFloatError, FloatDecimal, HasFloatBounds};
use crate::text::index::{IndexFromOne, MeasIndex};
use crate::text::keywords::{AlphaNumType, IntRangeError, NumType, Par, Range, Tot};
use crate::text::optional::KeywordPairMaybe as _;
use crate::text::parser::{
    LookupKeysError, LookupKeysWarning, LookupResult, LookupTentative, OptIndexedKey as _,
    OptIndexedKeyError, OptKeyError, ReqIndexedKey as _, ReqIndexedKeyError, ReqKeyError,
    ReqMetarootKey as _,
};

use crate::validated::{
    ascii_range::{AsciiRange, Chars, NewAsciiRangeError},
    bitmask::{
        Bitmask, Bitmask08, Bitmask16, Bitmask24, Bitmask32, Bitmask40, Bitmask48, Bitmask56,
        Bitmask64, BitmaskError, BitmaskLossError,
    },
    dataframe::{
        AllFCSCast, AnyFCSColumn, CastResult, FCSColIter, FCSColumn, FCSDataFrame, FCSDataType,
        LossError,
    },
    keys::{IndexedKey as _, MeasHeader, StdKeywords},
};

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

/// All possible byte layouts for the DATA segment in 2.0.
///
/// This is identical to 3.0 in every way except that the $TOT keyword in 2.0
/// is optional, which requires a different interface.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, T>, generics = "'a, T")]
#[delegate(InterLayoutOps<D>, generics = "D")]
pub struct DataLayout2_0(pub AnyOrderedLayout<MaybeTot>);

/// All possible byte layouts for the DATA segment in 2.0.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, T>, generics = "'a, T")]
#[delegate(InterLayoutOps<D>, generics = "D")]
pub struct DataLayout3_0(pub AnyOrderedLayout<KnownTot>);

/// All possible byte layouts for the DATA segment in 3.1.
///
/// Unlike 2.0 and 3.0, the integer layout allows the column widths to be
/// different. This is a consequence of making BYTEORD only mean "big or little
/// endian" and have nothing to do with number of bytes.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, T>, generics = "'a, T")]
#[delegate(InterLayoutOps<D>, generics = "D")]
pub struct DataLayout3_1(pub NonMixedEndianLayout<NoMeasDatatype>);

/// All possible byte layouts for the DATA segment in 3.2.
///
/// In addition to the loosened integer layouts in 3.1, 3.2 additionally allows
/// each column to have a different type and size (hence "Mixed").
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, T>, generics = "'a, T")]
pub enum DataLayout3_2 {
    Mixed(MixedLayout),
    NonMixed(NonMixedEndianLayout<HasMeasDatatype>),
}

pub type MixedLayout = EndianLayout<NullMixedType, HasMeasDatatype>;

/// All possible byte layouts for the DATA segment in 2.0 and 3.0.
///
/// It is so named "Ordered" because the BYTEORD keyword represents any possible
/// byte ordering that may occur rather than simply little or big endian.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Tot>, generics = "'a, Tot")]
#[delegate(InterLayoutOps<DT>, generics = "DT")]
pub enum AnyOrderedLayout<T> {
    Ascii(AnyAsciiLayout<T, NoMeasDatatype, true>),
    Integer(AnyOrderedUintLayout<T>),
    F32(OrderedLayout<F32Range, T>),
    F64(OrderedLayout<F64Range, T>),
}

// TODO make an integer layout which has only one width, which will cover the
// vast majority of cases and make certain operations easier.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Tot>, generics = "'a, Tot")]
#[delegate(InterLayoutOps<DT>, generics = "DT")]
pub enum NonMixedEndianLayout<D> {
    Ascii(AnyAsciiLayout<KnownTot, D, false>),
    Integer(EndianLayout<AnyNullBitmask, D>),
    F32(EndianLayout<F32Range, D>),
    F64(EndianLayout<F64Range, D>),
}

pub type EndianLayout<C, D> = FixedLayout<C, Endian, KnownTot, D>;

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
    _tot_def: PhantomData<T>,
    _meas_data_def: PhantomData<D>,
}

/// Byte layout where each column has a fixed width.
#[derive(Clone, AsRef, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FixedLayout<C, L, T, D> {
    columns: Vec<C>,
    #[as_ref(L)]
    byte_layout: L,
    _tot_def: PhantomData<T>,
    _meas_data_def: PhantomData<D>,
}

/// Byte layout for integers that may be in any byte order.
#[derive(Clone, From, Delegate, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(LayoutOps<'a, Tot>, generics = "'a, Tot")]
#[delegate(InterLayoutOps<DT>, generics = "DT")]
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

pub type OrderedLayout<C, T> = FixedLayout<C, <C as HasNativeWidth>::Order, T, NoMeasDatatype>;

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
    fn as_err(&self, i: MeasIndex) -> Option<ColumnError<AnyLossError>> {
        self.loss.as_ref().map(|&error| ColumnError::new(i, error))
    }
}

/// Marker type for layouts that might have $TOT
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct MaybeTot;

/// Marker type for layouts that always have $TOT
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct KnownTot;

/// Marker type for layouts without $PnDATATYPE.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct NoMeasDatatype;

/// Marker type for layouts with $PnDATATYPE.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct HasMeasDatatype;

/// Marker type representing absence of column datatype.
pub struct NullMeasDatatype;

/// A struct whose fields map 1-1 with keyword values in one data column
#[derive(new)]
pub struct ColumnLayoutValues<D> {
    width: Width,
    range: Range,
    datatype: D,
}

type ColumnLayoutValues2_0 = ColumnLayoutValues<NullMeasDatatype>;
type ColumnLayoutValues3_2 = ColumnLayoutValues<Option<NumType>>;

pub trait MeasDatatypeDef {
    type MeasDatatype;

    fn lookup_datatype(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self::MeasDatatype>;

    fn lookup_datatype_ro(
        kws: &StdKeywords,
        i: MeasIndex,
    ) -> DeferredWarningAndError<Self::MeasDatatype, OptIndexedKeyError<NumType>, RawParsedError>;

    fn lookup_all(
        kws: &mut StdKeywords,
        par: Par,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Vec<ColumnLayoutValues<Self::MeasDatatype>>> {
        (0..par.0)
            .map(|i| Self::lookup_one(kws, i.into(), conf))
            .mappend_cmt()
    }

    fn lookup_ro_all(
        kws: &StdKeywords,
    ) -> WarningsAndErrorsResult<
        Vec<ColumnLayoutValues<Self::MeasDatatype>>,
        (),
        OptIndexedKeyError<NumType>,
        RawParsedError,
    > {
        Par::get_metaroot_req(kws)
            .map_err(RawParsedError::from)
            .into_log()
            .and_then_cmt(|par| {
                (0..par.0)
                    .map(|i| Self::lookup_one_ro(kws, i.into()))
                    .mappend_cmt()
            })
    }

    // TODO why are width and range being put in lookup result which is
    // used for a totally different type of lookup?
    fn lookup_one(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<ColumnLayoutValues<Self::MeasDatatype>> {
        let w = Width::lookup_req(kws, i);
        let r = Range::lookup_req(kws, i);
        w.zip_cmt(r).and_then_cmt(|(width, range)| {
            Self::lookup_datatype(kws, i, conf)
                .map_ok_value(|datatype| ColumnLayoutValues::new(width, range, datatype))
                .errors_into()
                .set_err_value(())
        })
    }

    fn lookup_one_ro(
        kws: &StdKeywords,
        i: MeasIndex,
    ) -> WarningsAndErrorsResult<
        ColumnLayoutValues<Self::MeasDatatype>,
        (),
        OptIndexedKeyError<NumType>,
        RawParsedError,
    > {
        let w = Width::get_meas_req(kws, i)
            .map_err(RawParsedError::from)
            .into_nowarn();
        let r = Range::get_meas_req(kws, i)
            .map_err(RawParsedError::from)
            .into_nowarn();
        w.zip_cmt(r)
            .nowarn_into_warn()
            .and_then_cmt(|(width, range)| {
                Self::lookup_datatype_ro(kws, i)
                    .repack::<_, _, Vec<_>>()
                    .map_ok_value(|datatype| ColumnLayoutValues::new(width, range, datatype))
                    .map_errors(RawParsedError::from)
                    .set_err_value(())
            })
    }
}

/// Methods for a type which may or may not have $TOT
pub trait TotDefinition {
    type Tot;

    fn with_tot<F, G, I, X>(input: I, tot: Self::Tot, tot_f: F, notot_f: G) -> X
    where
        F: FnOnce(I, Tot) -> X,
        G: FnOnce(I) -> X;

    fn check_tot(
        total_events: u64,
        tot: Self::Tot,
        flag: AllowTotMismatch,
    ) -> FungibleErrorResult<(), (), AllowTotMismatch, TotEventMismatch> {
        Self::with_tot(
            (),
            tot,
            |(), t| Self::check_tot_inner(total_events, t, flag),
            |()| LogResult::new_fungible_ok((), flag),
        )
    }

    #[must_use]
    fn check_tot_inner(
        total_events: u64,
        tot: Tot,
        flag: AllowTotMismatch,
    ) -> FungibleErrorResult<(), (), AllowTotMismatch, TotEventMismatch> {
        let count = usize::try_from(total_events)
            .expect("event count exceeded maximum platform pointer size");
        let i = TotEventMismatch { tot, total_events };
        LogResult::new_fungible_ok_if(tot.0 == count, (), (), i, flag)
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
        tot: <T as TotDefinition>::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IOWarningsAndErrorsResult<FCSDataFrame, (), ReadDataframeWarning, ReadDataframeError>
    where
        T: TotDefinition;

    fn check_writer(&self, df: &'a FCSDataFrame)
    -> ErrorsResult<(), (), ColumnError<AnyLossError>>;

    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &'a FCSDataFrame,
        skip_conv_check: bool,
    ) -> DeferredWarningsAndError<(), ColumnError<AnyLossError>, io::Error>;

    fn check_transforms_and_len(
        &self,
        xforms: &[ScaleTransform],
    ) -> DeferredErrors<(), MeasLayoutMismatchError> {
        let meas_n = xforms.len();
        let layout_n = self.ncols();
        if meas_n != layout_n {
            let e = MeasLayoutLengthsError { meas_n, layout_n };
            return LogResult::new_err1(e.into());
        }
        self.check_transforms(xforms)
            .map_errors(MeasLayoutMismatchError::from)
    }

    // TODO this should be private
    fn check_transforms(
        &self,
        xforms: &[ScaleTransform],
    ) -> DeferredErrors<(), ColumnError<ScaleMismatchTransformError>> {
        // ASSUME measurements and layout columns are the same length
        self.datatypes()
            .iter()
            .zip(xforms)
            .enumerate()
            .map(|(i, (&datatype, &scale))| {
                // Only integers are allowed to have gain and log scaling, so
                // everything else should be a "noop" transform (ie a linear
                // transform with slope of 1.0). NOTE the standard itself is
                // vague about what should happen to ASCII values (presumably
                // since nobody cares) so here we just treat them like we treat
                // floating point types to keep the logic simple.
                let is_ok = datatype == AlphaNumType::Integer || scale.is_noop();
                let e = ScaleMismatchTransformError { datatype, scale };
                LogResult::new_log_if(is_ok, (), (), ColumnError::new(i, e))
            })
            .mappend_def()
            .map_def_value(|_| ())
    }

    fn truncate_df(
        &self,
        df: &'a FCSDataFrame,
        skip_conv_check: bool,
    ) -> WarningsResult<FCSDataFrame, ColumnError<AnyLossError>>;
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
    ) -> FungibleErrorsResult<(), (), DisallowRangeTrunc, AnyRangeError>;

    fn push(
        &mut self,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> FungibleErrorsResult<(), (), DisallowRangeTrunc, AnyRangeError>;

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
    for<'a> Self: Sized + LayoutOps<'a, Self::TotDef> + InterLayoutOps<Self::MeasDTDef>,
{
    type ByteLayout;
    type MeasDTDef: MeasDatatypeDef;
    type TotDef: TotDefinition;

    fn lookup<C>(kws: &mut StdKeywords, conf: &C, par: Par) -> LookupLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>;

    fn lookup_ro(kws: &StdKeywords, conf: &ReadLayoutConfig) -> FromRawResult<Self>;

    fn new_empty(datatype: AlphaNumType) -> Self;

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<<Self::MeasDTDef as MeasDatatypeDef>::MeasDatatype>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), ColumnError<NewMixedTypeWarning>, NewDataLayoutError>;

    fn h_read_df<R: Read + Seek>(
        &self,
        h: &mut BufReader<R>,
        tot: <Self::TotDef as TotDefinition>::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IOWarningsAndErrorsResult<FCSDataFrame, (), ReadDataframeWarning, ReadDataframeError> {
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
                    .into_io_log()
                    .and_then_nowarn_with_warn(|_| {
                        self.h_read_df_inner(h, &mut buf, tot, seg, conf)
                    })
            },
        )
    }

    fn h_write_df<W>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        skip_conv_check: bool,
    ) -> DeferredWarningsAndError<(), ColumnError<AnyLossError>, io::Error>
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
    ) -> DeferredErrors<(), MeasLayoutMismatchError> {
        let xforms: Vec<_> = meas
            .iter_with(&|_, _| ScaleTransform::default(), &|_, m| {
                m.value.as_transform()
            })
            .collect();
        self.check_transforms_and_len(&xforms[..])
    }
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
    ) -> DeferredFungibleErrors<Self, DisallowRangeTrunc, Self::Error>;
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

    fn as_err(&self, i: MeasIndex) -> Option<ColumnError<AnyLossError>>;
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
                    Err(UintToUintError {
                        from: w,
                        to: Self::BYTES.into(),
                    })
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
                        Err(UintToUintError {
                            from: w,
                            to: Self::BYTES.into(),
                        }
                        .into())
                    }
                } else {
                    let dest_type = value.as_alpha_num_type();
                    Err(MixedToInnerError::new(dest_type, value).into())
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

impl MeasDatatypeDef for NoMeasDatatype {
    type MeasDatatype = NullMeasDatatype;

    fn lookup_datatype(
        _: &mut StdKeywords,
        _: MeasIndex,
        _: &StdTextReadConfig,
    ) -> LookupTentative<Self::MeasDatatype> {
        LogResult::new_ok(NullMeasDatatype)
    }

    fn lookup_datatype_ro(
        _: &StdKeywords,
        _: MeasIndex,
    ) -> DeferredWarningAndError<Self::MeasDatatype, OptIndexedKeyError<NumType>, RawParsedError>
    {
        LogResult::new_ok(NullMeasDatatype)
    }
}

impl MeasDatatypeDef for HasMeasDatatype {
    type MeasDatatype = Option<NumType>;

    fn lookup_datatype(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self::MeasDatatype> {
        NumType::lookup_meas_opt(kws, i, false, conf)
    }

    // TODO what is this actually doing?
    fn lookup_datatype_ro(
        kws: &StdKeywords,
        i: MeasIndex,
    ) -> DeferredWarningAndError<Self::MeasDatatype, OptIndexedKeyError<NumType>, RawParsedError>
    {
        NumType::get_meas_opt(kws, i).into_succ()
    }
}

impl TotDefinition for MaybeTot {
    type Tot = Option<Tot>;

    fn with_tot<F, G, I, X>(input: I, tot: Self::Tot, tot_f: F, notot_f: G) -> X
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

impl TotDefinition for KnownTot {
    type Tot = Tot;

    fn with_tot<F, G, I, X>(input: I, tot: Self::Tot, tot_f: F, _: G) -> X
    where
        F: FnOnce(I, Tot) -> X,
        G: FnOnce(I) -> X,
    {
        tot_f(input, tot)
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
            type Error = MixedToInnerError;
            fn try_from(value: NullMixedType) -> Result<Self, Self::Error> {
                let dest_type = value.as_alpha_num_type();
                if let MixedType::$var(x) = value {
                    Ok(x)
                } else {
                    Err(MixedToInnerError::new(dest_type, value))
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

    fn as_err(&self, i: MeasIndex) -> Option<ColumnError<AnyLossError>> {
        self.as_err(i)
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

    fn as_err(&self, i: MeasIndex) -> Option<ColumnError<AnyLossError>> {
        match_any_mixed!(self, x, { x.as_err(i) })
    }
}

impl<'a> Writable<'a, Endian> for AnyWriterBitmask<'a> {
    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: Endian) -> io::Result<()> {
        match_any_uint!(self, Self, c, { c.h_write(h, byte_layout) })
    }

    fn truncate(self, skip_conv_check: bool) -> (AnyFCSColumn, Option<AnyLossError>) {
        match_any_uint!(self, Self, x, { x.truncate(skip_conv_check) })
    }

    fn as_err(&self, i: MeasIndex) -> Option<ColumnError<AnyLossError>> {
        match_any_uint!(self, Self, x, { x.as_err(i) })
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

// #[cfg(feature = "serde")]
// impl<C: Serialize, L: Serialize, T, D> Serialize for FixedLayout<C, L, T, D> {
//     fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
//         let mut state = serializer.serialize_struct("FixedLayout", 2)?;
//         state.serialize_field("columns", Vec::from(self.columns.as_ref()).as_slice())?;
//         state.serialize_field("byte_layout", &self.byte_layout)?;
//         state.end()
//     }
// }

// #[cfg(feature = "serde")]
// impl<T, D, const ORD: bool> Serialize for DelimAsciiLayout<T, D, ORD> {
//     fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
//         let mut state = serializer.serialize_struct("DelimitedLayout", 1)?;
//         state.serialize_field("ranges", Vec::from(self.ranges.as_ref()).as_slice())?;
//         state.end()
//     }
// }

impl<D> EndianLayout<AnyNullBitmask, D> {
    pub(crate) fn endian_uint_try_new(
        cs: Vec<ColumnLayoutValues<D::MeasDatatype>>,
        e: Endian,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<Self, (), ColumnError<BitmaskError>, ColumnError<NewUintTypeError>>
    where
        D: MeasDatatypeDef,
    {
        Self::try_new(cs, e, |c| {
            AnyBitmask::from_width_and_range(c.width, c.range, flag)
        })
    }

    pub(crate) fn uint_try_into_ordered<T>(self) -> LayoutConvertResult<AnyOrderedUintLayout<T>> {
        if let Some(cs) = NonEmpty::from_vec(self.columns) {
            cs.head
                .try_into_one_size(cs.tail, self.byte_layout, 1)
                .map_errors(|(index, error)| ConvertWidthError { index, error })
                .errors_into()
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
                            .map_err(|e| MixedColumnConvertError::new(i + 1, e))
                            .into_log()
                    })
                    .mappend_cmt()
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
                    .map_errors(|(index, error)| MixedColumnConvertError::new(index, error)),
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
    ) -> ErrorsResult<NonMixedEndianLayout<NoMeasDatatype>, (), MixedToNonMixedLayoutError> {
        if let Some(ne_cols) = NonEmpty::from_vec(self.columns) {
            macro_rules! from_iter {
                ($iter:expr, $head:expr, $byte_layout:expr) => {
                    $iter
                        .map(|(i, c)| c.try_into().map_err(|e| (i, e)).into_log())
                        .mappend_cmt()
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
                    .mappend_cmt()
                    .map_ok_value(|xs| FixedLayout::new1(x, xs, NoByteOrd))
                    .map_ok_value(|l| AnyAsciiLayout::Fixed(l).into()),
                MixedType::Uint(x) => from_iter!(it, x, byte_layout),
                MixedType::F32(x) => from_iter!(it, x, byte_layout),
                MixedType::F64(x) => from_iter!(it, x, byte_layout),
            }
            .map_errors(|(i, error)| MixedColumnConvertError::new(i + 1, error))
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
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<Self, (), DecimalToFloatError, FloatWidthError>
    where
        FloatDecimal<T>: TryFrom<BigDecimal, Error = DecimalToFloatError>,
        T: HasFloatBounds,
    {
        Bytes::try_from(width)
            .into_log::<_, _, Vec<_>>()
            .map_errors(FloatWidthError::from)
            .and_then_cmt(|bytes| {
                if usize::from(u8::from(bytes)) == LEN {
                    Self::from_range(range, flag)
                        .set_err_value(())
                        .fungible_into_commutative()
                        .map_errors(FloatWidthError::from)
                } else {
                    let e = FloatWidthError::from(WrongFloatWidth::new(bytes, LEN));
                    LogResult::new_err1(e)
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
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<Self, (), NewMixedTypeWarning, NewMixedTypeError> {
        macro_rules! from {
            ($t:ident, $width:expr, $range:expr, $flag:expr) => {
                $t::from_width_and_range($width, $range, $flag)
                    .map_ok_value(Self::from)
                    .cmt_warnings_into()
                    .errors_into()
            };
        }

        if let Some(dt) = datatype {
            match dt {
                NumType::Integer => from!(AnyBitmask, width, range, flag),
                NumType::Float => from!(F32Range, width, range, flag),
                NumType::Double => from!(F64Range, width, range, flag),
            }
        } else {
            from!(AsciiRange, width, range, flag)
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
        match Bytes::from_u64(value) {
            Bytes::B1 => Self::Uint08(Bitmask::from_u64(value).0),
            Bytes::B2 => Self::Uint16(Bitmask::from_u64(value).0),
            Bytes::B3 => Self::Uint24(Bitmask::from_u64(value).0),
            Bytes::B4 => Self::Uint32(Bitmask::from_u64(value).0),
            Bytes::B5 => Self::Uint40(Bitmask::from_u64(value).0),
            Bytes::B6 => Self::Uint48(Bitmask::from_u64(value).0),
            Bytes::B7 => Self::Uint56(Bitmask::from_u64(value).0),
            Bytes::B8 => Self::Uint64(Bitmask::from_u64(value).0),
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
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<Self, (), BitmaskError, NewUintTypeError> {
        width
            .try_into()
            .into_log::<_, _, Vec<_>>()
            .map_errors(NewUintTypeError::from)
            .and_then_cmt(|bytes| {
                Self::new1(bytes, range, flag)
                    .set_err_value(())
                    .fungible_into_commutative()
                    .map_errors(NewUintTypeError::from)
            })
    }

    /// Make a new bitmask with a given width (in bytes) using a float/int.
    fn new1(
        width: Bytes,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredFungibleErrors<Self, DisallowRangeTrunc, BitmaskError> {
        match width {
            Bytes::B1 => Bitmask08::from_range(range, flag).map_def_value(Into::into),
            Bytes::B2 => Bitmask16::from_range(range, flag).map_def_value(Into::into),
            Bytes::B3 => Bitmask24::from_range(range, flag).map_def_value(Into::into),
            Bytes::B4 => Bitmask32::from_range(range, flag).map_def_value(Into::into),
            Bytes::B5 => Bitmask40::from_range(range, flag).map_def_value(Into::into),
            Bytes::B6 => Bitmask48::from_range(range, flag).map_def_value(Into::into),
            Bytes::B7 => Bitmask56::from_range(range, flag).map_def_value(Into::into),
            Bytes::B8 => Bitmask64::from_range(range, flag).map_def_value(Into::into),
        }
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
        Self::new(value.width, value.range, NullMeasDatatype)
    }
}

impl<T, D, const ORD: bool> LayoutOps<'_, T> for DelimAsciiLayout<T, D, ORD>
where
    T: TotDefinition,
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
        tot: T::Tot,
        seg: AnyDataSegment,
        _: &ReaderConfig,
    ) -> IOWarningsAndErrorsResult<FCSDataFrame, (), ReadDataframeWarning, ReadDataframeError> {
        let rs = &self.ranges;
        let nbytes = usize::try_from(seg.len()).expect("DATA length > platform pntr size");
        let res = T::with_tot(
            h,
            tot,
            |h_, t| h_read_delim_with_rows(rs, h_, t, nbytes).map_err(ImpureError::inner_into),
            |h_| h_read_delim_without_rows(rs, h_, nbytes).map_err(ImpureError::inner_into),
        );
        res.into_log()
    }

    fn check_writer(&self, df: &FCSDataFrame) -> ErrorsResult<(), (), ColumnError<AnyLossError>> {
        df.iter_columns()
            .enumerate()
            .map(|(i, c)| {
                c.check_writer::<_, _, u64>(|_| None)
                    .map_err(|error| ColumnError::new(i, AnyLossError::Int(error)))
                    .into_log()
            })
            .mappend_def_void()
    }

    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        skip_conv_check: bool,
    ) -> DeferredWarningsAndError<(), ColumnError<AnyLossError>, io::Error> {
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
                        .map(|w| ColumnError::new(i, w))
                })
                .collect();
            write_res.set_commutative_warnings(cs)
        }
    }

    fn truncate_df(
        &self,
        df: &FCSDataFrame,
        skip_conv_check: bool,
    ) -> WarningsResult<FCSDataFrame, ColumnError<AnyLossError>> {
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
                    w.map(|x| ColumnError::new(i, AnyLossError::Ascii(LossError::Cast(x)))),
                )
            })
            .unzip();
        let ws: Vec<_> = warnings.into_iter().flatten().collect();
        let ret = FCSDataFrame::try_new(columns).unwrap();
        Success::new_non_fungible(ret).set_warnings(ws)
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
    ) -> FungibleErrorsResult<(), (), DisallowRangeTrunc, AnyRangeError> {
        range
            .into_uint()
            .nowarn_into_fungible(flag)
            .map_fungible_errors(AnyRangeError::from)
            .map_ok_value(|r| self.ranges.insert(index.into(), r))
            .set_err_value(())
            .repack()
    }

    fn push(
        &mut self,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> FungibleErrorsResult<(), (), DisallowRangeTrunc, AnyRangeError> {
        range
            .into_uint()
            .nowarn_into_fungible(flag)
            .map_fungible_errors(AnyRangeError::from)
            .map_ok_value(|r| self.ranges.push(r))
            .set_err_value(())
            .repack()
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
    D: MeasDatatypeDef,
    T: TotDefinition,
    C: Clone + IsFixed + HasDatatype + IntoReader<S> + IntoWriter<'a, S> + FromRange,
    S: Copy + HasByteOrd,
    S::ByteOrd: fmt::Display,
    for<'c> Range: From<&'c C>,
    <C as IntoReader<S>>::Target: Readable<S>,
    <C as IntoWriter<'a, S>>::Target: Writable<'a, S>,
    AnyRangeError: From<<C as FromRange>::Error>,
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
        tot: T::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IOWarningsAndErrorsResult<FCSDataFrame, (), ReadDataframeWarning, ReadDataframeError>
    where
        T: TotDefinition,
    {
        self.compute_nrows(seg, conf)
            .map_non_cmt_warnings(ReadDataframeWarning::from)
            .map_errors(ReadDataframeError::from)
            .map_errors(ImpureError::Pure)
            .non_cmt_into_cmt()
            .repack()
            .and_then_cmt(|n| {
                let check_res = T::check_tot(n, tot, conf.allow_tot_mismatch)
                    .fungible_into_commutative()
                    .map_commutative_warnings(ReadDataframeWarning::from)
                    .map_errors(ReadDataframeError::from)
                    .map_errors(ImpureError::Pure)
                    .repack::<_, _, Vec<_>>();
                let nn = usize::try_from(n).expect("nrows exceeds usize");
                let read_res = self.h_read_unchecked_df(h, nn, buf).into_log();
                check_res.zip_cmt(read_res).map_ok_value(|((), df)| df)
            })
    }

    fn check_writer(
        &self,
        df: &'a FCSDataFrame,
    ) -> ErrorsResult<(), (), ColumnError<AnyLossError>> {
        // ASSUME df has same number of columns as layout
        self.columns
            .iter()
            .zip(df.iter_columns())
            .enumerate()
            .map(|(i, (col_type, col_data))| {
                col_type
                    .check_writer(col_data)
                    .map_err(|error| ColumnError::new(i, error))
                    .into_log()
            })
            .mappend_def_void()
    }

    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &'a FCSDataFrame,
        skip_conv_check: bool,
    ) -> DeferredWarningsAndError<(), ColumnError<AnyLossError>, io::Error> {
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
                .iter()
                .enumerate()
                .filter_map(|(i, c)| c.as_err(i.into()))
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
    ) -> WarningsResult<FCSDataFrame, ColumnError<AnyLossError>> {
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
            .filter_map(|(i, e)| e.map(|f| ColumnError::new(i, f)))
            .collect();
        let ret = FCSDataFrame::try_new(new_columns).unwrap();
        Success::new_non_fungible(ret).set_warnings(ws)
    }
}

impl<'a, C, S, T, D> InterLayoutOps<D> for FixedLayout<C, S, T, D>
where
    T: TotDefinition,
    C: Clone + IsFixed + HasDatatype + IntoReader<S> + IntoWriter<'a, S> + FromRange,
    S: Copy + HasByteOrd,
    for<'c> Range: From<&'c C>,
    <C as IntoReader<S>>::Target: Readable<S>,
    <C as IntoWriter<'a, S>>::Target: Writable<'a, S>,
    AnyRangeError: From<<C as FromRange>::Error>,
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
    ) -> FungibleErrorsResult<(), (), DisallowRangeTrunc, AnyRangeError> {
        C::from_range(range, flag)
            .map_fungible_errors(AnyRangeError::from)
            .map_ok_value(|col| self.insert_column(index, col))
            .set_err_value(())
    }

    fn push(
        &mut self,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> FungibleErrorsResult<(), (), DisallowRangeTrunc, AnyRangeError> {
        C::from_range(range, flag)
            .map_fungible_errors(AnyRangeError::from)
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
        cs: Vec<ColumnLayoutValues<D::MeasDatatype>>,
        byte_layout: S,
        new_col_f: F,
    ) -> WarningsAndErrorsResult<Self, (), ColumnError<W>, ColumnError<E>>
    where
        D: MeasDatatypeDef,
        F: Fn(ColumnLayoutValues<D::MeasDatatype>) -> WarningsAndErrorsResult<C, P, W, E>,
    {
        cs.into_iter()
            .enumerate()
            .map(|(i, c)| {
                new_col_f(c)
                    .map_errors(|error| ColumnError::new(i, error))
                    .map_commutative_warnings(|error| ColumnError::new(i, error))
            })
            .mappend_cmt()
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
            LogResult::new_err1(EventWidthError::from(ZeroEventWidth::new(n)))
        } else {
            let total_events = n / w;
            let remainder = n % w;
            let is_ok = remainder == 0;
            let e = UnevenEventWidth::new(w, n, remainder);
            let flag = conf.allow_uneven_event_width;
            FungibleErrorResult::new_fungible_ok_if(is_ok, total_events, (), e, flag)
                .fungible_into_non_commutative()
                .map_errors(EventWidthError::from)
        }
    }
}

impl<C> EndianLayout<C, HasMeasDatatype> {
    fn insert_mixed(
        mut self,
        index: MeasIndex,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredFungibleErrors<DataLayout3_2, DisallowRangeTrunc, AnyRangeError>
    where
        C: TryFrom<NullMixedType, Error = MixedToInnerError>,
        NullMixedType: From<C>,
        NonMixedEndianLayout<HasMeasDatatype>: From<Self>,
    {
        NullMixedType::from_range(range, flag).map_def_value(|col| match col.try_into() {
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
    ) -> DeferredFungibleErrors<DataLayout3_2, DisallowRangeTrunc, AnyRangeError>
    where
        C: TryFrom<NullMixedType, Error = MixedToInnerError>,
        NullMixedType: From<C>,
        NonMixedEndianLayout<HasMeasDatatype>: From<Self>,
    {
        NullMixedType::from_range(range, flag).map_def_value(|col| match col.try_into() {
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
            const BYTES: Bytes = Bytes::$bytes;
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
    T: TryFrom<Range, Error = IntRangeError<T>> + PrimInt,
    u64: From<T>,
{
    type Error = BitmaskError;

    fn from_range(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredFungibleErrors<Self, DisallowRangeTrunc, Self::Error> {
        // TODO there is probably a better place to do this subtraction
        (range - Range::from(1_u8))
            .into_uint()
            .map_errors(BitmaskError::from)
            .repack_errors::<Vec<_>>()
            .and_then_nowarn(|x| {
                Self::try_from_native(x)
                    .map_errors(BitmaskError::from)
                    .repack_errors::<Vec<_>>()
            })
            .nowarn_into_fungible(flag)
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
    ) -> DeferredFungibleErrors<Self, DisallowRangeTrunc, Self::Error> {
        range
            .into_float()
            .map_def_value(Self::new)
            .nowarn_into_fungible(flag)
            .repack()
    }
}

impl FromRange for AsciiRange {
    type Error = IntRangeError<()>;

    /// Make new AsciiRange from a float or integer.
    ///
    /// The number of chars will be automatically selected as the minimum
    /// required to express the range.
    fn from_range(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredFungibleErrors<Self, DisallowRangeTrunc, Self::Error> {
        range
            .into_uint::<u64>()
            .map_def_value(Self::from)
            .nowarn_into_fungible(flag)
            .repack()
    }
}

impl FromRange for AnyNullBitmask {
    type Error = IntRangeError<()>;

    /// make a new bitmask from a float or integer.
    ///
    /// The size will be determined by the input and will be kept as small as
    /// possible.
    fn from_range(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredFungibleErrors<Self, DisallowRangeTrunc, Self::Error> {
        // TODO there is probably a better place to do this subtraction
        (range - Range::from(1_u8))
            .into_uint()
            .map_def_value(|x: u64| Self::from(x))
            .nowarn_into_fungible(flag)
            .repack()
    }
}

impl FromRange for NullMixedType {
    type Error = AnyRangeError;

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
    ) -> DeferredFungibleErrors<Self, DisallowRangeTrunc, Self::Error> {
        if range.0.is_integer() {
            AnyBitmask::from_range(range, flag)
                .map_def_value(Self::Uint)
                .map_fungible_errors(AnyRangeError::from)
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
                        FungibleErrorsResult::new_deferred_fungible(f, e, flag)
                            .map_fungible_errors(AnyRangeError::from)
                    },
                    |x| FungibleErrorsResult::new_fungible_ok(x, flag),
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
        Self::BYTES.into()
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
        Self::BYTES.into()
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
        self.chars().into()
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
        cs: Vec<ColumnLayoutValues<NullMeasDatatype>>,
        bo: ByteOrd2_0,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), ColumnError<BitmaskError>, NewFixedIntLayoutError> {
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
                .map(Bytes::try_from)
                .map(Result::into_log::<_, _, Vec<_>>)
                .mappend_cmt()
                .map_errors(SingleFixedWidthError::from)
                .and_then_cmt(|widths| {
                    let ws = widths.into_iter().filter(|&w| w != n);
                    if let Some(mismatches) = NonEmpty::collect(ws) {
                        let e = WidthMismatchError::new(real_bo, mismatches);
                        LogResult::new_err1(SingleFixedWidthError::from(e))
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
                FixedLayout::try_new(cs, o, |c| {
                    Bitmask::from_range(c.range, notrunc)
                        .fungible_into_commutative()
                        .map_errors(IntOrderedColumnError::from)
                })
                .set_err_value(())
                .map_errors(NewFixedIntLayoutError::from)
                .map_ok_value(Self::from)
            });

        width_res
            .nowarn_into_warn()
            .map_errors(NewFixedIntLayoutError::from)
            .zip_cmt(layout_res)
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
        cs: Vec<ColumnLayoutValues<D::MeasDatatype>>,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<
        Self,
        (),
        ColumnError<IntRangeError<()>>,
        ColumnError<NewAsciiRangeError>,
    >
    where
        D: MeasDatatypeDef,
    {
        if cs.iter().all(|c| c.width == Width::Variable) {
            cs.into_iter()
                .enumerate()
                .map(|(i, c)| {
                    c.range
                        .into_uint::<u64>()
                        .nowarn_into_fungible(flag)
                        .fungible_into_commutative()
                        .map_commutative_warnings(|e| ColumnError::new(i, e))
                        .map_errors(NewAsciiRangeError::from)
                        .map_errors(|e| ColumnError::new(i, e))
                        .repack()
                })
                .mappend_def()
                .map_ok_value(|ranges| DelimAsciiLayout::new(ranges).into())
                .map_err_value(|_| ())
        } else {
            FixedLayout::try_new(cs, NoByteOrd, |c| {
                AsciiRange::from_width_and_range(c.width, c.range, flag)
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

impl<T, const LEN: usize, Tot> OrderedLayout<Bitmask<T, LEN>, Tot>
where
    Bitmask<T, LEN>: HasNativeWidth<Order = SizedByteOrd<LEN>>,
{
    #[must_use]
    pub fn new_endian_uint(ranges: Vec<Bitmask<T, LEN>>, endian: Endian) -> Self {
        Self::new(ranges, SizedByteOrd::Endian(endian))
    }
}

impl<T, const LEN: usize, Tot> OrderedLayout<FloatRange<T, LEN>, Tot>
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
    type MeasDTDef = NoMeasDatatype;
    type TotDef = MaybeTot;

    fn lookup<C>(kws: &mut StdKeywords, conf: &C, par: Par) -> LookupLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        AnyOrderedLayout::lookup(kws, conf, par).map_ok_value(Self::from)
    }

    fn lookup_ro(kws: &StdKeywords, conf: &ReadLayoutConfig) -> FromRawResult<Self> {
        AnyOrderedLayout::lookup_ro(kws, conf).map_ok_value(Self::from)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        AnyOrderedLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<<Self::MeasDTDef as MeasDatatypeDef>::MeasDatatype>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), ColumnError<NewMixedTypeWarning>, NewDataLayoutError>
    {
        AnyOrderedLayout::try_new(datatype, byteord, columns, conf)
            .map_ok_value(Self::from)
            .map_commutative_warnings(ColumnError::inner_into)
    }
}

impl VersionedDataLayout for DataLayout3_0 {
    type ByteLayout = ByteOrd2_0;
    type MeasDTDef = NoMeasDatatype;
    type TotDef = KnownTot;

    fn lookup<C>(kws: &mut StdKeywords, conf: &C, par: Par) -> LookupLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        AnyOrderedLayout::lookup(kws, conf, par).map_ok_value(Into::into)
    }

    fn lookup_ro(kws: &StdKeywords, conf: &ReadLayoutConfig) -> FromRawResult<Self> {
        AnyOrderedLayout::lookup_ro(kws, conf).map_ok_value(Self::from)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        AnyOrderedLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<NullMeasDatatype>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), ColumnError<NewMixedTypeWarning>, NewDataLayoutError>
    {
        AnyOrderedLayout::try_new(datatype, byteord, columns, conf)
            .map_ok_value(Self::from)
            .map_commutative_warnings(ColumnError::inner_into)
    }
}

impl VersionedDataLayout for DataLayout3_1 {
    type ByteLayout = Endian;
    type MeasDTDef = NoMeasDatatype;
    type TotDef = KnownTot;

    fn lookup<C>(kws: &mut StdKeywords, conf: &C, par: Par) -> LookupLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        NonMixedEndianLayout::lookup(kws, conf, par).map_ok_value(Self::from)
    }

    fn lookup_ro(kws: &StdKeywords, conf: &ReadLayoutConfig) -> FromRawResult<Self> {
        NonMixedEndianLayout::lookup_ro(kws, conf).map_ok_value(Self::from)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        NonMixedEndianLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<NullMeasDatatype>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), ColumnError<NewMixedTypeWarning>, NewDataLayoutError>
    {
        NonMixedEndianLayout::try_new(datatype, byteord, columns, conf)
            .map_ok_value(Into::into)
            .map_commutative_warnings(ColumnError::inner_into)
    }
}

impl VersionedDataLayout for DataLayout3_2 {
    type ByteLayout = ByteOrd3_1;
    type MeasDTDef = HasMeasDatatype;
    type TotDef = KnownTot;

    fn lookup<C>(kws: &mut StdKeywords, conf: &C, par: Par) -> LookupLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        macro_rules! from {
            ($i:expr) => {
                $i.map_commutative_warnings(LookupLayoutWarning::from)
                    .map_errors(LookupLayoutError::from)
            };
        }

        let d = AlphaNumType::lookup_req_check_ascii(kws);
        let e = ByteOrd3_1::lookup_req(kws);
        let cs = HasMeasDatatype::lookup_all(kws, par, conf.as_ref());

        from!(d.zip3_cmt(e, cs)).and_then_cmt(|(datatype, endian, columns)| {
            from!(Self::try_new(datatype, endian, columns, conf.as_ref()))
        })
    }

    fn lookup_ro(kws: &StdKeywords, conf: &ReadLayoutConfig) -> FromRawResult<Self> {
        macro_rules! from1 {
            ($i:expr) => {
                $i.map_err(RawParsedError::from).into_log()
            };
        }

        macro_rules! from2 {
            ($i:expr) => {
                $i.map_commutative_warnings(RawToLayoutWarning::from)
                    .map_errors(RawToLayoutError::from)
            };
        }

        let d = from1!(AlphaNumType::get_metaroot_req(kws));
        let e = from1!(ByteOrd3_1::get_metaroot_req(kws));
        let cs = HasMeasDatatype::lookup_ro_all(kws);

        from2!(d.zip3_cmt(e, cs)).and_then_cmt(|(datatype, endian, columns)| {
            from2!(Self::try_new(datatype, endian, columns, conf))
        })
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        NonMixedEndianLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Option<NumType>>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), ColumnError<NewMixedTypeWarning>, NewDataLayoutError>
    {
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
                    .map(|c| ColumnLayoutValues::new(c.width, c.range, NullMeasDatatype))
                    .collect();
                NonMixedEndianLayout::try_new(dt, byteord.0, ds, conf)
                    .map_ok_value(|x| Self::NonMixed(x.phantom_into::<HasMeasDatatype>()))
                    .map_commutative_warnings(ColumnError::inner_into)
            }
            // has columns with 1+ datatypes, use mixed layout
            _ => {
                let go = |c: ColumnLayoutValues3_2| {
                    MixedType::from_width_and_range(c.width, c.range, c.datatype, notrunc)
                };
                FixedLayout::try_new(columns, byteord.0, go)
                    .map_errors(NewDataLayoutError::from)
                    .map_ok_value(Self::from)
            }
        }
    }
}

impl InterLayoutOps<HasMeasDatatype> for DataLayout3_2 {
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
    ) -> FungibleErrorsResult<(), (), DisallowRangeTrunc, AnyRangeError> {
        match mem::replace(self, Self::mixed_dummy()) {
            // If layout is mixed, interpret range as a mixed type
            Self::Mixed(mut x) => x
                .insert_nocheck(index, range, flag)
                .set_def_value(Self::Mixed(x)),
            // If layout is non-mixed, interpret range as an ASCII range and
            // keep the layout as ASCII. Otherwise, interpret as a mixed range
            // and convert the layout to a mixed layout if the interpreted
            // result is different from the rest of the types in the layout.
            Self::NonMixed(x) => match x {
                NonMixedEndianLayout::Ascii(mut y) => y
                    .insert_nocheck(index, range, flag)
                    .set_def_value(Self::NonMixed(y.into())),
                NonMixedEndianLayout::Integer(y) => y.insert_mixed(index, range, flag),
                NonMixedEndianLayout::F32(y) => y.insert_mixed(index, range, flag),
                NonMixedEndianLayout::F64(y) => y.insert_mixed(index, range, flag),
            },
        }
        .map_def_value(|newself| {
            *self = newself;
        })
    }

    fn push(
        &mut self,
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredFungibleErrors<(), DisallowRangeTrunc, AnyRangeError> {
        match mem::replace(self, Self::mixed_dummy()) {
            Self::Mixed(mut x) => x.push(range, flag).set_def_value(Self::Mixed(x)),
            Self::NonMixed(x) => match x {
                NonMixedEndianLayout::Ascii(mut y) => {
                    y.push(range, flag).set_def_value(Self::NonMixed(y.into()))
                }
                NonMixedEndianLayout::Integer(y) => y.push_mixed(range, flag),
                NonMixedEndianLayout::F32(y) => y.push_mixed(range, flag),
                NonMixedEndianLayout::F64(y) => y.push_mixed(range, flag),
            },
        }
        .map_def_value(|newself| {
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
            Self::Mixed(x) => x.try_into_ordered().errors_into(),
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
}

impl<T> Default for AnyOrderedLayout<T> {
    fn default() -> Self {
        Self::Integer(AnyOrderedUintLayout::default())
    }
}

impl<T> AnyOrderedLayout<T> {
    fn lookup<C>(kws: &mut StdKeywords, conf: &C, par: Par) -> LookupLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        macro_rules! from {
            ($i:expr) => {
                $i.map_commutative_warnings(LookupLayoutWarning::from)
                    .map_errors(LookupLayoutError::from)
            };
        }

        let cs = NoMeasDatatype::lookup_all(kws, par, conf.as_ref());
        let d = AlphaNumType::lookup_req(kws);
        let b = ByteOrd2_0::lookup_req(kws);

        from!(d.zip3_cmt(b, cs)).and_then_cmt(|(datatype, byteord, columns)| {
            from!(Self::try_new(datatype, byteord, columns, conf.as_ref()))
        })
    }

    fn lookup_ro(kws: &StdKeywords, conf: &ReadLayoutConfig) -> FromRawResult<Self> {
        macro_rules! from1 {
            ($i:expr) => {
                $i.map_err(RawParsedError::from).into_log()
            };
        }

        macro_rules! from2 {
            ($i:expr) => {
                $i.map_commutative_warnings(RawToLayoutWarning::from)
                    .map_errors(RawToLayoutError::from)
            };
        }

        let d = from1!(AlphaNumType::get_metaroot_req(kws));
        let b = from1!(ByteOrd2_0::get_metaroot_req(kws));
        let cs = NoMeasDatatype::lookup_ro_all(kws);

        from2!(d.zip3_cmt(b, cs)).and_then_cmt(|(datatype, byteord, columns)| {
            from2!(Self::try_new(datatype, byteord, columns, conf))
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
            From<FixedLayout<Bitmask<U, LEN>, SizedByteOrd<LEN>, T, NoMeasDatatype>>,
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
    ) -> WarningsAndErrorsResult<Self, (), ColumnError<NewMixedTypeWarning>, NewDataLayoutError>
    {
        macro_rules! from {
            ($i:expr) => {
                $i.map_errors(NewDataLayoutError::from)
                    .map_commutative_warnings(ColumnError::inner_into)
                    .map_ok_value(Self::from)
            };
        }

        macro_rules! go_float {
            ($t:ident, $notrunc:expr) => {
                byteord
                    .try_into()
                    .map_err(NewDataLayoutError::from)
                    .into_log()
                    .and_then_cmt(|b| {
                        from! {FixedLayout::try_new(columns, b, |c| {
                            $t::from_width_and_range(c.width, c.range, $notrunc)
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
        res.errors_into()
    }

    pub(crate) fn into_3_1(self) -> LayoutConvertResult<DataLayout3_1> {
        self.into_unmixed().map_ok_value(Into::into)
    }

    pub(crate) fn into_3_2(self) -> LayoutConvertResult<DataLayout3_2> {
        self.into_unmixed().map_ok_value(DataLayout3_2::NonMixed)
    }
}

impl NonMixedEndianLayout<NoMeasDatatype> {
    fn lookup<C>(kws: &mut StdKeywords, conf: &C, par: Par) -> LookupLayoutResult<Self>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        let cs = NoMeasDatatype::lookup_all(kws, par, conf.as_ref());
        let d = AlphaNumType::lookup_req_check_ascii(kws);
        let n = ByteOrd3_1::lookup_req(kws);
        d.zip3_cmt(n, cs)
            .map_commutative_warnings(LookupLayoutWarning::from)
            .map_errors(LookupLayoutError::from)
            .and_then_cmt(|(datatype, byteord, columns)| {
                Self::try_new(datatype, byteord.0, columns, conf.as_ref())
                    .map_commutative_warnings(LookupLayoutWarning::from)
                    .map_errors(LookupLayoutError::from)
            })
    }

    fn lookup_ro(kws: &StdKeywords, conf: &ReadLayoutConfig) -> FromRawResult<Self> {
        let cs = NoMeasDatatype::lookup_ro_all(kws);
        let d = AlphaNumType::get_metaroot_req(kws)
            .map_err(RawParsedError::from)
            .into_log();
        let n = ByteOrd3_1::get_metaroot_req(kws)
            .map_err(RawParsedError::from)
            .into_log();
        d.zip3_cmt(n, cs)
            .map_commutative_warnings(RawToLayoutWarning::from)
            .map_errors(RawToLayoutError::from)
            .and_then_cmt(|(datatype, byteord, columns)| {
                Self::try_new(datatype, byteord.0, columns, conf)
                    .map_commutative_warnings(RawToLayoutWarning::from)
                    .map_errors(RawToLayoutError::from)
            })
    }

    fn try_new(
        datatype: AlphaNumType,
        endian: Endian,
        columns: Vec<ColumnLayoutValues<NullMeasDatatype>>,
        conf: &ReadLayoutConfig,
    ) -> WarningsAndErrorsResult<Self, (), ColumnError<NewMixedTypeWarning>, NewDataLayoutError>
    {
        let notrunc = conf.disallow_range_truncation;

        let go_f32 =
            |c: ColumnLayoutValues<_>| F32Range::from_width_and_range(c.width, c.range, notrunc);

        let go_f64 =
            |c: ColumnLayoutValues<_>| F64Range::from_width_and_range(c.width, c.range, notrunc);

        macro_rules! from {
            ($x:expr) => {
                $x.map_errors(NewDataLayoutError::from)
                    .map_commutative_warnings(ColumnError::inner_into)
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
            Self::Integer(x) => x.uint_try_into_ordered().map_ok_value(Into::into),
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

#[derive(From, Display, Debug, Error)]
pub enum AsciiToUintError {
    NotAscii(NotAsciiError),
    Int(ParseIntError),
}

#[derive(Debug, Error)]
#[error("bytestring is not valid ASCII: {0:?}")]
pub struct NotAsciiError(Vec<u8>);

#[derive(From, Display, Debug, Error)]
pub enum NewDataLayoutError {
    Ascii(ColumnError<NewAsciiRangeError>),
    FixedInt(NewFixedIntLayoutError),
    Float(ColumnError<FloatWidthError>),
    VariableInt(ColumnError<NewUintTypeError>),
    Mixed(ColumnError<NewMixedTypeError>),
    ByteOrd(ByteOrdToSizedError),
}

#[derive(From, Display, Debug, Error)]
pub enum NewFixedIntLayoutError {
    Width(SingleFixedWidthError),
    Column(ColumnError<IntOrderedColumnError>),
}

#[derive(From, Display, Debug, Error)]
pub enum IntOrderedColumnError {
    Order(ByteOrdToSizedError),
    Endian(ByteOrdToSizedEndianError),
    Size(BitmaskError),
}

#[derive(From, Display, Debug, Error)]
pub enum SingleFixedWidthError {
    Bytes(WidthToBytesError),
    Width(WidthMismatchError),
}

#[derive(Debug, Error, new)]
pub struct WidthMismatchError {
    byteord: ByteOrd2_0,
    found: NonEmpty<Bytes>,
}

#[derive(From, Display, Debug, Error)]
pub enum NewMixedTypeError {
    Ascii(NewAsciiRangeError),
    Uint(NewUintTypeError),
    Float(FloatWidthError),
}

#[derive(From, Display, Debug, Error)]
pub enum NewMixedTypeWarning {
    Ascii(IntRangeError<()>),
    Uint(BitmaskError),
    Float(DecimalToFloatError),
}

#[derive(From, Display, Debug, Error)]
pub enum NewUintTypeError {
    Bitmask(BitmaskError),
    Bytes(WidthToBytesError),
}

#[derive(From, Display, Debug, Error)]
pub enum FloatWidthError {
    Bytes(WidthToBytesError),
    WrongWidth(WrongFloatWidth),
    Range(DecimalToFloatError),
}

#[derive(Debug, Error, new)]
#[error("expected width to be {expected} but got {width} when determining float type")]
pub struct WrongFloatWidth {
    pub width: Bytes,
    pub expected: usize,
}

pub type DataReaderResult<T> =
    WarningsAndErrorsResult<T, (), NewDataReaderWarning, NewDataReaderError>;

#[derive(From, Display, Debug, Error)]
pub enum NewDataReaderError {
    TotMismatch(TotEventMismatch),
    ParseTot(ReqKeyError<Tot>),
    ParseSeg(ReqSegmentWithDefaultError<DataSegmentId>),
    Width(UnevenEventWidth),
    Mismatch(SegmentMismatchWarning<DataSegmentId>),
}

#[derive(From, Display)]
pub enum NewDataReaderWarning {
    TotMismatch(TotEventMismatch),
    ParseTot(OptKeyError<Tot>),
    Layout(ColumnError<IntRangeError<()>>),
    Width(UnevenEventWidth),
    Segment(ReqSegmentWithDefaultWarning<DataSegmentId>),
}

#[derive(Error, Debug)]
#[error(
    "$TOT field is {tot} but number of events that \
     evenly fit into DATA is {total_events}"
)]
pub struct TotEventMismatch {
    tot: Tot,
    total_events: u64,
}

#[derive(Error, Debug, new)]
#[error(
    "Events are {event_width} bytes wide, but this does not evenly divide \
     DATA segment which is {nbytes} bytes long (remainder of {remainder})"
)]
pub struct UnevenEventWidth {
    event_width: u64,
    nbytes: u64,
    remainder: u64,
}

#[derive(Error, Debug, new)]
#[error("DATA segment is {event_width} bytes but event width is zero")]
pub struct ZeroEventWidth {
    event_width: u64,
}

#[derive(From, Display, Debug, Error)]
pub enum EventWidthError {
    Zero(ZeroEventWidth),
    Uneven(UnevenEventWidth),
}

#[derive(From, Display, Clone, Copy, Debug, Error)]
pub enum AnyLossError {
    Int(LossError<BitmaskLossError>),
    Float(LossError<Infallible>),
    Ascii(LossError<AsciiLossError>),
}

#[derive(Clone, Copy, Debug, Error)]
#[error("ASCII data was too big and truncated into {0} chars")]
pub struct AsciiLossError(Chars);

#[derive(new, Debug, Error)]
#[error("error when processing measurement {index}: {error}")]
pub struct ColumnError<E> {
    #[new(into)]
    pub index: IndexFromOne,
    #[new(into)]
    pub error: E,
}

impl<E> ColumnError<E> {
    fn inner_into<X>(self) -> ColumnError<X>
    where
        X: From<E>,
    {
        ColumnError::new(self.index, self.error)
    }
}

type LookupLayoutResult<T> = WarningsAndErrorsResult<T, (), LookupLayoutWarning, LookupLayoutError>;

#[derive(From, Display, Debug, Error)]
pub enum LookupLayoutError {
    New(NewDataLayoutError),
    Raw(LookupKeysError),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupLayoutWarning {
    New(ColumnError<NewMixedTypeWarning>),
    Raw(LookupKeysWarning),
}

type FromRawResult<T> = WarningsAndErrorsResult<T, (), RawToLayoutWarning, RawToLayoutError>;

#[derive(From, Display, Debug, Error)]
pub enum RawToLayoutError {
    New(NewDataLayoutError),
    Raw(RawParsedError),
}

#[derive(From, Display, Debug, Error)]
pub enum RawToLayoutWarning {
    New(ColumnError<NewMixedTypeWarning>),
    Raw(OptIndexedKeyError<NumType>),
}

#[derive(From, Display, Debug, Error)]
pub enum RawParsedError {
    AlphaNumType(ReqKeyError<AlphaNumType>),
    Endian(ReqKeyError<ByteOrd3_1>),
    ByteOrd(ReqKeyError<ByteOrd2_0>),
    Par(ReqKeyError<Par>),
    Width(ReqIndexedKeyError<Width>),
    Range(ReqIndexedKeyError<Range>),
}

#[derive(From, Display, Debug, Error)]
pub enum ReadDataframeError {
    Ascii(ReadAsciiError),
    Width(EventWidthError),
    TotMismatch(TotEventMismatch),
    Delim(ReadDelimWithRowsAsciiError),
    DelimNoRows(ReadDelimAsciiWithoutRowsError),
    AlphaNum(AsciiToUintError),
}

#[derive(From, Display, Debug, Error)]
pub enum ReadAsciiError {
    Delim(ReadDelimAsciiError),
    Fixed(ReadFixedAsciiError),
}

#[derive(From, Display, Debug, Error)]
pub enum ReadFixedAsciiError {
    Uneven(UnevenEventWidth),
    Tot(TotEventMismatch),
    ToUint(AsciiToUintError),
}

#[derive(From, Display, Debug, Error)]
pub enum ReadDataframeWarning {
    Uneven(UnevenEventWidth),
    Tot(TotEventMismatch),
}

#[derive(From, Display, Debug, Error)]
pub enum ReadDelimAsciiError {
    Rows(ReadDelimWithRowsAsciiError),
    NoRows(ReadDelimAsciiWithoutRowsError),
}

#[derive(From, Display, Debug, Error)]
pub enum ReadDelimWithRowsAsciiError {
    RowsExceeded(RowsExceededError),
    Incomplete(DelimIncompleteError),
    Parse(AsciiToUintError),
}

// signify that parsing exceeded max rows
#[derive(Debug, Error)]
#[error("Exceeded expected number of rows: {0}")]
pub struct RowsExceededError(usize);

// signify that a parsing ended in the middle of a row
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

#[derive(Debug, Error)]
pub enum ReadDelimAsciiWithoutRowsError {
    #[error("{0}")]
    Parse(AsciiToUintError),
    #[error(
        "parsing delimited ASCII without $TOT \
         resulted in columns with unequal length"
    )]
    Unequal,
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

#[derive(Debug, Error)]
#[error("integer conversion error in column {index}: {error}")]
pub struct ConvertWidthError {
    index: MeasIndex,
    error: UintToUintError,
}

pub type MixedToOrderedLayoutError = MixedColumnConvertError<MixedToOrderedConvertError>;
pub type MixedToNonMixedLayoutError = MixedColumnConvertError<MixedToInnerError>;

#[derive(From, Display, Debug, Error)]
pub enum MixedToOrderedConvertError {
    Integer(MixedToOrderedUintError),
    Other(MixedToInnerError),
}

#[derive(From, Display, Debug, Error)]
pub enum AnyRangeError {
    Ascii(IntRangeError<()>),
    Int(BitmaskError),
    Float(DecimalToFloatError),
}

#[derive(Debug, Error, new)]
#[error("mixed conversion error in column {index}: {error}")]
pub struct MixedColumnConvertError<E> {
    #[new(into)]
    index: MeasIndex,
    #[new(into)]
    error: E,
}

#[derive(From, Display, Debug, Error)]
pub enum MixedToOrderedUintError {
    IsWrongInteger(UintToUintError),
    Other(MixedToInnerError),
}

#[derive(Debug, Error)]
#[error("could not convert integer from {from} bytes to {to} bytes")]
pub struct UintToUintError {
    from: NonZeroU8,
    to: u8,
}

#[derive(Debug, Error, new)]
#[error("could not convert mixed from {} to {dest_type}", .src.as_alpha_num_type())]
pub struct MixedToInnerError {
    dest_type: AlphaNumType,
    src: NullMixedType,
}

#[derive(From, Display, Debug, Error)]
pub enum MeasLayoutMismatchError {
    Lengths(MeasLayoutLengthsError),
    Scale(ColumnError<ScaleMismatchTransformError>),
}

#[derive(Debug, Error)]
#[error("measurement number ({meas_n}) does not match layout column number ({layout_n})")]
pub struct MeasLayoutLengthsError {
    meas_n: usize,
    layout_n: usize,
}

#[derive(Debug, Error)]
#[error(
    "only integer columns may have non-unitary scale transforms, \
     column was '{datatype}' and its scale transform was '{scale}'"
)]
pub struct ScaleMismatchTransformError {
    datatype: AlphaNumType,
    scale: ScaleTransform,
}

#[cfg(feature = "serde")]
pub(crate) fn req_meas_headers() -> [MeasHeader; 2] {
    [Width::std_blank(), Range::std_blank()]
}

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::impl_from_pyerr;
    use crate::python::macros::impl_pyreflow1_err;
    use crate::text::float_decimal::FloatDecimal;
    use crate::text::float_decimal::HasFloatBounds;
    use crate::text::keywords::AlphaNumType;
    use crate::validated::ascii_range::AsciiRange;

    use super::ColumnError;
    use super::LookupLayoutWarning;
    use super::MeasLayoutLengthsError;
    use super::MeasLayoutMismatchError;
    use super::NewDataLayoutError;
    use super::NewMixedTypeWarning;
    use super::ScaleMismatchTransformError;
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

    impl_pyreflow1_err!(MeasurementException, MeasLayoutLengthsError);
    impl_pyreflow1_err!(
        MeasurementException,
        ColumnError<ScaleMismatchTransformError>
    );

    // These are relational because how Range is parsed depends on the value
    // of BYTEORD, DATATYPE, etc.
    impl_pyreflow1_err!(RelationalException, ColumnError<NewMixedTypeWarning>);

    // TODO this feels sloppy
    impl_pyreflow1_err!(FileLayoutError, NewDataLayoutError);

    impl_from_pyerr!(MeasLayoutMismatchError, Lengths, Scale);
    impl_from_pyerr!(LookupLayoutWarning, New, Raw);
}
