//! Create strings for HEADER and the first offsets in TEXT.
//!
//! See [OffsetFormatResult] for details on what is created here.
//!
//! This is surprisingly tricky given that some offsets are duplicated b/t
//! HEADER and TEXT, and that the values for each will change depending on
//! how big they are (mostly due to the fact that the HEADER only allows digits
//! up to 99,999,999). This process also differs b/t FCS versions.

use crate::header::HEADER_LEN;
use crate::text::keywords::*;
use crate::validated::standard::*;

/// HEADER and TEXT offsets
pub struct OffsetFormatResult {
    /// The HEADER segment. Always 58 bytes long.
    pub header: String,

    /// The offset TEXT keywords and their values.
    ///
    /// For 2.0 this will only contain $NEXTDATA. For 3.0+, this will contain,
    /// (BEGIN|END)(STEXT|ANALYSIS|DATA).
    pub offsets: Vec<(String, String)>,

    /// The offset where the next data segment can start.
    ///
    /// If beyond 99,999,999 bytes, this will be zero.
    pub real_nextdata: usize,
}

/// Create HEADER+TEXT offsets for FCS 2.0
pub fn make_data_offset_keywords_2_0(
    nooffset_text_len: usize,
    data_len: usize,
    analysis_len: usize,
) -> Option<OffsetFormatResult> {
    // compute rest of offsets
    let begin_prim_text = HEADER_LEN; // always starts after HEADER
    let begin_data = begin_prim_text + nextdata_len() + nooffset_text_len + 1;
    let begin_analysis = begin_data + data_len;
    let nextdata = begin_data + analysis_len;
    let end_prim_text = begin_data - 1;
    let end_data = begin_analysis - 1;
    let end_analysis = nextdata - 1;

    // format header and text offset strings
    let header_prim_text = offset_header_string(begin_prim_text, end_prim_text);
    let header_data = offset_header_string(begin_data, end_data);
    let header_analysis = offset_header_string(begin_analysis, end_analysis);
    let (real_nextdata, text_nextdata) = offset_nextdata_string(nextdata);

    // put everything together, rejoice (alot)
    let header = [header_prim_text, header_data, header_analysis].join("");
    Some(OffsetFormatResult {
        header,
        offsets: vec![text_nextdata],
        real_nextdata,
    })
}

/// Create HEADER+TEXT offsets for FCS 3.0
pub fn make_data_offset_keywords_3_0(
    nooffset_req_text_len: usize,
    opt_text_len: usize,
    data_len: usize,
    analysis_len: usize,
) -> Option<OffsetFormatResult> {
    // +1 at end accounts for first delimiter
    let header_req_text_len = HEADER_LEN + offsets_len_no_val() + nooffset_req_text_len + 1;
    let all_text_len = opt_text_len + header_req_text_len;

    // Find width of formatted offsets, which will depend on if TEXT+HEADER can
    // fit in the first 99,999,999 bytes. If yes, then there is no supplemental
    // text. If not, put all optional keywords in supplemental TEXT.
    let (width, begin_supp_text, begin_data) =
        if let Some(width) = find_offset_width(all_text_len, 0, data_len, analysis_len) {
            // Here we fool the downstream code by setting the Supplemental
            // offsets to be the same as DATA, which will make Supplemental TEXT
            // have zero length which will cause the formatters to do the right
            // thing (ie format as 0,0).
            let begin_data = all_text_len + 6 * width;
            (width, begin_data, begin_data)
        } else if let Some(width) =
            find_offset_width(header_req_text_len, opt_text_len, data_len, analysis_len)
        {
            let begin_supp_text = header_req_text_len + 6 * width;
            let begin_data = begin_supp_text + opt_text_len;
            (width, begin_supp_text, begin_data)
        } else {
            return None;
        };

    // compute rest of offsets
    let begin_prim_text = HEADER_LEN; // always starts after HEADER
    let begin_analysis = begin_data + data_len;
    let nextdata = begin_data + analysis_len;
    let end_prim_text = begin_supp_text - 1;
    let end_supp_text = begin_data - 1;
    let end_data = begin_analysis - 1;
    let end_analysis = nextdata - 1;

    // format header and text offset strings
    let header_prim_text = offset_header_string(begin_prim_text, end_prim_text);
    let header_data = offset_header_string(begin_data, end_data);
    let header_analysis = offset_header_string(begin_analysis, end_analysis);
    let text_supp_text = offset_text_string(
        begin_supp_text,
        end_supp_text,
        Beginstext::std(),
        Endstext::std(),
        width,
    );
    let text_data = offset_text_string(
        begin_data,
        end_data,
        Begindata::std(),
        Enddata::std(),
        width,
    );
    let text_analysis = offset_text_string(
        begin_analysis,
        end_analysis,
        Beginanalysis::std(),
        Endanalysis::std(),
        width,
    );
    let (real_nextdata, text_nextdata) = offset_nextdata_string(nextdata);

    // put everything together, rejoice (alot)
    let header = [header_prim_text, header_data, header_analysis].join("");
    let text = text_supp_text
        .into_iter()
        .chain(text_data)
        .chain(text_analysis)
        .chain([text_nextdata])
        .collect();
    Some(OffsetFormatResult {
        header,
        offsets: text,
        real_nextdata,
    })
}

/// Compute the number of bytes with which to store offsets in the TEXT segment.
///
/// This is tricky to do because the number of digits affects the length of the
/// TEXT segment, which in turn affects the magnitude of the offsets, and round
/// and round.
///
/// How to do this in 9.75 easy steps:
///
/// Define the following operator:
///
///   D := number of digits (f(x) = ceil(log10(x)))
///
/// Define the following constants:
///
///   T = Primary TEXT length (without offset numbers) + HEADER length
///   S = Supplemental TEXT length
///   N = DATA length
///   A = ANALYSIS length
///
/// Define the following variable:
///
///   s = offset for supplemental TEXT segment
///   d = offset for DATA segment
///   a = offset for ANALYSIS segment
///   n = offset for NEXTDATA
///
/// The following relationship must hold:
///
///   s0 = T +             D(s0) + D(s1) + D(d0) + D(d1) + D(a0) + D(a1) + D(n)
///   d0 = T + S +         D(s0) + D(s1) + D(d0) + D(d1) + D(a0) + D(a1) + D(n)
///   a0 = T + S + N +     D(s0) + D(s1) + D(d0) + D(d1) + D(a0) + D(a1) + D(n)
///   n  = T + S + N + A + D(s0) + D(s1) + D(d0) + D(d1) + D(a0) + D(a1) + D(n)
///   s1 = d0 - 1
///   d1 = a0 - 1
///   a1 = n - 1
///
/// What a cruel summation. I don't feel like solving this. :/ Luckily, we don't
/// have to be exact.
///
/// Replace all the D(*) stuff with a new variable "w" (number of digits) which
/// will be a single variable. This mess then becomes the following optimization:
/// $NEXTDATA is an exception since its maximum is capped at 99,999,999 and thus
/// can be assumed to be 8 bytes, which will be sneakily absorbed into T.
///
/// Minimize w
///
/// Subject to:
///
/// s0 = T +             6w
/// d0 = T + S +         6w
/// a0 = T + S + N +     6w
/// n  = T + S + N + A + 6w
/// s1 = d0 - 1
/// d1 = a0 - 1
/// a1 = n - 1
/// D(x) <= w for all x in {s0, s1, d0, d1, a0, a1}
///
/// This assumes the following:
/// - all offsets that require less digits than w will be left-padded with 0,
///   this wastes a few bits but is cheap in the grand scheme of things
/// - all offsets will be included (also quite cheap)
/// - if any segment's length is zero, it will effectively noop that set of
///   equations (they will still be run but they will collapse to be identical
///   to others, so the same comparison will get run multiple times when
///   checking the constraints)
///
/// Computing this is easy, just initialize w at 1 and increase 1 until the
/// constraints are met.
///
/// This is only necessary for FCS 3.0 and up.
fn find_offset_width(
    primary_text_len: usize,
    supp_text_len: usize,
    data_len: usize,
    analysis_len: usize,
) -> Option<usize> {
    let mut w = 1;
    loop {
        let s0 = primary_text_len + 7 * w;
        let d0 = s0 + supp_text_len;
        let a0 = d0 + data_len;
        let n = a0 + analysis_len;
        let s1 = d0 - 1;
        let d1 = a0 - 1;
        let a1 = n - 1;
        if n_digits(s0) <= w
            && n_digits(s1) <= w
            && n_digits(d0) <= w
            && n_digits(d1) <= w
            && n_digits(a0) <= w
            && n_digits(a1) <= w
            && n_digits(n) <= w
        {
            if primary_text_len + 7 * w > 99_999_999 {
                return None;
            } else {
                return Some(w);
            }
        }
        w += 1;
    }
}

/// Create offset keyword pairs for HEADER
///
/// Returns two right-aligned, space-padded numbers exactly 8 bytes long as one
/// contiguous string.
pub fn offset_header_string(begin: usize, end: usize) -> String {
    let nbytes = end - begin + 1;
    let (b, e) = if end <= 99_999_999 && nbytes > 0 {
        (begin, end)
    } else {
        (0, 0)
    };
    format!("{:0>8}{:0>8}", b, e)
}

/// Create offset keyword pairs for TEXT
///
/// Returns a string array like ["BEGINX", "0", "ENDX", "1000"]
pub fn offset_text_string(
    begin: usize,
    end: usize,
    begin_key: StdKey,
    end_key: StdKey,
    width: usize,
) -> [(String, String); 2] {
    let nbytes = end - begin + 1;
    let (b, e) = if nbytes > 0 { (begin, end) } else { (0, 0) };
    let fb = format_zero_padded(b, width);
    let fe = format_zero_padded(e, width);
    [(begin_key.to_string(), fb), (end_key.to_string(), fe)]
}

/// Compute $NEXTDATA offset and format the keyword pair.
///
/// Returns something like (12345678, ["$NEXTDATA", "12345678"])
pub fn offset_nextdata_string(nextdata: usize) -> (usize, (String, String)) {
    let n = if nextdata > 99999999 { 0 } else { nextdata };
    let s = format_zero_padded(n, NEXTDATA_VAL_LEN);
    (n, (Nextdata::std().to_string(), s))
}

/// Compute the number of digits for a number.
///
/// Assume number is greater than 0 and in decimal radix.
fn n_digits(x: usize) -> usize {
    // TODO cast?
    let n = usize::ilog10(x) as usize;
    if 10 ^ n == x {
        n
    } else {
        n + 1
    }
}

/// Format a digit with left-padded zeros to a given length
fn format_zero_padded(x: usize, width: usize) -> String {
    format!("{}{}", ("0").repeat(width - n_digits(x)), x)
}

/// Length of the $NEXTDATA offset length.
///
/// This value has a maximum of 99,999,999, and as such the length of this
/// number is always 8 bytes.
const NEXTDATA_VAL_LEN: usize = 8;

/// Number of bytes consumed by $NEXTDATA keyword + value + delimiters
fn nextdata_len() -> usize {
    Nextdata::len() + NEXTDATA_VAL_LEN + 2
}

/// The number of bytes each offset is expected to take (sans values).
///
/// These are the length of each keyword + 2 since there should be two
/// delimiters counting toward its byte real estate.
fn data_len_no_val() -> usize {
    Begindata::len() + Enddata::len() + 4
}

fn analysis_len_no_val() -> usize {
    Beginanalysis::len() + Endanalysis::len() + 4
}

fn supp_text_len_no_val() -> usize {
    Beginstext::len() + Endstext::len() + 4
}

/// The total number of bytes offset keywords are expected to take (sans values).
///
/// This only applies to 3.0+ since 2.0 only has NEXTDATA.
fn offsets_len_no_val() -> usize {
    data_len_no_val() + analysis_len_no_val() + supp_text_len_no_val() + nextdata_len()
}
