use std::time::{Duration, Instant};

pub(crate) trait U32Ext: Sized {
    fn into_u32(self) -> u32;

    fn u32_to_usize(self) -> usize {
        usize::try_from(self.into_u32()).expect("overflow")
    }
}

pub(crate) trait U64Ext: Sized {
    fn into_u64(self) -> u64;

    fn u64_to_usize(self) -> usize {
        usize::try_from(self.into_u64()).expect("overflow")
    }
}

pub(crate) trait UsizeExt: Sized {
    fn into_usize(self) -> usize;

    fn usize_to_u64(self) -> u64 {
        u64::try_from(self.into_usize()).expect("overflow")
    }
}

pub(crate) trait InstantExt: Sized {
    // better version of "duration_since" which does not panic in recent
    // versions; I want it to panic so I know what to fix ;)
    fn duration_since1(self, other: Instant) -> Duration;
}

impl U32Ext for u32 {
    fn into_u32(self) -> u32 {
        self
    }
}

impl U64Ext for u64 {
    fn into_u64(self) -> u64 {
        self
    }
}

impl UsizeExt for usize {
    fn into_usize(self) -> usize {
        self
    }
}

impl InstantExt for Instant {
    fn duration_since1(self, other: Instant) -> Duration {
        self.checked_duration_since(other)
            .expect("self be later than other")
    }
}
