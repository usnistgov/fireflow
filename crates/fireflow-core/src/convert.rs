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
