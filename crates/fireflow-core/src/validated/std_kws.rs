use fireflow_types::{
    nonempty::{str::NEStr, string::NEString},
    std_key::{
        CsvFlagKey, DfcKey, GateKey, MeasKey, PseudoStdKey, RealOrPseudoStdKey, RegionKey, RootKey,
        StdKey,
    },
};

use hashbrown::HashMap;

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer, ser::SerializeMap};

/// Encodes all keywords from TEXT that start with a '$'.
///
/// This is validated such that each key only appears once.
#[derive(Default)]
pub struct StdKeywordPairs {
    /// All non-indexed keywords.
    root: RootKeywordPairs,
    /// All meas keywords sorted by key.
    meas: Vec<(MeasKey, NEString)>,
    /// All gate keywords sorted by key.
    gate: Vec<(GateKey, NEString)>,
    /// All region keywords sorted by key.
    region: Vec<(RegionKey, NEString)>,
    /// All values of $CSVnFLAG sorted by 'n'.
    csv_flag: Vec<(CsvFlagKey, NEString)>,
    /// All values of $DFCmTOn sorted by 'm' and 'n'.
    dfc: Vec<(DfcKey, NEString)>,
    /// All pseudostandard keys (non-std keys that start with '$').
    pseudo: HashMap<PseudoStdKey, NEString>,
}

#[derive(Default)]
pub(crate) struct RootKeywordPairs {
    // $NEXTDATA and the two supp text offsets are by themselves since these are
    // independent of standardization and reading DATA. We may need $NEXTDATA to
    // read the next dataset and won't parse anything else if we are only
    // interested in *TEXT*. The supp text offsets are necessary for parsing
    // supplemental TEXT, so these are only necessary for the TEXT parse to
    // complete. Everything else is only necessary downstream. We can take the
    // inner struct and move it elsewhere as necessary.
    nextdata: RootKeyValue,
    stext: STEXTKeywords,
    inner: RootKeywordPairsInner,
}

#[derive(Default)]
pub struct STEXTKeywords {
    begin: RootKeyValue,
    end: RootKeyValue,
}

#[derive(Default)]
pub(crate) struct RootKeywordPairsInner {
    byteord: RootKeyValue,
    datatype: RootKeyValue,
    mode: RootKeyValue,
    par: RootKeyValue,
    tot: RootKeyValue,
    cyt: RootKeyValue,
    abrt: RootKeyValue,
    cells: RootKeyValue,
    com: RootKeyValue,
    exp: RootKeyValue,
    fil: RootKeyValue,
    inst: RootKeyValue,
    lost: RootKeyValue,
    op: RootKeyValue,
    proj: RootKeyValue,
    smno: RootKeyValue,
    src: RootKeyValue,
    sys: RootKeyValue,
    tr: RootKeyValue,
    cytsn: RootKeyValue,
    timestep: RootKeyValue,
    vol: RootKeyValue,
    unicode: RootKeyValue,
    flowrate: RootKeyValue,
    begindata: RootKeyValue,
    beginanalysis: RootKeyValue,
    enddata: RootKeyValue,
    endanalysis: RootKeyValue,
    btim: RootKeyValue,
    etim: RootKeyValue,
    date: RootKeyValue,
    begindatetime: RootKeyValue,
    enddatetime: RootKeyValue,
    comp: RootKeyValue,
    spillover: RootKeyValue,
    lastmodified: RootKeyValue,
    lastmodifier: RootKeyValue,
    originality: RootKeyValue,
    plateid: RootKeyValue,
    platename: RootKeyValue,
    wellid: RootKeyValue,
    unstainedcenters: RootKeyValue,
    unstainedinfo: RootKeyValue,
    carrierid: RootKeyValue,
    carriertype: RootKeyValue,
    locationid: RootKeyValue,
    csmode: RootKeyValue,
    csvbits: RootKeyValue,
    cstot: RootKeyValue,
    gating: RootKeyValue,
    gate: RootKeyValue,
}

#[derive(Default)]
pub(crate) struct RootKeyValue(String);

/// Convert a sequence of pairs into a standard key object.
///
/// If there are duplicates, only the first seen is kept.
///
/// This is intended to be used when creating this object from untrusted input.
/// For trusted cases, there are faster methods internal to the code.
impl FromIterator<(RealOrPseudoStdKey, NEString)> for StdKeywordPairs {
    fn from_iter<T: IntoIterator<Item = (RealOrPseudoStdKey, NEString)>>(iter: T) -> Self {
        // Make an empty object and shove everything into it based on key type
        let mut this = Self::default();
        for (k, v) in iter {
            match k {
                RealOrPseudoStdKey::Real(sk) => match sk {
                    StdKey::Root(rk) => {
                        let _ = this.root.insert(rk, v);
                    }
                    StdKey::Meas(mk) => this.meas.push((mk, v)),
                    StdKey::Gate(gk) => this.gate.push((gk, v)),
                    StdKey::Region(rk) => this.region.push((rk, v)),
                    StdKey::CsvFlag(ck) => this.csv_flag.push((ck, v)),
                    StdKey::Dfc(dk) => this.dfc.push((dk, v)),
                },
                RealOrPseudoStdKey::Pseudo(pk) => {
                    let _ = this.pseudo.insert(pk, v);
                }
            }
        }
        // Sort and dedup the indexed lists
        macro_rules! sort_and_dedup {
            ($field:ident) => {
                this.$field.sort_by_key(|(k, _)| *k);
                this.$field.dedup_by_key(|(k, _)| *k);
            };
        }
        sort_and_dedup!(meas);
        sort_and_dedup!(gate);
        sort_and_dedup!(region);
        sort_and_dedup!(csv_flag);
        sort_and_dedup!(dfc);
        // Be happy
        this
    }
}

impl StdKeywordPairs {
    fn len(&self) -> usize {
        self.iter().count()
            + self.meas.len()
            + self.gate.len()
            + self.region.len()
            + self.csv_flag.len()
            + self.dfc.len()
            + self.pseudo.len()
    }

    fn iter(&self) -> impl Iterator<Item = (RealOrPseudoStdKey, &NEStr)> {
        let root = self.root.iter().map(|(k, v)| (StdKey::Root(k), v));
        let meas = self.meas.iter().map(|(k, v)| (StdKey::Meas(*k), v));
        let gate = self.gate.iter().map(|(k, v)| (StdKey::Gate(*k), v));
        let region = self.region.iter().map(|(k, v)| (StdKey::Region(*k), v));
        let csv = self.csv_flag.iter().map(|(k, v)| (StdKey::CsvFlag(*k), v));
        let dfc = self.dfc.iter().map(|(k, v)| (StdKey::Dfc(*k), v));
        let indexed = meas
            .chain(gate)
            .chain(region)
            .chain(csv)
            .chain(dfc)
            .map(|(k, v)| (k, v.as_ne_str()));
        let real = root
            .chain(indexed)
            .map(|(k, v)| (RealOrPseudoStdKey::Real(k), v));
        // TODO this clone shouldn't be necessary, we don't need to copy the
        // key just to display it, which is the point of this iter
        let pseudo = self
            .pseudo
            .iter()
            .map(|(k, v)| (RealOrPseudoStdKey::Pseudo(k.clone()), v.as_ne_str()));
        real.chain(pseudo)
    }
}

impl RootKeyValue {
    fn put(&mut self, value: NEString) -> Option<NEString> {
        if self.0.is_empty() {
            self.0 = value.into();
            None
        } else {
            Some(value)
        }
    }

    fn as_ne_str(&self) -> Option<&NEStr> {
        NEStr::try_new(self.0.as_str())
    }
}

impl RootKeywordPairs {
    /// Emit all non-empty keys as an interator.
    fn iter(&self) -> impl Iterator<Item = (RootKey, &NEStr)> {
        macro_rules! pair {
            ($var:ident, $($field:ident).*) => {
                (
                    fireflow_types::std_key::RootKey::$var,
                    self.$($field).*.as_ne_str(),
                )
            };
        }
        // Order matters: the iterator will be made in exactly the order shown
        // here
        let ps = [
            pair!(Byteord, inner.byteord),
            pair!(Datatype, inner.datatype),
            pair!(Par, inner.par),
            pair!(Tot, inner.tot),
            pair!(Cyt, inner.cyt),
            pair!(Sys, inner.sys),
            pair!(Cytsn, inner.cytsn),
            pair!(Btim, inner.btim),
            pair!(Etim, inner.etim),
            pair!(Date, inner.date),
            pair!(Begindatetime, inner.begindatetime),
            pair!(Enddatetime, inner.enddatetime),
            pair!(Begindata, inner.begindata),
            pair!(Enddata, inner.enddata),
            pair!(Beginanalysis, inner.beginanalysis),
            pair!(Endanalysis, inner.endanalysis),
            pair!(Beginstext, stext.begin),
            pair!(Endstext, stext.end),
            pair!(Nextdata, nextdata),
            pair!(Mode, inner.mode),
            pair!(Abrt, inner.abrt),
            pair!(Cells, inner.cells),
            pair!(Com, inner.com),
            pair!(Exp, inner.exp),
            pair!(Fil, inner.fil),
            pair!(Inst, inner.inst),
            pair!(Lost, inner.lost),
            pair!(Op, inner.op),
            pair!(Proj, inner.proj),
            pair!(Smno, inner.smno),
            pair!(Src, inner.src),
            pair!(Tr, inner.tr),
            pair!(Timestep, inner.timestep),
            pair!(Vol, inner.vol),
            pair!(Unicode, inner.unicode),
            pair!(Flowrate, inner.flowrate),
            pair!(Comp, inner.comp),
            pair!(Spillover, inner.spillover),
            pair!(LastModified, inner.lastmodified),
            pair!(LastModifier, inner.lastmodifier),
            pair!(Originality, inner.originality),
            pair!(Plateid, inner.plateid),
            pair!(Platename, inner.platename),
            pair!(Wellid, inner.wellid),
            pair!(UnstainedCenters, inner.unstainedcenters),
            pair!(UnstainedInfo, inner.unstainedinfo),
            pair!(CarrierId, inner.carrierid),
            pair!(CarrierType, inner.carriertype),
            pair!(LocationId, inner.locationid),
            pair!(Csmode, inner.csmode),
            pair!(Csvbits, inner.csvbits),
            pair!(Cstot, inner.cstot),
            pair!(Gating, inner.gating),
            pair!(Gate, inner.gate),
        ];
        ps.into_iter().filter_map(|(k, v)| v.map(|vv| (k, vv)))
    }

    fn insert(&mut self, key: RootKey, value: NEString) -> Option<NEString> {
        match key {
            RootKey::Nextdata => self.nextdata.put(value),
            RootKey::Beginstext => self.stext.begin.put(value),
            RootKey::Endstext => self.stext.end.put(value),
            RootKey::Byteord => self.inner.byteord.put(value),
            RootKey::Datatype => self.inner.datatype.put(value),
            RootKey::Mode => self.inner.mode.put(value),
            RootKey::Par => self.inner.par.put(value),
            RootKey::Tot => self.inner.tot.put(value),
            RootKey::Cyt => self.inner.cyt.put(value),
            RootKey::Abrt => self.inner.abrt.put(value),
            RootKey::Cells => self.inner.cells.put(value),
            RootKey::Com => self.inner.com.put(value),
            RootKey::Exp => self.inner.exp.put(value),
            RootKey::Fil => self.inner.fil.put(value),
            RootKey::Inst => self.inner.inst.put(value),
            RootKey::Lost => self.inner.lost.put(value),
            RootKey::Op => self.inner.op.put(value),
            RootKey::Proj => self.inner.proj.put(value),
            RootKey::Smno => self.inner.smno.put(value),
            RootKey::Src => self.inner.src.put(value),
            RootKey::Sys => self.inner.sys.put(value),
            RootKey::Tr => self.inner.tr.put(value),
            RootKey::Cytsn => self.inner.cytsn.put(value),
            RootKey::Timestep => self.inner.timestep.put(value),
            RootKey::Vol => self.inner.vol.put(value),
            RootKey::Unicode => self.inner.unicode.put(value),
            RootKey::Flowrate => self.inner.flowrate.put(value),
            RootKey::Begindata => self.inner.begindata.put(value),
            RootKey::Beginanalysis => self.inner.beginanalysis.put(value),
            RootKey::Enddata => self.inner.enddata.put(value),
            RootKey::Endanalysis => self.inner.endanalysis.put(value),
            RootKey::Btim => self.inner.btim.put(value),
            RootKey::Etim => self.inner.etim.put(value),
            RootKey::Date => self.inner.date.put(value),
            RootKey::Begindatetime => self.inner.begindatetime.put(value),
            RootKey::Enddatetime => self.inner.enddatetime.put(value),
            RootKey::Comp => self.inner.comp.put(value),
            RootKey::Spillover => self.inner.spillover.put(value),
            RootKey::LastModified => self.inner.lastmodified.put(value),
            RootKey::LastModifier => self.inner.lastmodifier.put(value),
            RootKey::Originality => self.inner.originality.put(value),
            RootKey::Plateid => self.inner.plateid.put(value),
            RootKey::Platename => self.inner.platename.put(value),
            RootKey::Wellid => self.inner.wellid.put(value),
            RootKey::UnstainedCenters => self.inner.unstainedcenters.put(value),
            RootKey::UnstainedInfo => self.inner.unstainedinfo.put(value),
            RootKey::CarrierId => self.inner.carrierid.put(value),
            RootKey::CarrierType => self.inner.carriertype.put(value),
            RootKey::LocationId => self.inner.locationid.put(value),
            RootKey::Csmode => self.inner.csmode.put(value),
            RootKey::Csvbits => self.inner.csvbits.put(value),
            RootKey::Cstot => self.inner.cstot.put(value),
            RootKey::Gating => self.inner.gating.put(value),
            RootKey::Gate => self.inner.gate.put(value),
        }
    }
}

#[cfg(feature = "serde")]
impl Serialize for StdKeywordPairs {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (k, v) in self.iter() {
            map.serialize_entry(&k, v)?;
        }
        map.end()
    }
}

#[cfg(feature = "python")]
mod python {
    use super::StdKeywordPairs;

    use fireflow_types::{nonempty::string::NEString, std_key::RealOrPseudoStdKey};

    use pyo3::{prelude::*, types::PyDict};

    impl<'py> FromPyObject<'_, 'py> for StdKeywordPairs {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            // Cast to dict rather than going through rust hashmap to preserver
            // order. It will be sorted anyways but this might avoid some
            // overhead since the input will likely be partly grouped.
            obj.cast::<PyDict>()?
                .iter()
                .map(|(k, v)| Ok((k.extract::<RealOrPseudoStdKey>()?, v.extract::<NEString>()?)))
                .collect()
        }
    }

    impl<'py> IntoPyObject<'py> for StdKeywordPairs {
        type Target = PyDict;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            // Use dict to preserve order
            let out = PyDict::new(py);
            for (k, v) in self.iter() {
                let k_ = k.into_pyobject(py)?;
                let v_ = v.to_owned().into_pyobject(py)?;
                out.set_item(k_, v_)?;
            }
            Ok(out)
        }
    }
}
