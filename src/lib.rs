use std::{convert::TryFrom, iter, result, collections};

// impl iter for timeseries
// get_vec fns
// impl stream for async usage
// joins between timeseries ->
//      * inner joins (max len = shortest overlap of periods) - only non-contigious parts nullable
//      * full outer joins (max len = union of lengths, both resulting series nullable)
//      * left / right outer joins ->
//  statistics
//      * numerically stable
//      * online algo where possible
//      * allow choosing subset of times
//      * grouping to different timeresolutions (lazy impl?)
//      * max
//      * min
//      * sum
//      * average
//      * std.dev
//  operations
//      * add/mult/div/sub all values by scalar
//      * 
//  storage 
//      * using bincode?
//      * not intended for live-update (turn off db while updating files)
//      * opt a. single file per timeseries (probs not best choice)
//      * opt b. many timeseries per file. set up tree of tags for efficient finding
//          * this allows the analytics engine to keep the file locked.
//          * due to the file being read only, many threads can read it at once.
//

pub struct TagsTree {
}

struct RunLengthEncoded {
}
struct Bitpacked {
}
struct DeltaEncoded {
}

enum TimeseriesData {
    Basic(Vec<rust_decimal::Decimal>),
    // RunLengthEncoded(RunLengthEncoded),
    // Bitpacked(Bitpacked)
    // DeltaEncoded(DeltaEncoded),

}

struct TimeseriesDataIter<'data> {
    data: &'data TimeseriesData,
    position: usize,
}

impl<'data> TimeseriesDataIter<'data> {
    fn new(data: &'data TimeseriesData) -> TimeseriesDataIter<'data> {
        TimeseriesDataIter {
            data,
            position: 0,
        }
    }
}

impl<'data> Iterator for TimeseriesDataIter<'data> {
    type Item = rust_decimal::Decimal;
    fn next(&mut self) -> Option<Self::Item> {
        let point = self.data.get(self.position);
        self.position += 1;
        point
    }
}

impl TimeseriesData {
    fn iter(&self) -> TimeseriesDataIter {
        TimeseriesDataIter::new(self)
    }
    fn get(&self, idx: usize) -> Option<rust_decimal::Decimal> {
        match self {
            TimeseriesData::Basic(vec) => vec.get(idx).copied(),
        }
    }
}

/*
impl IntoIterator for TimeseriesData {
    type Item = rust_decimal::Decimal;
    type IntoIter = TimeseriesDataIter;
    fn into_iter(self) -> Self::IntoIter {
        TimeseriesDataIter::new(self)
    }
}
*/

pub struct Contigious<R: resolution::TimeResolution> {
    period: resolution::TimeRange<R>,
    tags: collections::HashMap<String, String>,

    // timeseries data. length is equal to that of the period.
    // later optimizations include:
    // * delta encoding
    // * run length encoding 
    // * bitpacking
    // * other compression (zstd?)
    // * storing as smaller data types depending on min/max values
    //      * eg factoring out a number if the scale is high but variance is consistent
    data: Vec<rust_decimal::Decimal>,
}

pub struct ContigiousIter<'data, R: resolution::TimeResolution> {
    data: &'data Contigious<R>,
    position: R,
}

impl<'data, R> ContigiousIter<'data, R> 
where R: resolution::TimeResolution 
{
    fn new(data: &'data Contigious<R>) -> ContigiousIter<'data, R> {
        ContigiousIter {
            data, 
            position: data.period().start(),
        }
    }
    fn empty(data: &'data Contigious<R>) -> ContigiousIter<'data, R> {
        ContigiousIter {
            data,
            position: data.period.end().succ(),
        }
    }
}

impl<'data, R> Iterator for ContigiousIter<'data, R> 
where R: resolution::TimeResolution 
{
    type Item = (R, rust_decimal::Decimal);
    fn next(&mut self) -> Option<Self::Item> {
        let current_pos = self.position;
        let item = self.data.get(self.position);
        self.position = self.position.succ();
        item.map(|i| (current_pos, i))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.position.between(self.data.period.end());
        if size <= 0 {
            (0, Some(0))
        } else {
            // this should always be ok
            // as the timerange itself can't be longer than u32, and while the `position`
            // may be moved further, that will simply make the between result negative
            // and we wouldn't end up in this branch anyway
            let size = usize::try_from(size).expect("TimeRange is never longer than u32");
            (size, Some(size))
        }
    }
}
impl<'data, R> iter::ExactSizeIterator for ContigiousIter<'data, R> 
where R: resolution::TimeResolution 
{}
impl<'data, R> iter::FusedIterator for ContigiousIter<'data, R> 
where R: resolution::TimeResolution 
{}

pub enum Error {
    LengthMismatch { period: usize, data: usize },
    Empty,
}

pub type Result<T> = result::Result<T, Error>;

impl<R> Contigious<R> 
where R: resolution::TimeResolution,
{
    fn union(&self, other: &Contigious<R>) -> Option<Contigious<R>> {
        todo!()
        /*
        if let Some(new_range) = self.period.union(other.period) {

            let (earlier, later) = self.period.difference(other.period);
            Some(Contigious {
                period: new_range,
                tags: self.tags.iter().chain(other.tags.iter()).map(|(k, v)| (k.to_string(), v.to_string())).collect(),
                data: if self.period.start() <= other.period.start() {
                    self.data.iter().chain(if let Some(other_range) = later {
                        other.data[].iter()
                    }  else {
                        None.iter()
                    }).collect()
                } else {
                    other.data.iter().chain(if let Some(local_range) = earlier {
                        self.data[].iter()
                    }  else {
                        None.iter()
                    }).collect()
                }
            })
        } else {
            None
        }
        */
    }
    pub fn from_parts(period: resolution::TimeRange<R>, tags: collections::HashMap<String, String>, data: Vec<rust_decimal::Decimal>) -> Result<Contigious<R>> {

        if period.len() == data.len() {
            Ok(Contigious { period, tags, data })
        } else {
            Err(Error::LengthMismatch { period: period.len(), data: data.len() })
        }
    }
    pub fn get_tag(&self, key: &str) -> Option<&String> {
        self.tags.get(key)
    }
    pub fn tags<'a>(&'a self) -> collections::hash_map::Iter<'a, String, String> {
        self.tags.iter()
    }
    pub fn get(&self, at: R) -> Option<rust_decimal::Decimal> {
        self.period.index_of(at).and_then(|idx| self.data.get(idx).copied())
    }
    pub fn iter<'data>(&'data self) -> ContigiousIter<'data, R> {
        ContigiousIter::new(self)
    }

    //later 
    fn filtered_iter<'data>(&'data self, period: resolution::TimeRange<R>) -> ContigiousIter<'data, R> {
        todo!()
    }

    pub fn contains(&self, other: resolution::TimeRange<R>) -> bool {
        self.compare(other) == resolution::TimeRangeComparison::Superset
    }
    pub fn compare(&self, other: resolution::TimeRange<R>) -> resolution::TimeRangeComparison {
        self.period.compare(other)
    }
    pub fn period(&self) -> resolution::TimeRange<R> {
        self.period
    }
}

pub struct NonContigious<R: resolution::TimeResolution> {
    period: resolution::TimeRange<R>,
    tags: collections::HashMap<String, String>,
    
    // timeseries data. 
    // later optimizations include:
    // * delta encoding
    // * run length encoding 
    // * bitpacking
    // * other compression (zstd?)
    // * storing as smaller data types depending on min/max values
    //      * eg factoring out a number if the scale is high but variance is consistent
    data: Vec<Option<rust_decimal::Decimal>>,
}

pub struct NonContigiousIter<'data, R: resolution::TimeResolution> {
    data: &'data NonContigious<R>,
    position: R,
}

impl<'data, R> NonContigiousIter<'data, R> 
where R: resolution::TimeResolution 
{
    fn new(data: &'data NonContigious<R>) -> NonContigiousIter<'data, R> {
        NonContigiousIter {
            data, 
            position: data.period().start(),
        }
    }
    fn empty(data: &'data NonContigious<R>) -> NonContigiousIter<'data, R> {
        NonContigiousIter {
            data,
            position: data.period.end().succ(),
        }
    }
}

impl<'data, R> Iterator for NonContigiousIter<'data, R> 
where R: resolution::TimeResolution 
{
    type Item = (R, Option<rust_decimal::Decimal>);
    fn next(&mut self) -> Option<Self::Item> {
        let current_pos = self.position;
        let item = self.data.get(self.position);
        self.position = self.position.succ();
        if current_pos <= self.data.period.end() {
            Some((current_pos, item))
        } else {
            None
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.position.between(self.data.period.end());
        if size <= 0 {
            (0, Some(0))
        } else {
            // this should always be ok
            // as the timerange itself can't be longer than u32, and while the `position`
            // may be moved further, that will simply make the between result negative
            // and we wouldn't end up in this branch anyway
            let size = usize::try_from(size).expect("TimeRange is never longer than u32");
            (size, Some(size))
        }
    }
}
impl<'data, R> iter::ExactSizeIterator for NonContigiousIter<'data, R> 
where R: resolution::TimeResolution 
{}
impl<'data, R> iter::FusedIterator for NonContigiousIter<'data, R> 
where R: resolution::TimeResolution 
{}

impl<R> NonContigious<R> 
where R: resolution::TimeResolution,
{
    pub fn from_parts(period: resolution::TimeRange<R>, tags: collections::HashMap<String, String>, data: Vec<Option<rust_decimal::Decimal>>) -> Result<NonContigious<R>> {

        if period.len() == data.len() {
            Ok(NonContigious { period, tags, data })
        } else {
            Err(Error::LengthMismatch { period: period.len(), data: data.len() })
        }
    }
    pub fn get_tag(&self, key: &str) -> Option<&String> {
        self.tags.get(key)
    }
    pub fn tags<'a>(&'a self) -> collections::hash_map::Iter<'a, String, String> {
        self.tags.iter()
    }
    pub fn get(&self, at: R) -> Option<rust_decimal::Decimal> {
        self.period.index_of(at).and_then(|idx| self.data.get(idx).copied()).flatten()
    }
    pub fn iter<'data>(&'data self) -> NonContigiousIter<'data, R> {
        NonContigiousIter::new(self)
    }

    //later 
    fn filtered_iter<'data>(&'data self, period: resolution::TimeRange<R>) -> NonContigiousIter<'data, R> {
        todo!()
    }

    pub fn contains(&self, other: resolution::TimeRange<R>) -> bool {
        self.compare(other) == resolution::TimeRangeComparison::Superset
    }
    pub fn compare(&self, other: resolution::TimeRange<R>) -> resolution::TimeRangeComparison {
        self.period.compare(other)
    }
    pub fn period(&self) -> resolution::TimeRange<R> {
        self.period
    }
}
/*
pub struct Sparse<R: resolution::TimeResolution> {
    period: resolution::TimeRange<R>,
    tags: collections::HashMap<String, String>,
    
    // the indexes from within the overall period
    // that there are actual values for.
    // the length of this must be less than the
    // length of the period.
    // later potential optimizations include:
    // * run length encoding
    // * delta encoding
    // * other?
    mask: Vec<bool>,
    // or
    indexes: collections::BTreeSet<usize>,

    // timeseries data. 
    // should have as many data points as there are `true` in the mask
    // all other values within the range will be 'None'
    // later optimizations include:
    // * delta encoding
    // * run length encoding 
    // * bitpacking
    // * other compression (zstd?)
    // * storing as smaller data types depending on min/max values
    //      * eg factoring out a number if the scale is high but variance is consistent
    data: Vec<rust_decimal::Decimal>,
}

impl<R> NonContigious<R> 
where R: resolution::TimeResolution,
{
    pub fn from_map(map: &collections::BTreeMap<R, rust_decimal::Decimal>, tags: collections::HashMap<String, String>) -> Result<Contigious<R>> {

        if !map.is_empty() {
            let start = map.iter().next().unwrap().0;
            let end = map.iter().rev().next().unwrap().0;
            let period = resolution::TimeRange::from_start_end(start, end).expect("Already verified map is not empty"),
            Ok(Contigious { 
                tags,
                period,
                indexes: 
                data: map.values().collect(),
            })
        } else {
            Err(Error::Empty)
        }
    }
    pub fn get_tag(&self, key: &str) -> Option<&String> {
        self.tags.get(key)
    }
    pub fn tags<'a>(&'a self) -> collections::hash_map::Iter<'a, String, String> {
        self.tags.iter()
    }
    pub fn get(&self, at: R) -> Option<rust_decimal::Decimal> {
        self.period.index_of(at).and_then(|idx| self.data.get(idx).copied())
    }
    pub fn iter<'data>(&'data self) -> ContigiousIter<'data, R> {
        ContigiousIter::new(self)
    }

    //later 
    fn filtered_iter<'data>(&'data self, period: resolution::TimeRange<R>) -> ContigiousIter<'data, R> {
        todo!()
    }

    pub fn contains(&self, other: resolution::TimeRange<R>) -> bool {
        self.compare(other) == resolution::TimeRangeComparison::Superset
    }
    pub fn compare(&self, other: resolution::TimeRange<R>) -> resolution::TimeRangeComparison {
        self.period.compare(other)
    }
    pub fn period(&self) -> resolution::TimeRange<R> {
        self.period
    }
}
*/
