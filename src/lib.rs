
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;


#[cfg(test)]
mod tests {
    use crate::map_reduce;

    #[test]
    fn it_works() {
        assert_eq!(map_reduce(vec![1,2,3], |x| (1, x), |x,y| x+y).into_iter().collect::<Vec<_>>(), vec![(1,6)]);
    }
}

fn map_reduce<I, K, V, M, R>(
    data: impl IntoParallelIterator<Item = I>,
    map: M,
    reduce: R,
) -> impl IntoIterator<Item = (K, V)>
    where
        M: Fn(I) -> (K, V) + Sync + Send,
        R: Fn(V, V) -> V + Sync + Send + Copy,
        I: Sync + Send,
        K: Sync + Send + Ord + Hash,
        V: Sync + Send + Default,
{
    let data: Vec<(K, V)> = data.into_par_iter().map(map).collect();

    let mut hash: HashMap<K, Vec<V>> = HashMap::with_capacity(data.len());
    for (key, value) in data.into_iter() {
        hash.entry(key).or_insert_with(Vec::new).push(value)
    }

    let result: BTreeMap<K, V> = hash
        .into_par_iter()
        .map(|(k, v)| (k, v.into_par_iter().reduce(V::default, reduce)))
        .collect();
    result
}