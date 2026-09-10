//! Grouping candidate stations into sets that agree with each other.
//!
//! A device usually sees several access points at once. Most belong together,
//! but a device can also see one access point that moved across town since it
//! was last recorded. Clustering separates those, so a single stale record
//! cannot drag the answer with it.

use crate::locate::network::Network;
use crate::model::StationKey;
use kodama::{linkage, Method};
use std::collections::BTreeMap;

/// Group stations that agree on roughly where they are.
///
/// Complete linkage means every member of a cluster is within `max_distance` of
/// every other member, not merely of its nearest neighbour. That is the strict
/// reading, and it is deliberate: single linkage would let a chain of stations
/// stretch a "cluster" across a whole city.
///
/// Clusters smaller than `min_in_cluster` are discarded. With the default of
/// two, that is what stops anyone locating one specific access point by pairing
/// it with a made-up one.
pub fn cluster_by_distance(
    networks: Vec<Network>,
    max_distance: f64,
    min_in_cluster: usize,
) -> Vec<Vec<Network>> {
    let n = networks.len();
    if n < min_in_cluster || n == 0 {
        return vec![];
    }
    if n == 1 {
        // Only reachable when min_in_cluster is 1, which is the cell setting.
        return vec![networks];
    }
    if n == 2 {
        let d = crate::geo::distance(
            networks[0].lat,
            networks[0].lon,
            networks[1].lat,
            networks[1].lon,
        );
        return if d <= max_distance {
            vec![networks]
        } else if min_in_cluster <= 1 {
            // They disagree, so each stands alone.
            networks.into_iter().map(|net| vec![net]).collect()
        } else {
            // Two singletons, neither big enough to answer with.
            vec![]
        };
    }

    // kodama wants the condensed upper triangle, row by row.
    let mut condensed = Vec::with_capacity(n * (n - 1) / 2);
    for i in 0..n {
        for j in (i + 1)..n {
            condensed.push(crate::geo::distance(
                networks[i].lat,
                networks[i].lon,
                networks[j].lat,
                networks[j].lon,
            ));
        }
    }

    let dendrogram = linkage(&mut condensed, n, Method::Complete);

    // Cut the tree at max_distance. Complete-linkage merges only ever get
    // larger, so every merge at or below the threshold belongs to a flat
    // cluster and everything above it splits.
    let mut uf = UnionFind::new(2 * n - 1);
    for (step_index, step) in dendrogram.steps().iter().enumerate() {
        let merged_node = n + step_index;
        if step.dissimilarity <= max_distance {
            uf.union(step.cluster1, merged_node);
            uf.union(step.cluster2, merged_node);
        }
    }

    let mut groups: BTreeMap<usize, Vec<Network>> = BTreeMap::new();
    for (i, net) in networks.into_iter().enumerate() {
        groups.entry(uf.find(i)).or_default().push(net);
    }

    groups.into_values().filter(|group| group.len() >= min_in_cluster).collect()
}

/// Group cells by the area they belong to: same radio, country, network and
/// location area.
///
/// Cells are not clustered by distance the way access points are. Two cells in
/// one area genuinely cover overlapping ground, however far apart their towers
/// stand, so distance would split sets that belong together.
pub fn cluster_cells_by_area(networks: Vec<Network>) -> Vec<Vec<Network>> {
    let mut areas: BTreeMap<(i8, i32, i32, i64), Vec<Network>> = BTreeMap::new();
    for net in networks {
        if let StationKey::Cell(c) = net.key {
            areas.entry((c.radio.as_i8(), c.mcc, c.mnc, c.lac)).or_default().push(net);
        }
    }
    areas.into_values().collect()
}

/// Disjoint-set over dendrogram nodes, used to turn merge steps into flat
/// clusters.
struct UnionFind {
    parent: Vec<usize>,
}

impl UnionFind {
    fn new(size: usize) -> Self {
        Self { parent: (0..size).collect() }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]]; // halve the path
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb {
            self.parent[ra] = rb;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::{CellKey, Radio, WifiKey};

    fn net_at(id: u64, lat: f64, lon: f64) -> Network {
        Network {
            key: StationKey::Wifi(WifiKey(id)),
            lat,
            lon,
            radius: 50.0,
            age_ms: 1_000.0,
            signal: -70.0,
            score: 1.0,
            seen_today: true,
        }
    }

    fn cell_at(radio: Radio, lac: i64, cid: i64, lat: f64, lon: f64) -> Network {
        Network {
            key: StationKey::Cell(CellKey { radio, mcc: 432, mnc: 11, lac, cid }),
            lat,
            lon,
            ..net_at(0, lat, lon)
        }
    }

    #[test]
    fn nearby_stations_form_one_cluster() {
        // Three points within about 100 m of each other.
        let nets = vec![
            net_at(1, 35.7219, 51.3347),
            net_at(2, 35.7222, 51.3350),
            net_at(3, 35.7216, 51.3344),
        ];
        let clusters = cluster_by_distance(nets, 500.0, 2);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].len(), 3);
    }

    #[test]
    fn distant_stations_split_and_singletons_are_dropped() {
        // Two together, one 5 km away.
        let nets = vec![
            net_at(1, 35.7219, 51.3347),
            net_at(2, 35.7222, 51.3350),
            net_at(3, 35.7700, 51.3900),
        ];
        let clusters = cluster_by_distance(nets, 500.0, 2);
        assert_eq!(clusters.len(), 1, "the lone station is not an answer on its own");
        assert_eq!(clusters[0].len(), 2);
    }

    #[test]
    fn two_groups_both_survive_when_both_are_big_enough() {
        let nets = vec![
            net_at(1, 35.7219, 51.3347),
            net_at(2, 35.7222, 51.3350),
            net_at(3, 35.7700, 51.3900),
            net_at(4, 35.7703, 51.3903),
        ];
        let mut clusters = cluster_by_distance(nets, 500.0, 2);
        clusters.sort_by_key(|c| c.len());
        assert_eq!(clusters.len(), 2);
        assert!(clusters.iter().all(|c| c.len() == 2));
    }

    #[test]
    fn complete_linkage_refuses_to_chain_a_cluster_across_a_city() {
        // A chain where each neighbour is close but the ends are far apart.
        // Single linkage would join all four; complete linkage must not.
        let nets = vec![
            net_at(1, 35.7000, 51.3000),
            net_at(2, 35.7035, 51.3000), // ~390 m from 1
            net_at(3, 35.7070, 51.3000), // ~390 m from 2, ~780 m from 1
            net_at(4, 35.7105, 51.3000), // ~1.2 km from 1
        ];
        let clusters = cluster_by_distance(nets, 500.0, 2);
        assert!(
            clusters.iter().all(|c| c.len() < 4),
            "the whole chain must not become one cluster"
        );
    }

    #[test]
    fn a_single_station_is_not_a_cluster_by_default() {
        assert!(cluster_by_distance(vec![net_at(1, 35.7, 51.3)], 500.0, 2).is_empty());
    }

    #[test]
    fn a_single_station_is_a_cluster_when_the_minimum_allows_it() {
        let clusters = cluster_by_distance(vec![net_at(1, 35.7, 51.3)], 500.0, 1);
        assert_eq!(clusters.len(), 1);
    }

    #[test]
    fn two_stations_that_disagree_produce_nothing() {
        let nets = vec![net_at(1, 35.7219, 51.3347), net_at(2, 35.7700, 51.3900)];
        assert!(cluster_by_distance(nets, 500.0, 2).is_empty());
    }

    #[test]
    fn an_empty_input_produces_nothing() {
        assert!(cluster_by_distance(vec![], 500.0, 2).is_empty());
    }

    #[test]
    fn the_threshold_decides_where_the_split_falls() {
        let nets = vec![net_at(1, 35.7000, 51.3000), net_at(2, 35.7070, 51.3000)];
        // About 780 m apart.
        assert!(cluster_by_distance(nets.clone(), 500.0, 2).is_empty());
        assert_eq!(cluster_by_distance(nets, 1_000.0, 2).len(), 1);
    }

    #[test]
    fn cells_group_by_area_not_by_distance() {
        // Two cells in one area, far apart, plus one in another area.
        let nets = vec![
            cell_at(Radio::Lte, 100, 1, 35.70, 51.30),
            cell_at(Radio::Lte, 100, 2, 35.90, 51.60), // ~35 km away, same area
            cell_at(Radio::Lte, 200, 3, 35.71, 51.31),
        ];
        let mut clusters = cluster_cells_by_area(nets);
        clusters.sort_by_key(|c| c.len());
        assert_eq!(clusters.len(), 2);
        assert_eq!(clusters[0].len(), 1);
        assert_eq!(clusters[1].len(), 2, "distance does not split an area");
    }

    #[test]
    fn cells_on_different_radios_are_different_areas() {
        let nets = vec![
            cell_at(Radio::Lte, 100, 1, 35.70, 51.30),
            cell_at(Radio::Gsm, 100, 1, 35.70, 51.30),
        ];
        assert_eq!(cluster_cells_by_area(nets).len(), 2);
    }

    #[test]
    fn union_find_merges_transitively() {
        let mut uf = UnionFind::new(5);
        uf.union(0, 1);
        uf.union(1, 2);
        assert_eq!(uf.find(0), uf.find(2));
        assert_ne!(uf.find(0), uf.find(3));
    }

    // ---- reference groupings from the real scipy -------------------------
    //
    // scipy.cluster.hierarchy.linkage(method="complete") followed by
    // fcluster(criterion="distance"). These pin the exact grouping, not merely
    // the cluster count, so a change in the cut rule cannot slip through.

    fn assert_groups(points: &[(f64, f64)], max_distance: f64, want: &[&[usize]]) {
        let nets: Vec<Network> =
            points.iter().enumerate().map(|(i, p)| net_at(i as u64, p.0, p.1)).collect();
        let clusters = cluster_by_distance(nets, max_distance, 1);

        let mut got: Vec<Vec<usize>> = clusters
            .iter()
            .map(|c| {
                let mut ids: Vec<usize> = c
                    .iter()
                    .map(|n| match n.key {
                        StationKey::Wifi(w) => w.0 as usize,
                        _ => unreachable!("wifi only in this test"),
                    })
                    .collect();
                ids.sort_unstable();
                ids
            })
            .collect();
        got.sort();

        let mut want: Vec<Vec<usize>> = want.iter().map(|g| g.to_vec()).collect();
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn clustering_matches_scipy_groupings() {
        assert_groups(
            &[(35.7219, 51.3347), (35.7222, 51.3350), (35.7216, 51.3344)],
            500.0,
            &[&[0, 1, 2]],
        );
        assert_groups(
            &[(35.7219, 51.3347), (35.7222, 51.3350), (35.7700, 51.3900)],
            500.0,
            &[&[0, 1], &[2]],
        );
        assert_groups(
            &[(35.7219, 51.3347), (35.7222, 51.3350), (35.7700, 51.3900), (35.7703, 51.3903)],
            500.0,
            &[&[0, 1], &[2, 3]],
        );
        // A chain of four, each ~390 m from the next. Complete linkage splits
        // it in the middle; single linkage would have joined all four.
        assert_groups(
            &[(35.7000, 51.3000), (35.7035, 51.3000), (35.7070, 51.3000), (35.7105, 51.3000)],
            500.0,
            &[&[0, 1], &[2, 3]],
        );
    }

    #[test]
    fn clustering_matches_scipy_on_an_awkward_grid() {
        // A 3x3 grid at a threshold that cuts through it. The grouping is not
        // obvious by eye, which is exactly why it is worth pinning.
        let points: Vec<(f64, f64)> = (0..3)
            .flat_map(|r| {
                (0..3)
                    .map(move |c| (35.7200 + f64::from(r) * 0.002, 51.3300 + f64::from(c) * 0.002))
            })
            .collect();
        assert_groups(&points, 300.0, &[&[0, 3], &[1, 2], &[4, 5, 7, 8], &[6]]);
    }
}
