//! An implementation of a spatial index based on the H3 Hexagonal hierarchical
//! geospatial indexing system (<https://h3geo.org/>). The main entry point is
//! the [`make_terms`] function, which fulfils two purposes:
//!
//! * In index mode, it converts a geometry to terms that can be inserted into
//!   an inverted index.
//! * In query mode, it converts a geometry to terms that can be used to query
//!   an inverted index and to find other geometries that *likely* intersect
//!   the given one.
//!
//! The main idea is as follows:
//!
//! * When creating indexing terms, the function first tries to find so-called
//!   *covering cells*, which are H3 cells that describe the shape of the
//!   geometry to index as closely as possible and completely cover it.
//! * The function also records all hierarchical ancestors of the covering
//!   cells. They are called *ancestor cells*.
//! * Finally, the function converts these cells to terms. Ancestor terms get
//!   a special prefix, so they can be differentiated from covering cells.
//!
//! In query mode, the algorithm works similar. It converts a query geometry
//! (such as a bounding box or any polygon) to terms but this time, the covering
//! cells get a prefix and the ancestors don't. Basically, covering cells
//! become ancestor terms, and ancestor cells become covering terms.
//!
//! The generated terms can then be used to query an inverted index. If a
//! geometry is found in the index with at least one matching term, the geometry
//! *likely* intersects with the query geometry. Note that the exact spatial
//! relation between indexed geometry and query geometry still has to be
//! checked afterwards.
//!
//! # Example
//!
//! ```rust
//! use std::collections::{HashMap, HashSet};
//!
//! use assertor::{assert_that, SetAssertion, VecAssertion};
//! use geo::{coord, Intersects, Rect};
//! use georocket_core::index::h3_term_index::{make_terms, TermMode, TermOptions};
//! use h3o::Resolution;
//!
//! // in reality, this would most likely be a database
//! let mut index: HashMap<String, Vec<usize>> = HashMap::new();
//!
//! // Options for creating the terms. MUST be the same for indexing and
//! // querying. Otherwise, the results are undefined.
//! let options = TermOptions {
//!     min_resolution: Resolution::One,
//!     max_resolution: Resolution::Fifteen,
//!     max_cells: 8,
//!     optimize_for_space: false
//! };
//!
//! // prepare some geometries to index
//! let empire_state_building = Rect::new(
//!     coord! { x: -73.986479, y: 40.747914 },
//!     coord! { x: -73.984866, y: 40.748978 },
//! );
//! let fictional_building_close_to_empire_state_building = Rect::new(
//!     coord! { x: -73.986579, y: 40.748914 },
//!     coord! { x: -73.984966, y: 40.749078 },
//! );
//! let main_tower = Rect::new(
//!     coord! { x: 8.6718699, y: 50.1123123 },
//!     coord! { x: 8.6725155, y: 50.1127384 },
//! );
//! let geometries = vec![
//!     empire_state_building,
//!     fictional_building_close_to_empire_state_building,
//!     main_tower
//! ];
//!
//! // add geometries to index
//! for (id, geom) in geometries.iter().enumerate() {
//!     let terms = make_terms(geom, options, TermMode::Index)
//!         .expect("Unable to create index terms");
//!     for t in terms {
//!         index.entry(t).or_default().push(id);
//!     }
//! }
//!
//! // prepare a query
//! let query_rect = Rect::new(
//!     coord! { x: -73.9855, y: 40.7485 },
//!     coord! { x: -73.9856, y: 40.7486 },
//! );
//!
//! // generate terms for the query
//! let query_terms = make_terms(&query_rect, options, TermMode::Query)
//!     .expect("Unable to create query terms");
//!
//! // query the index
//! let mut candidates: HashSet<usize> = HashSet::new();
//! for t in query_terms {
//!     if let Some(ids) = index.get(&t) {
//!         candidates.extend(ids);
//!     }
//! }
//!
//! // the list of candidates will contain the Empire State Building and the
//! // fictional building close to the Empire State Building
//! assert_that!(candidates).has_length(2);
//! assert_that!(candidates).contains(0);
//! assert_that!(candidates).contains(1);
//!
//! // find actual result set by checking the spatial relation between each
//! // candidate and the query geometry
//! let mut result = Vec::new();
//! for c in candidates {
//!     let geom = geometries[c];
//!     if geom.intersects(&query_rect) {
//!         result.push(geom);
//!     }
//! }
//!
//! assert_that!(result).contains_exactly(vec![geometries[0]]);
//! ```
//!
//! # Implementation details
//!
//! The implementation quite closely follows the idea of the `S2RegionTermIndexer`
//! from Google's S2 Geometry Library (<http://s2geometry.io/>) but was adapted
//! to use hexagons instead of squares. The basic idea has already been described
//! above. Here, we give some additional implementation details.
//!
//! ## Why do we need covering terms and ancestor terms?
//!
//! The reason for this is that [`make_terms`] creates covering cells that
//! describe the geometry as best as possible to make the index efficient (i.e.
//! to keep the set of false positives in the list of candidates returned
//! during querying as small as possible). If the indexed geometry and the query
//! geometry are of different sizes, considering covering terms alone might not
//! be sufficient, because they might refer to H3 cells of different
//! resolutions. Therefore, we also need to consider their ancestors to be able
//! to find larger and smaller cells.
//!
//! As described above, [`make_terms`] will create covering and ancestor terms
//! for the indexed geometry. Ancestor terms will have a prefix, so they can be
//! differentiated from the covering terms. For the query geometry, it will
//! also create covering and ancestor teams, but this time, the covering terms
//! will have a prefix (so covering terms will become ancestor terms and
//! ancestor terms will become covering terms).
//!
//! Three cases have to be considered:
//!
//! 1. *The indexed geometry is smaller than the query geometry:* In this case,
//!    The ancestor terms of the indexed geometry will match the covering terms
//!    of the query geometry.
//!
//! 2. *The indexed geometry is larger than the query geometry:* The ancestor
//!    terms of the query geometry will match the covering terms of the indexed
//!    geometry.
//!
//! 3. *Both geometries are roughly of same size:* In this case, the set of
//!    ancestor and covering terms might be disjunct. [`make_terms`] therefore
//!    adds all covering terms twice to its result, once as a covering term
//!    (without a prefix) and once as an ancestor (with a prefix). This makes
//!    sure, at least one of the previous two cases will apply.
//!
//! ## Optimize for space vs. optimize for query complexity
//!
//! Regarding case 3 from the previous section, the artificial ancestor terms
//! [`make_terms`] creates for all covering terms only have to be added at one
//! side: either during indexing or during querying. You can decide yourself,
//! which option suits you best.
//!
//! Adding these terms during querying is called *optimize for space* as less
//! terms need to be added to the index. The index will be smaller, but queries
//! will be more complex, so they *might* take longer, depending on your
//! indexing system.
//!
//! Adding them during indexing will make the index larger but the queries
//! will be less complex and *might* be faster.
//!
//! You can chose between *optimize for space* and *optimize for query
//! complexity* when creating [`TermOptions`].
use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use anyhow::Result;
use geo::{Contains, Intersects, Polygon, Rect};
use h3o::{
    geom::{ContainmentMode, PolyfillConfig, ToCells, ToGeo},
    CellIndex, Resolution,
};
use rustc_hash::FxHashSet;

/// A candidate cell to be converted to a term
#[derive(Debug)]
struct Candidate {
    /// The actual H3 cell
    cell: CellIndex,

    /// The cell's children. `None` if:
    /// * [`terminal`](Self::terminal) is `true`, which means that the cell
    ///   either has no children or should not be refined any further
    /// * [`terminal`](Self::terminal) is `false` and the children have not
    ///   been determined yet
    children: Option<Vec<Candidate>>,

    /// `true` if the cell should not be refined any further. This happens if
    /// the cell's resolution is 15 (the highest resolution possible), if none
    /// of the cell's children intersect with the geometry to index, or if
    /// refining the cell would produce more cells than requested.
    terminal: bool,
}

impl Eq for Candidate {}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cell == other.cell
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Candidates will be added to a priority queue and then processed one after
/// the other. The order defined here ensures that those candidates where we
/// can make the most progress are processed first. We prefer cells with the
/// lowest resolution (i.e. the largest cells), then terminal cells that cannot
/// be refined anymore, then those that have the smallest number of children
/// intersecting with the geometry to index, and finally those with the smallest
/// number of terminal children.
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // prefer cells with the lowest resolution
        self.cell
            .resolution()
            .cmp(&other.cell.resolution())
            // prefer terminal cells (n.b. false is less than true)
            .then(other.terminal.cmp(&self.terminal))
            // prefer cells with the smallest number of intersecting children
            .then(
                self.children
                    .as_ref()
                    .map_or(0, |c| c.len())
                    .cmp(&other.children.as_ref().map_or(0, |c| c.len())),
            )
            // prefer cells with the smallest number of terminal children
            .then(
                self.children
                    .as_ref()
                    .map_or(0, |cs| cs.iter().filter(|c| c.terminal).count())
                    .cmp(
                        &other
                            .children
                            .as_ref()
                            .map_or(0, |cs| cs.iter().filter(|c| c.terminal).count()),
                    ),
            )
    }
}

/// Since we know that we only ever check if hexagon cells lie within a polygon,
/// we can take some shortcuts. For example, to decide whether a hexagon lies
/// within a Rect, we only need to check if all its vertices lie within. This
/// is faster than the default implementation of [`Rect::contains`], which
/// calculates a full DE-9IM matrix.
pub trait FastContains {
    /// Check if the given hexagon cell polygon lies within this geometry
    fn fast_contains(&self, cell_polygon: &Polygon) -> bool;
}

impl FastContains for Rect {
    fn fast_contains(&self, cell_polygon: &Polygon) -> bool {
        cell_polygon.exterior().0.iter().all(|v| self.contains(v))
    }
}

impl FastContains for Polygon {
    fn fast_contains(&self, cell_polygon: &Polygon) -> bool {
        self.contains(cell_polygon)
    }
}

/// Converts a cell to a string and adds a prefix if it is an ancestor cell
fn format_cell(cell: CellIndex, ancestor: bool) -> String {
    if ancestor {
        format!("${cell:x}")
    } else {
        format!("{cell:x}")
    }
}

/// Subdivides a candidate cell. After calling this function, either the
/// candidate's children will have been initialized or the candidate will
/// have been marked as a terminal cell.
///
/// The process works as follows:
///
/// * If the candidate is a terminal cell, do nothing. Terminal cells cannot
///   be subdivided.
/// * Calculate all immediate children of the candidate and also some children
///   of the candidate's neighbors to get a set of cells that completely
///   cover the candidate. Remove those cells from the set that don't intersect
///   with the given geometry.
/// * If the remaining set is empty, mark the candidate as terminal.
/// * If the remaining set is not empty, initialize the candidate's children.
/// * Also, mark children cells that lie completely within the given geometry
///   as terminal.
/// * For performance reasons, you can pass a maximum number of children to
///   generate. If this number is exceeded, the function immediately stops with
///   the process described above and marks the cell as terminal. We can do this
///   because we know that candidates with too many children (more than
///   `max_cells`, see [`make_terms`]) would later be added to the result terms
///   directly and their children would be thrown away anyway.
fn subdivide<G>(
    candidate: &mut Candidate,
    geom: &G,
    max_resolution: Resolution,
    max_children: usize,
) where
    G: Intersects<Polygon> + FastContains,
{
    if candidate.terminal {
        return;
    }

    let child_resolution = candidate
        .cell
        .resolution()
        .succ()
        .expect("Cell resolution can never be None because candidate is not terminal");

    // We need to check two rings and not just the children of the candidate
    // cell (which would be one ring around the center child), because the
    // children of an H3 cell do not completely cover its area. If we only
    // checked the children, we would miss cells if `geom` is close to the
    // candidate's edge and does not fall into the area of at least one of its
    // children. This ensures that we also consider children of neighbours of
    // the candidate cell that extend into it and also intersect with `geom`.
    // let all_children = candidate.cell.children(child_resolution);
    let center_child = candidate.cell.center_child(child_resolution).unwrap();
    let all_children = center_child.grid_disk::<Vec<_>>(2);

    let mut children = Vec::new();
    for child in all_children {
        let child_geom = child.to_geom(true).expect("Function cannot fail");
        if !geom.intersects(&child_geom) {
            continue;
        }

        if children.len() > max_children {
            break;
        }

        let terminal = child_resolution == max_resolution || geom.fast_contains(&child_geom);
        children.push(Candidate {
            cell: child,
            children: None,
            terminal,
        });
    }

    if children.is_empty() || children.len() > max_children {
        candidate.terminal = true
    } else {
        candidate.children = Some(children);
    }
}

/// Builds a tree of cells that completely cover the given geometry. The
/// function performs a breadth-first search (shortest path) starting with
/// `min_resolution`. It stops when it has either reached `max_resolution` and
/// cannot go down the tree any further or when it has found `max_cells`
/// covering cells.
///
/// The function returns a tuple of covering cells and their ancestors (all
/// cells it has visited in the lower resolutions of the tree).
fn build_tree<G>(
    geom: &G,
    min_resolution: Resolution,
    max_resolution: Resolution,
    max_cells: usize,
) -> Result<(Vec<CellIndex>, Vec<CellIndex>)>
where
    G: Intersects<Polygon> + Into<Polygon> + FastContains + Clone,
{
    // get initial candidates covering the geometry completely at minimum resolution
    let geom_poly = h3o::geom::Polygon::from_degrees(Into::<Polygon>::into(geom.clone())).unwrap();
    let cells = geom_poly
        .to_cells(PolyfillConfig::new(min_resolution).containment_mode(ContainmentMode::Covers));

    // perform BFS (shortest path) ...

    // add initial candidates to queue
    let mut queue = BinaryHeap::new();
    let mut seen = FxHashSet::default();
    let mut ancestors = Vec::new();
    let mut result = Vec::new();
    for c in cells {
        let mut candidate = Candidate {
            cell: c,
            children: None,
            terminal: c.resolution() == max_resolution,
        };

        // always subdivide a candidate before adding it to the queue!
        // PartialOrd relies on that!
        subdivide(&mut candidate, geom, max_resolution, max_cells);

        queue.push(Reverse(candidate));
    }

    while queue.len() + result.len() < max_cells {
        let Some(Reverse(c)) = queue.pop() else {
            break;
        };
        if c.terminal {
            // always keep terminal cells
            result.push(c.cell);
        } else if let Some(children) = c.children {
            if result.len() + queue.len() + children.len() > max_cells {
                // Adding children to the queue would exceed max_cells.
                // Keep this candidate instead and don't refine it further.
                result.push(c.cell);
            } else {
                // refine candidate, add children to queue, and then add
                // candidate to ancestors
                for mut new_child in children {
                    subdivide(
                        &mut new_child,
                        geom,
                        max_resolution,
                        // don't produce more children than necessary
                        max_cells - result.len() - queue.len(),
                    );
                    if !seen.contains(&new_child.cell) {
                        seen.insert(new_child.cell);
                        queue.push(Reverse(new_child));
                    }
                }
                ancestors.push(c.cell);
            }
        } else {
            // always keep cells without children (i.e. cells that intersect
            // the polygon but where none of their children do)
            result.push(c.cell);
        }
    }

    // add remaining candidates (if there are any)
    for Reverse(c) in queue {
        result.push(c.cell);
    }

    Ok((result, ancestors))
}

/// Specifies for what purpose the terms should be created (indexing or querying)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TermMode {
    /// Create terms that can be added into an inverted index
    Index,

    /// Create terms that can be used to query an inverted index
    Query,
}

/// Specifies options for [`make_terms`]
#[derive(Clone, Copy)]
pub struct TermOptions {
    /// The minimum H3 resolution at which the function should start to look
    /// for covering cells
    pub min_resolution: Resolution,

    /// The maximum H3 resolution until which the function should look for
    /// covering cells
    pub max_resolution: Resolution,

    /// The maximum number of covering terms the function should return. Note
    /// that the result set will actually be larger as it will also contain
    /// ancestor terms and, depending on [`optimize_for_space`](Self::optimize_for_space),
    /// also artificial ancestor terms.
    pub max_cells: usize,

    /// `true` if the terms created should aim to keep the index size low (i.e.
    /// artificial ancestor terms will only be added during querying). `false`
    /// if index size does not matter but the query complexity should be kept
    /// low (i.e. artificial ancestor terms will only be added during indexing).
    pub optimize_for_space: bool,
}

/// Process the given geometry and create terms that can be added to an inverted
/// index or that can be used to query said index. Please refer to the module's
/// documentation for more information.
pub fn make_terms<G>(poly: &G, options: TermOptions, mode: TermMode) -> Result<Vec<String>>
where
    G: Intersects<Polygon> + Into<Polygon> + FastContains + Clone,
{
    let (covering, ancestors) = build_tree(
        poly,
        options.min_resolution,
        options.max_resolution,
        options.max_cells,
    )?;

    let mut result = Vec::new();
    let ancestor = match mode {
        TermMode::Index => false,
        TermMode::Query => true,
    };

    // add covering cells
    for c in covering {
        result.push(format_cell(c, ancestor));

        // add covering cell also as ancestor if necessary
        if mode == TermMode::Index && !options.optimize_for_space
            || mode == TermMode::Query && options.optimize_for_space
        {
            result.push(format_cell(c, !ancestor));
        }
    }

    // add ancestor cells
    for a in ancestors {
        result.push(format_cell(a, !ancestor));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assertor::{assert_that, ComparableAssertion, VecAssertion};
    use geo::{coord, Coord, Intersects, Rect};
    use h3o::Resolution;
    use rand::{rngs::ThreadRng, Rng};
    use rustc_hash::FxHashSet;

    use crate::index::h3_term_index::{TermMode, TermOptions};

    use super::make_terms;

    fn process_empire_state(options: TermOptions, mode: TermMode) -> Vec<String> {
        let empire_state_building = Rect::new(
            coord! { x: -73.986479, y: 40.747914 },
            coord! { x: -73.984866, y: 40.748978 },
        );
        make_terms(&empire_state_building, options, mode).unwrap()
    }

    /// Index the Empire State Building in 'optimize for space' mode
    #[test]
    fn empire_state_indexing_space() {
        let terms = process_empire_state(
            TermOptions {
                min_resolution: Resolution::Nine,
                max_resolution: Resolution::Twelve,
                max_cells: 8,
                optimize_for_space: true,
            },
            TermMode::Index,
        );

        let expected = vec![
            // at most 8 covering terms
            "8a2a100d2d4ffff".to_string(),
            "8a2a100d2c67fff".to_string(),
            "8a2a100d2d5ffff".to_string(),
            "8a2a100d2897fff".to_string(),
            // at most three ancestors for resolution ten, eleven, and twelve
            "$892a100d28bffff".to_string(),
            "$892a100d2c7ffff".to_string(),
            "$892a100d2d7ffff".to_string(),
        ];

        assert_that!(terms).contains_exactly(expected);
    }

    /// Index the Empire State Building in 'optimize for complexity' mode
    #[test]
    fn empire_state_indexing_complexity() {
        let terms = process_empire_state(
            TermOptions {
                min_resolution: Resolution::Nine,
                max_resolution: Resolution::Twelve,
                max_cells: 8,
                optimize_for_space: false,
            },
            TermMode::Index,
        );

        let expected = vec![
            // at most 8 covering terms + 1 artificial ancestor terms for each
            // covering term
            "8a2a100d2d4ffff".to_string(),
            "$8a2a100d2d4ffff".to_string(),
            "8a2a100d2c67fff".to_string(),
            "$8a2a100d2c67fff".to_string(),
            "8a2a100d2d5ffff".to_string(),
            "$8a2a100d2d5ffff".to_string(),
            "8a2a100d2897fff".to_string(),
            "$8a2a100d2897fff".to_string(),
            // at most three ancestors for resolution ten, eleven, and twelve
            "$892a100d28bffff".to_string(),
            "$892a100d2c7ffff".to_string(),
            "$892a100d2d7ffff".to_string(),
        ];

        assert_that!(terms).contains_exactly(expected);
    }

    /// Query the Empire State Building in 'optimize for space' mode
    #[test]
    fn empire_state_querying_space() {
        let terms = process_empire_state(
            TermOptions {
                min_resolution: Resolution::Nine,
                max_resolution: Resolution::Twelve,
                max_cells: 8,
                optimize_for_space: true,
            },
            TermMode::Query,
        );

        let expected = vec![
            // at most 8 covering terms (marked as ancestors) + 1 artificial
            // ancestor term for each covering term (marked as covering term)
            "$8a2a100d2d4ffff".to_string(),
            "8a2a100d2d4ffff".to_string(),
            "$8a2a100d2c67fff".to_string(),
            "8a2a100d2c67fff".to_string(),
            "$8a2a100d2d5ffff".to_string(),
            "8a2a100d2d5ffff".to_string(),
            "$8a2a100d2897fff".to_string(),
            "8a2a100d2897fff".to_string(),
            // at most three ancestors for resolution ten, eleven, and twelve
            // (marked as covering terms)
            "892a100d28bffff".to_string(),
            "892a100d2c7ffff".to_string(),
            "892a100d2d7ffff".to_string(),
        ];

        assert_that!(terms).contains_exactly(expected);
    }

    /// Query the Empire State Building in 'optimize for complexity' mode
    #[test]
    fn empire_state_querying_complexity() {
        let terms = process_empire_state(
            TermOptions {
                min_resolution: Resolution::Nine,
                max_resolution: Resolution::Twelve,
                max_cells: 8,
                optimize_for_space: false,
            },
            TermMode::Query,
        );

        let expected = vec![
            // at most 8 covering terms (marked as ancestors)
            "$8a2a100d2d4ffff".to_string(),
            "$8a2a100d2c67fff".to_string(),
            "$8a2a100d2d5ffff".to_string(),
            "$8a2a100d2897fff".to_string(),
            // at most three ancestors for resolution ten, eleven, and twelve
            // (marked as covering terms)
            "892a100d28bffff".to_string(),
            "892a100d2c7ffff".to_string(),
            "892a100d2d7ffff".to_string(),
        ];

        assert_that!(terms).contains_exactly(expected);
    }

    /// Create a random point somewhere on Earth
    fn random_point(rng: &mut ThreadRng) -> Coord {
        let lon = rng.gen_range(-180.0..180.0);
        let lat = rng.gen_range(-90.0..90.0);
        coord! { x: lon , y: lat }
    }

    /// Create a random rectangle somewhere on Earth
    fn random_rect(rng: &mut ThreadRng, min_area: f64, max_area: f64) -> Rect {
        let center = random_point(rng);
        let area = max_area * (min_area / max_area).powf(rng.gen());
        Rect::new(
            coord! { x: center.x - area / 2.0, y: center.y - area / 2.0 },
            coord! { x: center.x + area / 2.0, y: center.y + area / 2.0 },
        )
    }

    /// Generate 400 random geometries, add them to an index, then perform 400
    /// queries and check the results
    fn random_geometries(options: TermOptions) {
        let iters = 400;

        let mut index: HashMap<String, Vec<usize>> = HashMap::new();

        let mut rng = rand::thread_rng();
        let min_area = 0.3 * options.min_resolution.area_rads2().to_degrees();
        let max_area = 4.0 * options.max_resolution.area_rads2().to_degrees();
        let mut geometries = Vec::new();
        for _ in 0..iters {
            geometries.push(random_rect(&mut rng, min_area, max_area));
        }

        // add geometries to index
        for (id, geom) in geometries.iter().enumerate() {
            let terms = make_terms(geom, options, TermMode::Index).unwrap();
            for t in terms {
                index.entry(t).or_default().push(id);
            }
        }

        // prepare queries
        let mut queries = Vec::new();
        for _ in 0..iters {
            queries.push(random_rect(&mut rng, min_area, max_area));
        }

        // perform the queries, filter candidates, and compare results
        for q in &queries {
            let expected = geometries
                .iter()
                .filter(|g| g.intersects(q))
                .copied()
                .collect::<Vec<_>>();

            let query_terms = make_terms(q, options, TermMode::Query).unwrap();
            let mut candidates: FxHashSet<usize> = FxHashSet::default();
            for t in query_terms {
                if let Some(ids) = index.get(&t) {
                    candidates.extend(ids);
                }
            }

            assert_that!(candidates.len()).is_less_than(iters / 10);

            let mut result = Vec::new();
            for c in candidates {
                let geom = geometries[c];
                if geom.intersects(q) {
                    result.push(geom);
                }
            }

            assert_that!(result).contains_exactly(&expected);
        }
    }

    /// Runs [`random_geometries`] in 'optimize for space' mode
    #[test]
    fn random_optimize_space() {
        random_geometries(TermOptions {
            min_resolution: Resolution::One,
            max_resolution: Resolution::Fifteen,
            max_cells: 8,
            optimize_for_space: true,
        });
    }

    /// Runs [`random_geometries`] in 'optimize for complexity' mode
    #[test]
    fn random_optimize_complexity() {
        random_geometries(TermOptions {
            min_resolution: Resolution::One,
            max_resolution: Resolution::Fifteen,
            max_cells: 8,
            optimize_for_space: false,
        });
    }
}
