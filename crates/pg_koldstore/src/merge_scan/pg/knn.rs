//! Nearest-neighbour segment skipping for `ORDER BY vec <-> const LIMIT k`.
//!
//! The planner side recognises the query shape and hands the scan a [`KnnHint`] through its private
//! data; cold planning then asks the catalog for per-segment vector sketches and drops segments that
//! provably hold no top-`k` row (see `koldstore_parquet::vector_sketch`).
//!
//! The shape is deliberately narrow, because the scan itself never sorts: PostgreSQL's Sort and Limit
//! sit above it, so skipping a segment is only sound when the result cannot depend on it. That needs a
//! single base relation without any filter, grouping or `WITH TIES`, ascending order with NULLs last, a
//! constant query vector, and a constant `LIMIT`/`OFFSET`. Anything else yields no hint.

use koldstore_parquet::KnnMetric;
use koldstore_schema::PgType;
use pgrx::pg_sys;

use super::literals::{list_node_pointers, unwrap_relabel};

/// What the scan needs to rank cold segments by distance.
#[derive(Debug, Clone)]
pub(in crate::merge_scan) struct KnnHint {
    pub(in crate::merge_scan) column_id: i16,
    pub(in crate::merge_scan) metric: KnnMetric,
    /// Rows the query can keep from the scan: `LIMIT` plus `OFFSET`.
    pub(in crate::merge_scan) rows: u64,
    pub(in crate::merge_scan) query: Vec<f32>,
}

/// The planner-time form of a hint: what is stored in the scan's private list.
pub(in crate::merge_scan) struct PlannedKnn {
    pub(in crate::merge_scan) column_id: i16,
    pub(in crate::merge_scan) metric: i32,
    pub(in crate::merge_scan) rows: i32,
    pub(in crate::merge_scan) query_text: String,
}

/// Metric codes persisted in the scan's private list.
const METRIC_L2: i32 = 1;
const METRIC_COSINE: i32 = 2;

impl KnnHint {
    pub(in crate::merge_scan) fn from_private(
        column_id: i32,
        metric: i32,
        rows: i32,
        query_text: &str,
    ) -> Option<Self> {
        let metric = match metric {
            METRIC_L2 => KnnMetric::L2,
            METRIC_COSINE => KnnMetric::Cosine,
            _ => return None,
        };
        Some(Self {
            column_id: i16::try_from(column_id).ok().filter(|id| *id > 0)?,
            metric,
            rows: u64::try_from(rows).ok().filter(|rows| *rows > 0)?,
            query: koldstore_parquet::parse_vector_text(query_text)?,
        })
    }
}

unsafe fn opname(opno: pg_sys::Oid) -> Option<&'static str> {
    let ptr = pg_sys::get_opname(opno);
    if ptr.is_null() {
        return None;
    }
    std::ffi::CStr::from_ptr(ptr).to_str().ok()
}

/// Reads a non-null `int8` constant.
unsafe fn int8_const(node: *mut pg_sys::Node) -> Option<i64> {
    let expr = unwrap_relabel(node.cast::<pg_sys::Expr>());
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let konst = expr.cast::<pg_sys::Const>();
    if (*konst).constisnull || (*konst).consttype != pg_sys::INT8OID {
        return None;
    }
    Some((*konst).constvalue.value() as i64)
}

/// Recognises the nearest-neighbour shape of the query this scan belongs to.
///
/// # Safety
///
/// `root` must be the live planner state of the query being planned.
pub(in crate::merge_scan) unsafe fn plan_knn_hint(
    root: *mut pg_sys::PlannerInfo,
    scanrelid: pg_sys::Index,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    scan_clauses: *mut pg_sys::List,
) -> Option<PlannedKnn> {
    if root.is_null() || (*root).parse.is_null() || !scan_clauses.is_null() {
        return None;
    }
    let parse = (*root).parse;
    let from = (*parse).jointree;
    if from.is_null()
        || !(*from).quals.is_null()
        || (*parse).commandType != pg_sys::CmdType::CMD_SELECT
        || !(*parse).setOperations.is_null()
        || !(*parse).groupClause.is_null()
        || !(*parse).groupingSets.is_null()
        || !(*parse).distinctClause.is_null()
        || !(*parse).havingQual.is_null()
        || (*parse).hasAggs
        || (*parse).hasWindowFuncs
        || (*parse).hasTargetSRFs
        || (*parse).limitOption != pg_sys::LimitOption::LIMIT_OPTION_COUNT
    {
        return None;
    }
    let sources = list_node_pointers((*from).fromlist);
    if sources.len() != 1 {
        return None;
    }
    let source = sources[0].cast::<pg_sys::RangeTblRef>();
    if (*source).type_ != pg_sys::NodeTag::T_RangeTblRef
        || usize::try_from((*source).rtindex).ok() != usize::try_from(scanrelid).ok()
    {
        return None;
    }

    let limit = int8_const((*parse).limitCount)?;
    let offset = if (*parse).limitOffset.is_null() {
        0
    } else {
        int8_const((*parse).limitOffset)?
    };
    if limit <= 0 || offset < 0 {
        return None;
    }
    let rows = i32::try_from(limit.checked_add(offset)?).ok()?;

    let sorts = list_node_pointers((*parse).sortClause);
    let first = sorts.first()?.cast::<pg_sys::SortGroupClause>();
    // Ascending, NULLs last: a NULL distance must not outrank a real one.
    if (*first).nulls_first || opname((*first).sortop) != Some("<") {
        return None;
    }
    let entry = list_node_pointers((*parse).targetList)
        .into_iter()
        .map(|node| node.cast::<pg_sys::TargetEntry>())
        .find(|entry| (**entry).ressortgroupref == (*first).tleSortGroupRef)?;
    let expr = unwrap_relabel((*entry).expr.cast::<pg_sys::Expr>());
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_OpExpr {
        return None;
    }
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let metric = match opname((*op_expr).opno)? {
        "<->" => METRIC_L2,
        "<=>" => METRIC_COSINE,
        _ => return None,
    };
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }
    let vector_oid = vector_type_oid_quiet()?;
    // The distance operators are symmetric.
    let (column_expr, query_expr) = (0..2).find_map(|swap| {
        let (a, b) = if swap == 0 { (args[0], args[1]) } else { (args[1], args[0]) };
        let a = unwrap_relabel(a.cast::<pg_sys::Expr>());
        (!a.is_null() && (*a).type_ == pg_sys::NodeTag::T_Var).then_some((a, b))
    })?;
    let var = column_expr.cast::<pg_sys::Var>();
    if (*var).varlevelsup != 0
        || (*var).varno != i32::try_from(scanrelid).ok()?
        || (*var).varattno <= 0
        || (*var).vartype != vector_oid
    {
        return None;
    }
    let column = catalog.column_by_attnum((*var).varattno)?;
    if column.pg_type != PgType::Vector {
        return None;
    }
    let query = unwrap_relabel(query_expr.cast::<pg_sys::Expr>());
    if query.is_null() || (*query).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let konst = query.cast::<pg_sys::Const>();
    if (*konst).constisnull || (*konst).consttype != vector_oid {
        return None;
    }
    let query_text = super::literals::datum_output_text((*konst).constvalue, vector_oid)?;
    // The text must be one we can parse back, or the hint is unusable.
    koldstore_parquet::parse_vector_text(&query_text)?;
    Some(PlannedKnn {
        column_id: column.column_id.get(),
        metric,
        rows,
        query_text,
    })
}

/// `vector_type_oid` without raising when the extension is missing.
fn vector_type_oid_quiet() -> Option<pg_sys::Oid> {
    // SAFETY: only reads the syscache by name.
    let oid = unsafe { pg_sys::TypenameGetTypid(c"vector".as_ptr()) };
    (oid != pg_sys::InvalidOid).then_some(oid)
}

/// Active segments' sketches for one `vector` column, keyed by relative path.
pub(super) fn load_sketches(
    table_oid: pg_sys::Oid,
    column_id: i16,
) -> Result<Vec<(String, Option<koldstore_parquet::VectorSketch>)>, String> {
    let statement = koldstore_catalog::queries::plan_vector_sketches().map_err(|e| e.to_string())?;
    let args = [
        pgrx::datum::DatumWithOid::from(table_oid),
        pgrx::datum::DatumWithOid::from(i32::from(column_id)),
    ];
    crate::catalog::owner::with_extension_owner(|| {
        crate::spi::execute_prepared(&statement, &args, |tuples| {
            let mut out = Vec::new();
            for tuple in tuples {
                let Some(path) = tuple.get::<String>(1)? else {
                    continue;
                };
                let sketch = tuple
                    .get::<Vec<u8>>(2)?
                    .and_then(|bytes| koldstore_parquet::VectorSketch::decode(&bytes));
                out.push((path, sketch));
            }
            Ok(out)
        })
        .map_err(|error| error.to_string())
    })?
}

/// Upper bound on cold rows hidden from this scan by a hot row or a pending delete: each such row
/// shares its key with a hot heap row or a mirror tombstone.
pub(super) fn masking_rows_bound(
    table_oid: pg_sys::Oid,
    mirror: &koldstore_common::TableName,
) -> Result<u64, String> {
    let sql = format!(
        "SELECT (SELECT count(*) FROM {hot})::bigint + (SELECT count(*) FROM {mirror} WHERE \"op\" = 3)::bigint",
        hot = unsafe { regclass_text(table_oid) }?,
        mirror = mirror.quoted(),
    );
    let count = crate::catalog::owner::with_extension_owner(|| {
        super::with_hook_disabled(|| {
            pgrx::Spi::get_one::<i64>(&sql).map_err(|error| error.to_string())
        })
    })??;
    Ok(u64::try_from(count.unwrap_or(0)).unwrap_or(u64::MAX))
}

unsafe fn regclass_text(table_oid: pg_sys::Oid) -> Result<String, String> {
    crate::catalog::resolve::qualified_relation_name(table_oid)
        .map(|name| name.to_string())
        .map_err(|error| error.to_string())
}
