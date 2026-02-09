//! Extract block numbers and other useful information from query plan
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::ops::Range;

use substrait::proto::{
    expression, expression::field_reference, expression::literal::LiteralType, Expression,
};

use sql_query_plan::plan::*;

use crate::sql::rewrite_target::{rewrite_err, RewriteTargetErr, RewriteTargetResult};

static UNIVERSE: Range<i128> = Range {
    start: i128::MIN,
    end: i128::MAX,
};

static EMPTY: Range<i128> = Range { start: 0, end: 0 };

fn is_universe(f: &FieldRange) -> bool {
    extract_range_from_field(f) == &UNIVERSE
}

fn is_empty(f: &FieldRange) -> bool {
    extract_range_from_field(f) == &EMPTY
}

fn extract_range_from_field<'a>(f: &'a FieldRange) -> &'a Range<i128> {
    match f {
        &FieldRange::BlockNumber(ref r) => r,
        &FieldRange::Timestamp(ref r) => r,
    }
}

/// Extracts block numbers and timestamps from the query plan.
pub fn extract_blocks(
    tctx: &mut TraversalContext,
    sources: &mut Sources,
    filter: &Expression,
) -> RewriteTargetResult<()> {
    for src in sources {
        if !src.sqd {
            continue;
        }
        tracing::debug!("Extracting from {}.{}: ", src.schema_name, src.table_name);
        let xtr = ExtractorExprTransformer {};
        let filter = src.filter.as_ref().unwrap_or(filter);
        let _ = match xtr.transform_expr(filter, &src, tctx)? {
            Extractor::Op(o, l, r) => match ranges_from_extractor(&o, &l, &r, &mut src.blocks)? {
                Some(x) => src.blocks.push(x),
                _ => (),
            },
            Extractor::List(f, os) => match ranges_from_list(&f, &os)? {
                Some(x) => src.blocks.push(x),
                _ => (),
            },
            Extractor::Empty => (),
            _ => (),
        };
        tracing::debug!("Ranges: {:?}", src.blocks);
        consolidate_ranges(&mut src.blocks);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    BlockNumber,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Extractor {
    Empty,
    Literal(i128),
    Field(FieldType),
    List(FieldType, Vec<i128>),
    Op(Ext, Box<Extractor>, Box<Extractor>),
}

fn ranges_from_extractor(
    x: &Ext,
    left: &Box<Extractor>,
    right: &Box<Extractor>,
    ranges: &mut Vec<FieldRange>,
) -> RewriteTargetResult<Option<FieldRange>> {
    if COMPARE_OPS.contains(x) {
        ranges_from_compare(x, left, right)
    } else if LOGICAL_OPS.contains(x) {
        ranges_from_logic(x, left, right, ranges)
    } else {
        Ok(None)
    }
}

fn ranges_from_list(
    field: &FieldType,
    options: &[i128],
) -> RewriteTargetResult<Option<FieldRange>> {
    let (mn, mx) = min_and_max(options);
    let r = Range {
        start: mn,
        end: if mx < i128::MAX { mx + 1 } else { i128::MAX },
    };

    if field == &FieldType::BlockNumber {
        Ok(Some(FieldRange::BlockNumber(r)))
    } else {
        Ok(None)
    }
}

fn min_and_max(items: &[i128]) -> (i128, i128) {
    let mut mn = i128::MAX;
    let mut mx = i128::MIN;

    for item in items {
        if *item < mn {
            mn = *item;
        }
        if *item > mx {
            mx = *item;
        }
    }

    if mn > mx {
        (mx, mn)
    } else {
        (mn, mx)
    }
}

fn ranges_from_compare(
    x: &Ext,
    left: &Box<Extractor>,
    right: &Box<Extractor>,
) -> RewriteTargetResult<Option<FieldRange>> {
    if **left == Extractor::Empty || **right == Extractor::Empty {
        return Ok(None);
    }

    let (op, t, i) = normalize_compare_args(x, left, right)?;

    let r = match op {
        Ext::GT => Ok(Range {
            start: i + 1,
            end: i128::MAX,
        }),
        Ext::GE => Ok(Range {
            start: i,
            end: i128::MAX,
        }),
        Ext::LT => Ok(Range {
            start: i128::MIN,
            end: i,
        }),
        Ext::LE => Ok(Range {
            start: i128::MIN,
            end: i + 1,
        }),
        Ext::EQ => Ok(Range {
            start: i,
            end: i + 1,
        }),
        Ext::NE => Ok(UNIVERSE.clone()), // that's a bit harsh
        op => rewrite_err(format!("operator {:?} is not a comparison", op)),
    }?;

    if t == FieldType::BlockNumber {
        Ok(Some(FieldRange::BlockNumber(r)))
    } else if t == FieldType::Timestamp {
        Ok(Some(FieldRange::Timestamp(r)))
    } else {
        rewrite_err("unexpected field type".to_string())
    }
}

fn ranges_from_logic(
    x: &Ext,
    left: &Box<Extractor>,
    right: &Box<Extractor>,
    ranges: &mut Vec<FieldRange>,
) -> RewriteTargetResult<Option<FieldRange>> {
    if **left == Extractor::Empty || **right == Extractor::Empty {
        return Ok(None);
    }

    // TODO: not
    if *x == Ext::Not {
        return Ok(None);
    }

    // TODO: what about a plain boolean?
    let l = match &**left {
        Extractor::Op(o, l, r) => ranges_from_extractor(o, l, r, ranges),
        Extractor::List(f, os) => ranges_from_list(f, os),
        _ => Ok(None),
    }?;

    let r = match &**right {
        Extractor::Op(o, l, r) => ranges_from_extractor(o, l, r, ranges),
        Extractor::List(f, os) => ranges_from_list(f, os),
        _ => Ok(None),
    }?;

    if *x == Ext::And {
        ranges_from_and(l, r, ranges)
    } else {
        ranges_from_or(l, r, ranges)
    }
}

// TODO: handle field-field in compare
fn normalize_compare_args(
    x: &Ext,
    left: &Box<Extractor>,
    right: &Box<Extractor>,
) -> RewriteTargetResult<(Ext, FieldType, i128)> {
    let (op, t, i) = match (&**left, &**right) {
        (Extractor::Field(t), Extractor::Literal(i)) => Ok((x.clone(), t, *i)),
        (Extractor::Literal(i), Extractor::Field(t)) => match x {
            Ext::GT => Ok((Ext::LE, t, *i)),
            Ext::GE => Ok((Ext::LT, t, *i)),
            Ext::LT => Ok((Ext::GE, t, *i)),
            Ext::LE => Ok((Ext::GT, t, *i)),
            Ext::EQ => Ok((Ext::EQ, t, *i)),
            Ext::NE => Ok((Ext::NE, t, *i)),
            op => rewrite_err(format!("operator {:?} is not a comparison", op)),
        },
        op => rewrite_err(format!(
            "unexpected argument for logical operator: {:?}",
            op
        )),
    }?;

    Ok((op, t.clone(), i))
}

fn ranges_from_and(
    left: Option<FieldRange>,
    right: Option<FieldRange>,
    ranges: &mut Vec<FieldRange>,
) -> RewriteTargetResult<Option<FieldRange>> {
    if left.is_some() && right.is_some() {
        range_intersect(left, right, ranges)
    } else if left.is_some() {
        if ranges.is_empty() {
            Ok(left)
        } else {
            distribute_ranges(left.unwrap(), ranges)
        }
    } else if right.is_some() {
        if ranges.is_empty() {
            Ok(right)
        } else {
            distribute_ranges(right.unwrap(), ranges)
        }
    } else {
        Ok(None)
    }
}

fn ranges_from_or(
    left: Option<FieldRange>,
    right: Option<FieldRange>,
    ranges: &mut Vec<FieldRange>,
) -> RewriteTargetResult<Option<FieldRange>> {
    if left.is_some() && right.is_some() {
        range_union(left, right, ranges)
    } else if left.is_some() {
        Ok(left)
    } else if right.is_some() {
        Ok(right)
    } else {
        Ok(None)
    }
}

fn range_union(
    left: Option<FieldRange>,
    right: Option<FieldRange>,
    ranges: &mut Vec<FieldRange>,
) -> RewriteTargetResult<Option<FieldRange>> {
    let (t, l, r) = get_raw_ranges(&left, &right, ranges)?;
    match (&l, &r) {
        (None, None) => return Ok(None),
        (_, None) => return Ok(left),
        (None, _) => return Ok(right),
        _ => (),
    };

    let l = l.unwrap();
    let r = r.unwrap();

    if l == UNIVERSE {
        return Ok(left);
    }
    if r == UNIVERSE {
        return Ok(right);
    }

    let (l, r) = if l.start > r.end { (r, l) } else { (l, r) };

    if l.end <= r.start {
        ranges.push(left.unwrap());
        ranges.push(right.unwrap());
        return Ok(None);
    }

    let s = if l.start < r.start { l.start } else { r.start };

    let e = if l.end > r.end { l.end } else { r.end };

    if s < e {
        match t {
            FieldType::BlockNumber => Ok(Some(FieldRange::BlockNumber(Range { start: s, end: e }))),
            FieldType::Timestamp => Ok(Some(FieldRange::Timestamp(Range { start: s, end: e }))),
        }
    } else {
        match t {
            FieldType::BlockNumber => Ok(Some(FieldRange::BlockNumber(UNIVERSE.clone()))),
            FieldType::Timestamp => Ok(Some(FieldRange::Timestamp(UNIVERSE.clone()))),
        }
    }
}

fn range_intersect(
    left: Option<FieldRange>,
    right: Option<FieldRange>,
    ranges: &mut Vec<FieldRange>,
) -> RewriteTargetResult<Option<FieldRange>> {
    let (t, l, r) = get_raw_ranges(&left, &right, ranges)?;
    match (&l, &r) {
        (None, None) => return Ok(None),
        (_, None) => return Ok(left),
        (None, _) => return Ok(right),
        _ => (),
    };

    let l = l.unwrap();
    let r = r.unwrap();

    if l == UNIVERSE {
        return Ok(right);
    }
    if r == UNIVERSE {
        return Ok(left);
    }

    let (l, r) = if l.start > r.end { (r, l) } else { (l, r) };

    if l.end <= r.start {
        match t {
            FieldType::BlockNumber => return Ok(Some(FieldRange::BlockNumber(EMPTY.clone()))),
            FieldType::Timestamp => return Ok(Some(FieldRange::Timestamp(EMPTY.clone()))),
        }
    }

    let s = if l.start > r.start { l.start } else { r.start };

    let e = if l.end < r.end { l.end } else { r.end };

    if s < e {
        match t {
            FieldType::BlockNumber => Ok(Some(FieldRange::BlockNumber(Range { start: s, end: e }))),
            FieldType::Timestamp => Ok(Some(FieldRange::Timestamp(Range { start: s, end: e }))),
        }
    } else {
        match t {
            FieldType::BlockNumber => Ok(Some(FieldRange::BlockNumber(EMPTY.clone()))),
            FieldType::Timestamp => Ok(Some(FieldRange::Timestamp(EMPTY.clone()))),
        }
    }
}

fn get_raw_ranges(
    left: &Option<FieldRange>,
    right: &Option<FieldRange>,
    ranges: &mut Vec<FieldRange>,
) -> RewriteTargetResult<(FieldType, Option<Range<i128>>, Option<Range<i128>>)> {
    match (left, right) {
        (None, None) => return Ok((FieldType::BlockNumber, None, None)),
        (Some(FieldRange::BlockNumber(l)), None) => {
            return Ok((FieldType::BlockNumber, Some(l.clone()), None));
        }
        (Some(FieldRange::Timestamp(l)), None) => {
            return Ok((FieldType::Timestamp, Some(l.clone()), None));
        }
        (None, Some(FieldRange::BlockNumber(r))) => {
            return Ok((FieldType::BlockNumber, None, Some(r.clone())));
        }
        (None, Some(FieldRange::Timestamp(r))) => {
            return Ok((FieldType::Timestamp, None, Some(r.clone())));
        }
        _ => (),
    };

    let left = left.clone().unwrap();
    let right = right.clone().unwrap();

    if !left.buddy(&right) {
        ranges.push(left);
        ranges.push(right);
        return Ok((FieldType::BlockNumber, None, None));
    }

    match (&left, &right) {
        (FieldRange::BlockNumber(l), FieldRange::BlockNumber(r)) => {
            Ok((FieldType::BlockNumber, Some(l.clone()), Some(r.clone())))
        }
        (FieldRange::Timestamp(l), FieldRange::Timestamp(r)) => {
            Ok((FieldType::Timestamp, Some(l.clone()), Some(r.clone())))
        }
        (_, _) => Ok((FieldType::BlockNumber, None, None)),
    }
}

fn distribute_ranges(
    range: FieldRange,
    ranges: &mut Vec<FieldRange>,
) -> RewriteTargetResult<Option<FieldRange>> {
    if ranges.is_empty() {
        return Ok(Some(range));
    }

    if is_universe(&range) {
        return Ok(None);
    }

    let mut v = Vec::new();
    let mut others = Vec::new();
    let mut dummy = Vec::with_capacity(0);

    for r in ranges.iter() {
        if !range.buddy(r) {
            others.push(r.clone());
            continue;
        }
        let k = range_intersect(Some(range.clone()), Some(r.clone()), &mut dummy)?;
        if let Some(k) = k {
            if !is_empty(&k) {
                v.push(k);
            }
        }
    }

    ranges.clear();
    if v.len() == 1 && others.is_empty() {
        Ok(v.pop())
    } else {
        ranges.append(&mut v);
        if !others.is_empty() {
            ranges.append(&mut others);
        }
        Ok(None)
    }
}

fn consolidate_ranges(ranges: &mut Vec<FieldRange>) {
    if ranges.contains(&FieldRange::BlockNumber(UNIVERSE.clone())) {
        ranges.clear();
    } else {
        let mut seen = HashSet::new();
        ranges.retain(|f| {
            let have_seen = seen.contains(f);
            if !have_seen {
                seen.insert(f.clone());
            }
            !have_seen
        });
    }
}

pub struct ExtractorExprTransformer;

impl ExprTransformer<Extractor, RewriteTargetErr> for ExtractorExprTransformer {
    fn err_producer<T>(msg: String) -> Result<T, RewriteTargetErr> {
        rewrite_err(msg)
    }

    fn transform_literal(&self, l: &expression::Literal) -> Result<Extractor, RewriteTargetErr> {
        match l.literal_type {
            Some(LiteralType::I32(i)) => Ok(Extractor::Literal(i as i128)),
            Some(LiteralType::I64(i)) => Ok(Extractor::Literal(i as i128)),
            Some(LiteralType::String(ref s)) => Ok(extract_string_literal(s)),
            Some(LiteralType::Boolean(_)) => Ok(Extractor::Empty),
            _ => rewrite_err("unsupported literal type".to_string()),
        }
    }

    fn transform_selection(
        &self,
        _tctx: &TraversalContext,
        source: &Source,
        f: &expression::FieldReference,
    ) -> Result<Extractor, RewriteTargetErr> {
        match f.reference_type {
            Some(field_reference::ReferenceType::DirectReference(ref s)) => map_on_dirref(
                source,
                s,
                |src, r| {
                    if let Ok(i) = Self::get_field_index(src, r) {
                        if BLOCK_NUMBER_FIELD_NAMES.contains(&src.fields[i].as_str()) {
                            Ok(Extractor::Field(FieldType::BlockNumber))
                        } else if TIMESTAMP_FIELD_NAMES.contains(&src.fields[i].as_str()) {
                            Ok(Extractor::Field(FieldType::Timestamp))
                        } else {
                            Ok(Extractor::Empty)
                        }
                    } else {
                        Ok(Extractor::Empty)
                    }
                },
                |m| Self::err_producer(m),
            ),
            _ => rewrite_err("not a direct reference".to_string()),
        }
    }

    fn transform_fun(
        &self,
        tctx: &TraversalContext,
        source: &Source,
        f: &expression::ScalarFunction,
    ) -> Result<Extractor, RewriteTargetErr> {
        if f.arguments.len() != 2 {
            return Ok(Extractor::Empty);
        }

        let args = self.get_fun_args(tctx, source, &f.arguments)?;

        if args.len() != 2 {
            return rewrite_err("inconsistent arguments".to_string());
        }

        let fun = tctx.get_fun_from_ext(f)?;

        match (&args[0], &args[1]) {
            (Extractor::Empty, Extractor::Op(_, _, _)) => Ok(args[1].clone()),
            (Extractor::Empty, Extractor::Field(_)) => Ok(args[1].clone()),
            (Extractor::Empty, _) => Ok(Extractor::Empty),
            (Extractor::Op(_, _, _), Extractor::Empty) => Ok(args[0].clone()),
            (Extractor::Field(_), Extractor::Empty) => Ok(args[0].clone()),
            (_, Extractor::Empty) => Ok(Extractor::Empty),
            _ => Ok(Extractor::Op(
                fun,
                Box::new(args[0].clone()),
                Box::new(args[1].clone()),
            )),
        }
    }

    fn transform_list(
        &self,
        tctx: &TraversalContext,
        source: &Source,
        l: &expression::SingularOrList,
    ) -> Result<Extractor, RewriteTargetErr> {
        let field = if let Some(ref v) = l.value {
            Some(self.transform_expr(&*v, source, tctx)?)
        } else {
            None
        };

        let mut os = Vec::new();
        for option in &l.options {
            if let Extractor::Literal(i) = self.transform_expr(option, source, tctx)? {
                os.push(i);
            }
        }

        match field {
            Some(Extractor::Field(t)) => Ok(Extractor::List(t, os)),
            _ => Ok(Extractor::Empty),
        }
    }
}

fn extract_string_literal(s: &str) -> Extractor {
    match s.parse::<DateTime<Utc>>() {
        Ok(dt) => Extractor::Literal(dt.timestamp() as i128),
        _ => Extractor::Empty,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::ops::Range;

    fn block_number() -> Extractor {
        Extractor::Field(FieldType::BlockNumber)
    }

    fn timestamp() -> Extractor {
        Extractor::Field(FieldType::Timestamp)
    }

    fn int_literal(n: i128) -> Extractor {
        Extractor::Literal(n)
    }

    fn binary_op(op: Ext, left: Extractor, right: Extractor) -> Extractor {
        Extractor::Op(op, Box::new(left), Box::new(right))
    }

    #[test]
    fn test_and_two_compares_plain() {
        let mut ranges = Vec::new();
        let left = Box::new(binary_op(Ext::GE, block_number(), int_literal(123456789)));
        let right = Box::new(binary_op(Ext::LE, block_number(), int_literal(123456792)));

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::BlockNumber(Range {
                start: 123456789,
                end: 123456793,
            }))
        );

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_and_two_compares_tp() {
        let mut ranges = Vec::new();
        let left = Box::new(binary_op(Ext::GE, timestamp(), int_literal(123456789)));
        let right = Box::new(binary_op(Ext::LE, timestamp(), int_literal(123456792)));

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::Timestamp(Range {
                start: 123456789,
                end: 123456793,
            }))
        );

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_or_two_compares_plain() {
        let mut ranges = Vec::new();
        let left = Box::new(binary_op(Ext::GE, block_number(), int_literal(123456789)));
        let right = Box::new(binary_op(Ext::LE, block_number(), int_literal(123456792)));

        let have = ranges_from_extractor(&Ext::Or, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, Some(FieldRange::BlockNumber(UNIVERSE.clone())),);

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_or_two_compares_gap() {
        let mut ranges = Vec::new();
        let left = Box::new(binary_op(Ext::GE, block_number(), int_literal(123456792)));
        let right = Box::new(binary_op(Ext::LE, block_number(), int_literal(123456789)));

        let have = ranges_from_extractor(&Ext::Or, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, None);

        assert_eq!(
            ranges,
            vec![
                FieldRange::BlockNumber(Range {
                    start: 123456792,
                    end: i128::MAX,
                }),
                FieldRange::BlockNumber(Range {
                    start: i128::MIN,
                    end: 123456790,
                }),
            ]
        );
    }

    #[test]
    fn test_and_two_equal() {
        let mut ranges = Vec::new();
        let left = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456789)));
        let right = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456792)));

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, Some(FieldRange::BlockNumber(EMPTY.clone())));

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_or_two_equal() {
        let mut ranges = Vec::new();
        let left = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456700)));
        let right = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456900)));

        let have = ranges_from_extractor(&Ext::Or, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, None);

        assert_eq!(
            ranges,
            vec![
                FieldRange::BlockNumber(Range {
                    start: 123456700,
                    end: 123456701,
                }),
                FieldRange::BlockNumber(Range {
                    start: 123456900,
                    end: 123456901,
                }),
            ]
        );
    }

    #[test]
    fn test_and_and_two_compares_with_eq() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456792));

        let and = binary_op(Ext::And, left, right);

        let left = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456790)));

        let right = Box::new(and);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::BlockNumber(Range {
                start: 123456790,
                end: 123456791,
            })),
        );

        assert_eq!(ranges, vec![]);
    }

    #[test]
    // note that 'and' with different field types (i.e. block number and timestamp)
    // is treated exactly like 'or': we need to verify if the block is in the timestamp range
    fn test_and_and_two_compares_with_eq_tp() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, timestamp(), int_literal(123456789));
        let right = binary_op(Ext::LE, timestamp(), int_literal(123456792));

        let and = binary_op(Ext::And, left, right);

        let left = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456790)));

        let right = Box::new(and);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, None);

        assert_eq!(
            ranges,
            vec![
                FieldRange::BlockNumber(Range {
                    start: 123456790,
                    end: 123456791,
                }),
                FieldRange::Timestamp(Range {
                    start: 123456789,
                    end: 123456793,
                }),
            ]
        );
    }

    #[test]
    fn test_and_and_two_compares_with_ne() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456792));

        let and = binary_op(Ext::And, left, right);

        let left = Box::new(binary_op(Ext::NE, block_number(), int_literal(123456790)));

        let right = Box::new(and);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::BlockNumber(Range {
                start: 123456789,
                end: 123456793,
            })),
        );

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_and_or_two_compares_with_eq() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456792));

        let or = binary_op(Ext::Or, left, right);

        let left = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456790)));

        let right = Box::new(or);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::BlockNumber(Range {
                start: 123456790,
                end: 123456791,
            })),
        );

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_and_or_two_compares_with_ne() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456792));

        let or = binary_op(Ext::Or, left, right);

        let left = Box::new(binary_op(Ext::NE, block_number(), int_literal(123456790)));

        let right = Box::new(or);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, Some(FieldRange::BlockNumber(UNIVERSE.clone())),);

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_and_or_two_compares_with_gt() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456792));

        let or = binary_op(Ext::Or, left, right);

        let left = Box::new(binary_op(Ext::GT, block_number(), int_literal(123456790)));

        let right = Box::new(or);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::BlockNumber(Range {
                start: 123456791,
                end: i128::MAX,
            })),
        );

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_and_or_two_compares_with_gap_eq() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::LE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::GE, block_number(), int_literal(123456792));

        let or = binary_op(Ext::Or, left, right);

        let left = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456760)));

        let right = Box::new(or);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::BlockNumber(Range {
                start: 123456760,
                end: 123456761,
            })),
        );

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_and_or_two_compares_with_gap_ne() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::LE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::GE, block_number(), int_literal(123456792));

        let or = binary_op(Ext::Or, left, right);

        let left = Box::new(binary_op(Ext::NE, block_number(), int_literal(123456760)));

        let right = Box::new(or);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, None);

        assert_eq!(
            ranges,
            vec![
                FieldRange::BlockNumber(Range {
                    start: i128::MIN,
                    end: 123456790,
                }),
                FieldRange::BlockNumber(Range {
                    start: 123456792,
                    end: i128::MAX,
                }),
            ]
        );
    }

    #[test]
    fn test_and_or_two_compares_with_gap_le() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::LE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::GE, block_number(), int_literal(123456792));

        let or = binary_op(Ext::Or, left, right);

        let left = Box::new(binary_op(Ext::LE, block_number(), int_literal(123456760)));

        let right = Box::new(or);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::BlockNumber(Range {
                start: i128::MIN,
                end: 123456761,
            })),
        );

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_and_or_two_compares_with_gap_ge() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::LE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::GE, block_number(), int_literal(123456792));

        let or = binary_op(Ext::Or, left, right);

        let left = Box::new(binary_op(Ext::GE, block_number(), int_literal(123456760)));

        let right = Box::new(or);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, None);

        assert_eq!(
            ranges,
            vec![
                FieldRange::BlockNumber(Range {
                    start: 123456760,
                    end: 123456790,
                }),
                FieldRange::BlockNumber(Range {
                    start: 123456792,
                    end: i128::MAX,
                }),
            ]
        );
    }

    #[test]
    fn test_or_and_two_compares_with_eq() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456792));

        let and = binary_op(Ext::And, left, right);

        let left = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456790)));

        let right = Box::new(and);

        let have = ranges_from_extractor(&Ext::Or, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::BlockNumber(Range {
                start: 123456789,
                end: 123456793,
            })),
        );

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_or_and_two_compares_with_eq_tp() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, timestamp(), int_literal(123456789));
        let right = binary_op(Ext::LE, timestamp(), int_literal(123456792));

        let and = binary_op(Ext::And, left, right);

        let left = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456790)));

        let right = Box::new(and);

        let have = ranges_from_extractor(&Ext::Or, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, None);

        assert_eq!(
            ranges,
            vec![
                FieldRange::BlockNumber(Range {
                    start: 123456790,
                    end: 123456791,
                }),
                FieldRange::Timestamp(Range {
                    start: 123456789,
                    end: 123456793,
                }),
            ],
        );
    }

    #[test]
    fn test_or_and_two_compares_with_eq_outside() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456792));

        let and = binary_op(Ext::And, left, right);

        let left = Box::new(binary_op(Ext::EQ, block_number(), int_literal(123456900)));

        let right = Box::new(and);

        let have = ranges_from_extractor(&Ext::Or, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, None);

        assert_eq!(
            ranges,
            vec![
                FieldRange::BlockNumber(Range {
                    start: 123456900,
                    end: 123456901,
                }),
                FieldRange::BlockNumber(Range {
                    start: 123456789,
                    end: 123456793,
                }),
            ]
        );
    }

    #[test]
    fn test_or_and_two_compares_with_ne() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456792));

        let and = binary_op(Ext::And, left, right);

        let left = Box::new(binary_op(Ext::NE, block_number(), int_literal(123456790)));

        let right = Box::new(and);

        let have = ranges_from_extractor(&Ext::Or, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(have, Some(FieldRange::BlockNumber(UNIVERSE.clone())),);

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_and_and_or_two_compares() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456892));

        let or = binary_op(Ext::Or, left, right);

        let left = binary_op(Ext::GE, block_number(), int_literal(123456800));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456802));

        let and = binary_op(Ext::And, left, right);

        let left = Box::new(and);
        let right = Box::new(or);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::BlockNumber(Range {
                start: 123456800,
                end: 123456803,
            })),
        );

        assert_eq!(ranges, vec![]);
    }

    #[test]
    fn test_and_and_or_two_distinct_compares() {
        let mut ranges = Vec::new();

        let left = binary_op(Ext::GE, block_number(), int_literal(123456789));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456792));

        let or = binary_op(Ext::Or, left, right);

        let left = binary_op(Ext::GE, block_number(), int_literal(123456800));
        let right = binary_op(Ext::LE, block_number(), int_literal(123456802));

        let and = binary_op(Ext::And, left, right);

        let left = Box::new(and);
        let right = Box::new(or);

        let have = ranges_from_extractor(&Ext::And, &left, &right, &mut ranges)
            .expect("cannot reduce extractor to ranges");

        assert_eq!(
            have,
            Some(FieldRange::BlockNumber(Range {
                start: 123456800,
                end: 123456803,
            })),
        );

        assert_eq!(ranges, vec![]);
    }
}
