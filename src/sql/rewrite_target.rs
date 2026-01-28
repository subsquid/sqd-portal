//! Convert query plan to subplans for workers.
//! This is the implementation of Target Plan for the portal.
use std::collections::HashMap;
use substrait::proto::{
    expression, expression::field_reference, expression::literal::LiteralType, expression::RexType,
    function_argument::ArgType, Expression, FunctionArgument, Rel,
};

use crate::sql::extractor;
use crate::sql::extractor::{Extractor, FieldType};
use sql_query_plan::plan::*;

#[derive(Debug, thiserror::Error)]
pub enum RewriteTargetErr {
    #[error("cannot compile plan: {0}")]
    RewriteTarget(String),
    #[error("traversal context error: {0}")]
    Plan(#[from] PlanErr),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Cannot convert integer: {0}")]
    IntError(#[from] std::num::TryFromIntError),
}

/// Helper to generate RewriteTargetErr
pub fn rewrite_err<T>(msg: String) -> Result<T, RewriteTargetErr> {
    Err(RewriteTargetErr::RewriteTarget(msg))
}

/// The Result Type for this Target Plan.
pub type RewriteTargetResult<T> = Result<T, RewriteTargetErr>;

/// Trait implementation for this specific Target Plan.
#[derive(Clone, Debug)]
pub enum RewriteTarget {
    Empty,
    Relation(RelationType, Vec<Expression>, Box<RewriteTarget>),
    Join(Vec<Expression>, Box<RewriteTarget>, Box<RewriteTarget>),
    Source(Source),
}

impl RewriteTarget {
    /// Extracts block numbers and timestamps from the query plan.
    pub fn extract_blocks(
        &self,
        tctx: &mut TraversalContext,
        sources: &mut Sources,
        filter: &Expression,
    ) -> RewriteTargetResult<()> {
        extractor::extract_blocks(tctx, sources, filter)
    }

    /// Invoke filter pushdown.
    pub fn pushdown_filters(
        &self,
        tctx: &mut TraversalContext,
        sources: &mut Vec<Source>,
    ) -> RewriteTargetResult<()> {
        if tctx.options.filter_pushdown == FilterPushdownLevel::NoPushdown {
            return Ok(());
        }

        self.add_field_refs(sources);

        if let Some(filter) = self.get_filter() {
            if !tctx.has_joins && sources.len() == 1 && sources[0].filter.is_none() {
                self.pushdown_all_filters(sources, &filter);
            } else {
                self.pushdown_filters_per_source(tctx, sources, &filter)?;
                self.pushdown_join_conditions(tctx, sources)?;
            }
            if [
                FilterPushdownLevel::Extract,
                FilterPushdownLevel::Experimental,
            ]
            .contains(&tctx.options.filter_pushdown)
            {
                self.extract_blocks(tctx, sources, &filter)?;
            }
        }
        Ok(())
    }

    // TODO: does not need to be a method
    fn add_field_refs(&self, sources: &mut Vec<Source>) {
        let mut idx: usize = 0;
        for src in sources {
            src.first_field = idx;
            idx += src.projection.len();
        }
    }

    // TODO: does not need to be a method
    fn map_fields_on_sources(&self, sources: &[Source]) -> HashMap<usize, usize> {
        let mut m = HashMap::new();
        let mut s = 0;
        for src in sources.iter() {
            let l = src.projection.len();
            for i in src.first_field..src.first_field + l {
                m.insert(i, s);
            }
            s += 1;
        }
        m
    }

    fn pushdown_all_filters(&self, sources: &mut Vec<Source>, filter: &Expression) {
        sources[0].filter = Some(filter.clone());
    }

    fn pushdown_filters_per_source(
        &self,
        tctx: &TraversalContext,
        sources: &mut Vec<Source>,
        filter: &Expression,
    ) -> RewriteTargetResult<()> {
        for src in sources {
            if !src.sqd || src.filter.is_some() {
                continue; // TODO: smarter is to add to existing filters
            }
            let psh = PushdownExprTransformer::new();
            match psh.transform_expr(filter, src, tctx)? {
                None => continue,
                Some(f) => src.filter = Some(f),
            };
        }
        Ok(())
    }

    fn pushdown_join_conditions(
        &self,
        tctx: &TraversalContext,
        sources: &mut Vec<Source>,
    ) -> RewriteTargetResult<()> {
        let mut joins = Vec::new();
        self.get_joins(&mut joins);
        let field_map = self.map_fields_on_sources(&sources);
        for join in joins.iter() {
            let (l, r) = extract_sources_from_join(join, &field_map, &sources[0]);
            if l.is_none() || r.is_none() {
                continue;
            }

            let l = l.unwrap();
            let r = r.unwrap();

            let ls = &sources[l];
            let rs = &sources[r];

            if extract_field_from_join(join, &ls, tctx) != Extractor::Field(FieldType::BlockNumber)
            {
                continue;
            }

            if extract_field_from_join(join, &rs, tctx) != Extractor::Field(FieldType::BlockNumber)
            {
                continue;
            }

            let lf = ls
                .filter
                .as_ref()
                .map(|f| extract_block_number_compares_from_filter(f, ls, tctx).ok()?);
            let rf = rs
                .filter
                .as_ref()
                .map(|f| extract_block_number_compares_from_filter(f, rs, tctx).ok()?);

            let lf = if lf.is_some() { lf.unwrap() } else { None };

            let rf = if rf.is_some() { rf.unwrap() } else { None };

            // and: extract blocks and add them to ranges!
            if lf.is_some() && rf.is_none() {
                add_expression_to_filter(tctx, &mut sources[r], lf.as_ref().unwrap())?;
            } else if lf.is_none() && rf.is_some() {
                add_expression_to_filter(tctx, &mut sources[l], rf.as_ref().unwrap())?;
            }
        }
        Ok(())
    }

    /// Get all sources from the plan.
    pub fn get_all_sources(&self, v: &mut Vec<Source>) {
        match self {
            RewriteTarget::Empty => (),
            RewriteTarget::Relation(_, _, kid) => kid.get_all_sources(v),
            RewriteTarget::Join(_, left, right) => {
                left.get_all_sources(v);
                right.get_all_sources(v);
            }
            RewriteTarget::Source(src) => v.push(src.clone()),
        }
    }

    /// Get the filter relation.
    pub fn get_filter(&self) -> Option<Expression> {
        match self {
            RewriteTarget::Empty => None,
            RewriteTarget::Relation(RelationType::Filter, exps, _) if exps.len() == 1 => {
                Some(exps[0].clone())
            }
            RewriteTarget::Relation(_, _, kid) => kid.get_filter(),
            RewriteTarget::Join(_, _, _) => None,
            RewriteTarget::Source(_) => None,
        }
    }

    /// Get all joins
    pub fn get_joins(&self, joins: &mut Vec<Expression>) {
        match self {
            RewriteTarget::Empty => (),
            RewriteTarget::Relation(_, _, kid) => kid.get_joins(joins),
            RewriteTarget::Join(x, left, right) => {
                joins.append(&mut x.clone());
                left.get_joins(joins);
                right.get_joins(joins);
            }
            RewriteTarget::Source(_) => (),
        }
    }
}

// TODO: Reduce this monster using generic functions
fn extract_sources_from_join(
    x: &Expression,
    m: &HashMap<usize, usize>,
    dummy: &Source,
) -> (Option<usize>, Option<usize>) {
    match &x.rex_type {
        Some(RexType::ScalarFunction(f)) => {
            if f.arguments.len() != 2 {
                return (None, None);
            }

            let one = match &f.arguments[0].arg_type {
                Some(ArgType::Value(exp)) => match extract_sources_from_join(exp, m, dummy) {
                    (None, None) => None,
                    (l, None) => l,
                    (None, r) => r,
                    (_, _) => None, // we don't handle complex conditions yet
                },
                _ => None,
            };

            let two = match &f.arguments[1].arg_type {
                Some(ArgType::Value(exp)) => match extract_sources_from_join(exp, m, dummy) {
                    (None, None) => None,
                    (l, None) => l,
                    (None, r) => r,
                    (_, _) => None, // we don't handle complex conditions yet
                },
                _ => None,
            };

            (one, two)
        }
        Some(RexType::Selection(f)) => match &f.reference_type {
            Some(field_reference::ReferenceType::DirectReference(s)) => {
                if let Ok(r) = map_on_dirref(
                    dummy,
                    s,
                    |_, r| {
                        let i = r as usize;
                        Ok((m.get(&i).map(|l| *l), None))
                    },
                    |m| rewrite_err(m),
                ) {
                    r
                } else {
                    (None, None)
                }
            }
            _ => (None, None),
        },
        _ => (None, None),
    }
}

fn extract_field_from_join(join: &Expression, s: &Source, tctx: &TraversalContext) -> Extractor {
    let xtr = extractor::ExtractorExprTransformer {};
    match xtr.transform_expr(join, s, tctx) {
        Ok(field @ Extractor::Field(FieldType::BlockNumber)) => field,
        Ok(Extractor::Op(Ext::EQ, left, right)) => match (&*left, &*right) {
            (Extractor::Field(FieldType::BlockNumber), _) => *left,
            (_, Extractor::Field(FieldType::BlockNumber)) => *right,
            _ => Extractor::Empty,
        },
        _ => Extractor::Empty,
    }
}

fn extract_block_number_compares_from_filter(
    x: &Expression,
    src: &Source,
    tctx: &TraversalContext,
) -> Result<Pushdown, RewriteTargetErr> {
    Ok(PushdownExprTransformer::new()
        .with_field_types(BLOCK_NUMBER_FIELD_NAMES)
        .transform_expr(x, src, tctx)?)
}

fn add_expression_to_filter(
    tctx: &TraversalContext,
    source: &mut Source,
    x: &Expression,
) -> Result<(), RewriteTargetErr> {
    let new_x = swap_field_ref(x, source, tctx)?;
    if new_x.is_none() {
        return Ok(());
    }
    if source.filter.is_none() {
        source.filter = Some(new_x.unwrap());
        return Ok(());
    }
    let fun = make_scalar_fun(
        tctx,
        Ext::And,
        &[source.filter.clone().unwrap(), new_x.unwrap()],
    )?;
    source.filter = Some(Expression {
        rex_type: Some(RexType::ScalarFunction(fun)),
    });

    Ok(())
}

fn swap_field_ref(
    x: &Expression,
    src: &Source,
    tctx: &TraversalContext,
) -> Result<Pushdown, RewriteTargetErr> {
    PushdownExprTransformer::new()
        .with_new_reference(src.first_field)
        .transform_expr(x, src, tctx)
}

/// Compiles the Target Plan to SQL. We usually want to transform the original substrait plan
/// into worker-specific substrait plans; for the time being, we use SQL instead because it
/// is easier to debug. In the future this will be a mere testing device.
pub fn compile_sql(src: &Source, tctx: &TraversalContext) -> RewriteTargetResult<String> {
    let pj = compile_projection_sql(src, tctx)?;
    let from = compile_from_sql(src, tctx)?;
    let filter = compile_filter_sql(src, tctx)?;
    Ok(format!("select {} from {} {}", pj, from, filter))
}

fn compile_projection_sql(src: &Source, _tctx: &TraversalContext) -> RewriteTargetResult<String> {
    if !src.sqd {
        return rewrite_err("compile on alien table!".to_string());
    }
    if src.fields.is_empty() {
        return rewrite_err("table without fields!".to_string());
    }

    let fields = map_through_projection(src);
    if fields.is_empty() {
        // the only case with empty projection I'm aware of is count_star
        Ok(format!("\"{}\"", src.fields[0]))
    } else {
        Ok(format!("{}", fields.join(", ")))
    }
}

fn map_through_projection(src: &Source) -> Vec<String> {
    let mut fields = Vec::new();
    for idx in &src.projection {
        fields.push(format!("\"{}\"", src.fields[*idx]));
    }
    fields
}

fn compile_from_sql(src: &Source, _tctx: &TraversalContext) -> RewriteTargetResult<String> {
    Ok(format!("{}.{}.{}", SQD_ID, src.schema_name, src.table_name))
}

fn compile_filter_sql(src: &Source, tctx: &TraversalContext) -> RewriteTargetResult<String> {
    let rw = RewriteExprTransformer {};
    match &src.filter {
        None => Ok("".to_string()),
        Some(f) => Ok(format!("where {}", rw.transform_expr(f, src, tctx)?)),
    }
}

impl TargetPlan for RewriteTarget {
    fn empty() -> Self {
        RewriteTarget::Empty
    }

    fn from_relation(
        relt: RelationType,
        exps: &[Expression],
        _from: &Rel,
        rel: Self,
    ) -> PlanResult<Self> {
        Ok(RewriteTarget::Relation(relt, exps.to_vec(), Box::new(rel)))
    }

    fn from_join(exps: &[Expression], _from: &Rel, left: Self, right: Self) -> PlanResult<Self> {
        match (left, right) {
            (RewriteTarget::Empty, RewriteTarget::Empty) => Ok(RewriteTarget::Empty),
            (RewriteTarget::Empty, right) => Ok(RewriteTarget::Relation(
                RelationType::Other("reduced join"),
                exps.to_vec(),
                Box::new(right),
            )),
            (left, RewriteTarget::Empty) => Ok(RewriteTarget::Relation(
                RelationType::Other("reduced join"),
                exps.to_vec(),
                Box::new(left),
            )),
            (left, right) => Ok(RewriteTarget::Join(
                exps.to_vec(),
                Box::new(left),
                Box::new(right),
            )),
        }
    }

    fn from_source(source: Source) -> Self {
        RewriteTarget::Source(source)
    }

    fn get_source(&self) -> Option<&Source> {
        match self {
            RewriteTarget::Empty => None,
            RewriteTarget::Relation(_, _, kid) => kid.get_source(),
            RewriteTarget::Join(_, left, right) => {
                if let Some(lft) = left.get_source() {
                    Some(lft)
                } else {
                    right.get_source()
                }
            }
            RewriteTarget::Source(src) => Some(src),
        }
    }

    fn get_sources(&self) -> Vec<Source> {
        let mut v = Vec::new();
        self.get_all_sources(&mut v);
        v
    }
}

// The Pushdown Transformer extracts filters relevant for one source,
// it, hence, transforms substrait expressions into substrait expressions.
struct PushdownExprTransformer {
    // Option to extract only filters relevant for the fields
    // in the field list `ft` ("field_type")
    ft: Option<&'static [&'static str]>,
    // Option to change the field reference of the extracted filters
    // setting it to `ff` ("first_field")
    ff: Option<usize>,
}

impl PushdownExprTransformer {
    fn new() -> PushdownExprTransformer {
        PushdownExprTransformer { ft: None, ff: None }
    }

    fn with_field_types(mut self, ft: &'static [&'static str]) -> PushdownExprTransformer {
        self.ff = None;
        self.ft = Some(ft);
        self
    }

    fn with_new_reference(mut self, ff: usize) -> PushdownExprTransformer {
        self.ft = None;
        self.ff = Some(ff);
        self
    }

    fn dirref_to_filter(
        &self,
        source: &Source,
        r: &expression::ReferenceSegment,
    ) -> RewriteTargetResult<Option<expression::ReferenceSegment>> {
        map_on_dirref(
            source,
            r,
            |src, f| {
                if let Some(l) = self.ft {
                    if let Ok(i) = Self::get_field_index(src, f) {
                        if l.contains(&src.fields[i].as_str()) {
                            Ok(Some(r.clone()))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(None)
                    }
                } else if let Some(ff) = self.ff {
                    Ok(Some(ref_seg_from_ref(ff)))
                } else if let Ok(_) = Self::get_field_index(src, f) {
                    Ok(Some(r.clone()))
                } else {
                    Ok(None)
                }
            },
            |m| rewrite_err(m),
        )
    }
}

type Pushdown = Option<Expression>;

impl ExprTransformer<Pushdown, RewriteTargetErr> for PushdownExprTransformer {
    fn err_producer<T>(msg: String) -> Result<T, RewriteTargetErr> {
        rewrite_err(msg)
    }

    fn transform_literal(&self, l: &expression::Literal) -> Result<Pushdown, RewriteTargetErr> {
        Ok(Some(Expression {
            rex_type: Some(RexType::Literal(l.clone())),
        }))
    }

    fn transform_cast(
        &self,
        _tctx: &TraversalContext,
        _source: &Source,
        c: &expression::Cast,
    ) -> Result<Pushdown, RewriteTargetErr> {
        Ok(Some(Expression {
            rex_type: Some(RexType::Cast(Box::new(c.clone()))),
        }))
    }

    fn transform_selection(
        &self,
        _tctx: &TraversalContext,
        source: &Source,
        f: &expression::FieldReference,
    ) -> Result<Pushdown, RewriteTargetErr> {
        let mine = match &f.reference_type {
            Some(field_reference::ReferenceType::DirectReference(s)) => {
                match self.dirref_to_filter(source, s)? {
                    None => Ok(None),
                    Some(mine) => Ok(Some(expression::FieldReference {
                        reference_type: Some(field_reference::ReferenceType::DirectReference(mine)),
                        root_type: f.root_type.clone(),
                    })),
                }
            }
            _ => rewrite_err("unsupported reference type".to_string()),
        }?;

        if let Some(m) = mine {
            Ok(Some(Expression {
                rex_type: Some(RexType::Selection(Box::new(m))),
            }))
        } else {
            Ok(None)
        }
    }

    fn transform_fun(
        &self,
        tctx: &TraversalContext,
        source: &Source,
        f: &expression::ScalarFunction,
    ) -> Result<Pushdown, RewriteTargetErr> {
        let fun = tctx.get_fun_from_ext(f)?;
        if fun == Ext::Or || fun == Ext::Not {
            // TODO: there may be ors and nots we should consider!
            return Ok(None);
        }

        let l = f.arguments.len();
        if l != 2 {
            // only binary operators for now
            return Ok(None);
        }

        let mut xs = Vec::with_capacity(l);
        for arg in &f.arguments {
            match &arg.arg_type {
                Some(ArgType::Value(exp)) => {
                    if let Some(x) = self.transform_expr(exp, source, tctx)? {
                        xs.push(FunctionArgument {
                            arg_type: Some(ArgType::Value(x)),
                        });
                    }
                }
                Some(_) => continue,
                None => continue,
            }
        }

        let mine = if xs.len() == l {
            Some(make_scalar_fun_from_other(&f, &xs))
        } else if xs.len() > 0 {
            let mut s = None;
            for x in &xs {
                if let Some(f) = get_expr_if_scalar_fun(&x) {
                    s = Some(make_scalar_fun_from_other(&f, &f.arguments));
                }
            }
            s
        } else {
            None
        };

        if let Some(m) = mine {
            Ok(Some(Expression {
                rex_type: Some(RexType::ScalarFunction(m)),
            }))
        } else {
            Ok(None)
        }
    }

    fn transform_list(
        &self,
        tctx: &TraversalContext,
        source: &Source,
        l: &expression::SingularOrList,
    ) -> Result<Pushdown, RewriteTargetErr> {
        let f = match &l.value {
            Some(v) => self.transform_expr(v, source, tctx)?,
            _ => None,
        };

        if f.is_none() {
            return Ok(None);
        }

        f.unwrap();

        let mut idx = 0;
        for o in &l.options {
            if let Some(_) = self.transform_expr(o, source, tctx)? {
                idx += 1;
            }
        }

        if idx == l.options.len() {
            Ok(Some(Expression {
                rex_type: Some(RexType::SingularOrList(Box::new(l.clone()))),
            }))
        } else {
            Ok(None)
        }
    }
}

// Compiles substrait expressions into textual SQL expressions.
struct RewriteExprTransformer;

impl RewriteExprTransformer {
    fn get_col_from_source(source: &Source, r: i32) -> Result<String, RewriteTargetErr> {
        Ok(format!(
            "\"{}\"",
            Self::get_field_name_from_source(source, r)?,
        ))
    }
}

impl ExprTransformer<String, RewriteTargetErr> for RewriteExprTransformer {
    fn err_producer<T>(msg: String) -> Result<T, RewriteTargetErr> {
        rewrite_err(msg)
    }

    fn transform_literal(&self, l: &expression::Literal) -> Result<String, RewriteTargetErr> {
        match &l.literal_type {
            Some(LiteralType::Boolean(b)) => Ok(b.to_string()),
            Some(LiteralType::I32(i)) => Ok(i.to_string()),
            Some(LiteralType::I64(i)) => Ok(i.to_string()),
            Some(LiteralType::String(s)) => Ok(format!("'{}'", s)),
            _ => rewrite_err("unsupported literal type".to_string()),
        }
    }

    fn transform_selection(
        &self,
        _tctx: &TraversalContext,
        source: &Source,
        f: &expression::FieldReference,
    ) -> Result<String, RewriteTargetErr> {
        match &f.reference_type {
            Some(field_reference::ReferenceType::DirectReference(s)) => map_on_dirref(
                source,
                &s,
                |s, i| Self::get_col_from_source(s, i),
                |m| Self::err_producer(m),
            ),
            _ => rewrite_err("unsupported reference type".to_string()),
        }
    }

    fn transform_fun(
        &self,
        tctx: &TraversalContext,
        source: &Source,
        f: &expression::ScalarFunction,
    ) -> Result<String, RewriteTargetErr> {
        let fun = tctx.get_fun_from_ext(f)?;
        let args = self.get_fun_args(tctx, source, &f.arguments)?;
        connect_expr(fun, args)
    }

    fn transform_list(
        &self,
        tctx: &TraversalContext,
        source: &Source,
        l: &expression::SingularOrList,
    ) -> Result<String, RewriteTargetErr> {
        let field = match &l.value {
            Some(x) => self.transform_expr(&*x, source, tctx),
            None => rewrite_err("no field in list expression".to_string()),
        }?;

        let mut os = Vec::new();
        for option in &l.options {
            os.push(self.transform_expr(option, source, tctx)?);
        }
        Ok(format!("{} in ({})", field, os.join(", ")))
    }
}

fn connect_expr(fun: Ext, args: Vec<String>) -> RewriteTargetResult<String> {
    match fun {
        Ext::GT if args.len() >= 2 => Ok(format!("{} > {}", args[0], args[1])),
        Ext::LT if args.len() >= 2 => Ok(format!("{} < {}", args[0], args[1])),
        Ext::EQ if args.len() >= 2 => Ok(format!("{} = {}", args[0], args[1])),
        Ext::NE if args.len() >= 2 => Ok(format!("{} != {}", args[0], args[1])),
        Ext::GE if args.len() >= 2 => Ok(format!("{} >= {}", args[0], args[1])),
        Ext::LE if args.len() >= 2 => Ok(format!("{} <= {}", args[0], args[1])),
        Ext::And if args.len() >= 2 => Ok(format!("({} and {})", args[0], args[1])),
        Ext::Or if args.len() >= 2 => Ok(format!("({} or {})", args[0], args[1])),
        Ext::Add if args.len() >= 2 => Ok(format!("({} + {})", args[0], args[1])),
        Ext::Sub if args.len() >= 2 => Ok(format!("({} - {})", args[0], args[1])),
        Ext::Mul if args.len() >= 2 => Ok(format!("({} * {})", args[0], args[1])),
        Ext::Div if args.len() >= 2 => Ok(format!("({} / {})", args[0], args[1])),
        Ext::Not if args.len() >= 1 => Ok(format!("NOT({})", args[0])),
        Ext::Unknown => Ok("".to_string()),
        unknown => rewrite_err(format!(
            "invalid or unknown function {:?} with {} args",
            unknown,
            args.len()
        )),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::ops::Range;
    use substrait::proto::Plan;

    static JSON_PLAN_BLOCK_COLS: &str =
        include_str!("../../../resources/block_plain_with_cols.json");
    static JSON_PLAN_BLOCK_SORT: &str =
        include_str!("../../../resources/block_simple_with_sort.json");
    static JSON_PLAN_BLOCK_OR: &str = include_str!("../../../resources/block_plain_with_or.json");
    static JSON_PLAN_BLOCK_NOT: &str = include_str!("../../../resources/block_plain_with_not.json");
    static JSON_PLAN_BLOCK_IN: &str = include_str!("../../../resources/block_plain_with_in.json");
    static JSON_PLAN_BLOCK_COOL_JOIN: &str =
        include_str!("../../../resources/block_simple_join.json");
    static JSON_PLAN_BLOCK_SQD_JOIN: &str =
        include_str!("../../../resources/block_remote_join2.json");
    static JSON_BLOCK_SQD_AND_LOCAL_JOIN: &str =
        include_str!("../../../resources/block_tx_local_join.json");
    static JSON_BLOCK_SQD_AND_LOCAL_JOIN2: &str =
        include_str!("../../../resources/block_tx_local_join2.json");
    static JSON_BLOCK_TX_JOIN_WITH_OR: &str =
        include_str!("../../../resources/block_tx_join_with_or.json");
    static JSON_BLOCK_TX_JOIN_WITH_NOT: &str =
        include_str!("../../../resources/block_tx_join_with_not.json");
    static JSON_BLOCK_TX_JOIN_WITH_WHERE_ON_TX: &str =
        include_str!("../../../resources/block_tx_join_with_where_on_tx.json");

    fn make_block_example_with_cols() -> Plan {
        serde_json::from_str(JSON_PLAN_BLOCK_COLS).unwrap()
    }

    fn make_block_example_with_sort() -> Plan {
        serde_json::from_str(JSON_PLAN_BLOCK_SORT).unwrap()
    }

    fn make_block_example_with_or() -> Plan {
        serde_json::from_str(JSON_PLAN_BLOCK_OR).unwrap()
    }

    fn make_block_example_with_not() -> Plan {
        serde_json::from_str(JSON_PLAN_BLOCK_NOT).unwrap()
    }

    fn make_block_example_with_in() -> Plan {
        serde_json::from_str(JSON_PLAN_BLOCK_IN).unwrap()
    }

    fn make_block_join_local_example() -> Plan {
        serde_json::from_str(JSON_PLAN_BLOCK_COOL_JOIN).unwrap()
    }

    fn make_block_join_remote_example() -> Plan {
        serde_json::from_str(JSON_PLAN_BLOCK_SQD_JOIN).unwrap()
    }

    fn make_block_tx_and_local_join_example() -> Plan {
        serde_json::from_str(JSON_BLOCK_SQD_AND_LOCAL_JOIN).unwrap()
    }

    fn make_block_tx_and_local_join2_example() -> Plan {
        serde_json::from_str(JSON_BLOCK_SQD_AND_LOCAL_JOIN2).unwrap()
    }

    fn make_block_tx_join_with_or_example() -> Plan {
        serde_json::from_str(JSON_BLOCK_TX_JOIN_WITH_OR).unwrap()
    }

    fn make_block_tx_join_with_not_example() -> Plan {
        serde_json::from_str(JSON_BLOCK_TX_JOIN_WITH_NOT).unwrap()
    }

    fn make_block_tx_join_with_where_on_tx() -> Plan {
        serde_json::from_str(JSON_BLOCK_TX_JOIN_WITH_WHERE_ON_TX).unwrap()
    }

    #[test]
    fn test_plan_with_simple_block() {
        let p = make_block_example_with_cols();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &mut tctx).expect("cannot compile target");

            assert_eq!(
                have,
                format!(
                    "select {} from {}.solana_mainnet.block where {}",
                    "\"number\", \"timestamp\"",
                    SQD_ID,
                    "(\"number\" >= 217710084 and \"number\" <= 217710086)",
                ),
            );

            assert_eq!(
                src.blocks,
                &[FieldRange::BlockNumber(Range {
                    start: 217710084,
                    end: 217710087,
                })]
            );
        }
    }

    #[test]
    fn test_plan_with_sorted_block() {
        let p = make_block_example_with_sort();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &mut tctx).expect("cannot compile target");

            assert_eq!(
                have,
                format!(
                    "select {} from {}.solana_mainnet.block where {}",
                    "\"number\", \"hash\", \"timestamp\"",
                    SQD_ID,
                    "(\"number\" >= 217710084 and \"number\" <= 217710086)",
                )
            );

            assert_eq!(
                src.blocks,
                &[FieldRange::BlockNumber(Range {
                    start: 217710084,
                    end: 217710087,
                })]
            );
        }
    }

    #[test]
    fn test_plan_block_with_or() {
        let p = make_block_example_with_or();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &mut tctx).expect("cannot compile target");

            assert_eq!(
                have,
                format!(
                    "select {} from {}.solana_mainnet.block where {} and {}",
                    "\"number\", \"height\", \"parent_number\", \"timestamp\"",
                    SQD_ID,
                    "((\"number\" >= 217710084 and \"number\" <= 217710086)",
                    "(\"height\" > 1 or \"parent_number\" <= \"number\"))",
                ),
            );

            assert_eq!(
                src.blocks,
                &[FieldRange::BlockNumber(Range {
                    start: 217710084,
                    end: 217710087,
                })]
            );
        }
    }

    #[test]
    fn test_plan_block_with_not() {
        let p = make_block_example_with_not();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &mut tctx).expect("cannot compile target");

            assert_eq!(
                have,
                format!(
                    "select {} from {}.solana_mainnet.block where {} and {}",
                    "\"number\", \"height\", \"parent_number\", \"timestamp\"",
                    SQD_ID,
                    "((\"number\" >= 217710084 and \"number\" <= 217710086)",
                    "NOT((\"height\" < 1 and \"parent_number\" < \"number\")))",
                ),
            );

            assert_eq!(
                src.blocks,
                &[FieldRange::BlockNumber(Range {
                    start: 217710084,
                    end: 217710087,
                })]
            );
        }
    }

    #[test]
    fn test_plan_block_with_in() {
        let p = make_block_example_with_in();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &mut tctx).expect("cannot compile target");

            assert_eq!(
                have,
                format!(
                    "select \"number\", \"timestamp\" from {}.solana_mainnet.block where {}",
                    SQD_ID, "\"number\" in (217710084, 217710085, 217710086)",
                ),
            );

            assert_eq!(
                src.blocks,
                &[FieldRange::BlockNumber(Range {
                    start: 217710084,
                    end: 217710087,
                })]
            );
        }
    }

    #[test]
    fn test_plan_with_cool_join() {
        let p = make_block_join_local_example();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &mut tctx).expect("cannot compile target");

            assert_eq!(
                have,
                format!(
                    "select {} from {}.solana_mainnet.block where {}",
                    "\"number\", \"timestamp\"",
                    SQD_ID,
                    "(\"number\" >= 217710084 and \"number\" <= 217710086)",
                ),
            );

            assert_eq!(
                src.blocks,
                &[FieldRange::BlockNumber(Range {
                    start: 217710084,
                    end: 217710087,
                })]
            );
        }
    }

    #[test]
    fn test_plan_with_sqd_join() {
        let p = make_block_join_remote_example();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            let have = compile_sql(&src, &mut tctx).expect("cannot compile target");

            assert_eq!(
                have,
                if src.table_name == "block" {
                    format!(
                        "select {} from {}.solana_mainnet.block where {}",
                        "\"number\", \"height\"",
                        SQD_ID,
                        "(\"number\" >= 217710084 and \"number\" <= 217710086)",
                    )
                } else {
                    format!(
                        "select {} from {}.solana_mainnet.transactions where ({} and {})",
                        "\"block_number\", \"transaction_index\"",
                        SQD_ID,
                        "\"transaction_index\" < 1000",
                        "(\"block_number\" >= 217710084 and \"block_number\" <= 217710086)",
                    )
                }
            );

            assert_eq!(
                src.blocks,
                if src.table_name == "block" {
                    vec![FieldRange::BlockNumber(Range {
                        start: 217710084,
                        end: 217710087,
                    })]
                } else {
                    vec![]
                }
            );
        }
    }

    #[test]
    fn test_plan_with_sqd_and_local_join() {
        let p = make_block_tx_and_local_join_example();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &mut tctx).expect("cannot compile target");

            assert_eq!(
                have,
                if src.table_name == "block" {
                    format!(
                        "select \"number\", \"height\" from {}.solana_mainnet.block where \"height\" > 1",
                        SQD_ID,
                    )
                } else {
                    format!(
                        "select {} from {}.solana_mainnet.transactions where {}",
                        "\"block_number\", \"transaction_index\"",
                        SQD_ID,
                        "\"transaction_index\" < 10",
                    )
                },
            );

            assert_eq!(src.blocks, vec![]);
        }
    }

    #[test]
    fn test_plan_with_sqd_and_local_join2() {
        let p = make_block_tx_and_local_join2_example();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &mut tctx).expect("cannot compile target");

            assert_eq!(
                have,
                if src.table_name == "block" {
                    format!(
                        "select \"number\", \"height\" from {}.solana_mainnet.block where \"height\" > 1",
                        SQD_ID,
                    )
                } else {
                    format!(
                        "select {} from {}.solana_mainnet.transactions where {}",
                        "\"block_number\", \"transaction_index\"",
                        SQD_ID,
                        "\"transaction_index\" < 10",
                    )
                },
            );

            assert_eq!(src.blocks, vec![]);
        }
    }

    #[test]
    fn test_plan_block_tx_join_with_or() {
        let p = make_block_tx_join_with_or_example();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &tctx).expect("cannot compile target");

            assert_eq!(
                have,
                if src.table_name == "block" {
                    format!(
                        "select \"number\", \"height\", \"parent_number\" from {}.solana_mainnet.block ",
                        SQD_ID,
                    )
                } else {
                    format!(
                        "select {} from {}.solana_mainnet.transactions where {}",
                        "\"block_number\", \"transaction_index\"",
                        SQD_ID,
                        "\"transaction_index\" < 10",
                    )
                },
            );

            assert_eq!(src.blocks, vec![]);
        }
    }

    #[test]
    fn test_plan_block_tx_join_with_not() {
        let p = make_block_tx_join_with_not_example();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &tctx).expect("cannot compile target");

            assert_eq!(
                have,
                if src.table_name == "block" {
                    format!(
                        "select \"number\", \"height\", \"parent_number\" from {}.solana_mainnet.block ",
                        SQD_ID,
                    )
                } else {
                    format!(
                        "select {} from {}.solana_mainnet.transactions where {}",
                        "\"block_number\", \"transaction_index\"",
                        SQD_ID,
                        "\"transaction_index\" < 10",
                    )
                }
            );

            assert_eq!(src.blocks, vec![]);
        }
    }

    #[test]
    fn test_plan_block_tx_join_with_where_on_tx() {
        let p = make_block_tx_join_with_where_on_tx();
        let mut tctx = TraversalContext::new(Default::default());
        let target =
            traverse_plan::<RewriteTarget>(&p, &mut tctx).expect("plan resulted in an error");
        let mut sources = target.get_sources();
        target
            .pushdown_filters(&mut tctx, &mut sources)
            .expect("cannot pushdown filters");
        for src in sources {
            if !src.sqd {
                continue;
            }
            let have = compile_sql(&src, &tctx).expect("cannot compile target");

            assert_eq!(
                have,
                if src.table_name == "block" {
                    format!(
                        "select \"number\", \"timestamp\", \"height\" from {}.solana_mainnet.block where {}",
                        SQD_ID, "(\"number\" >= 217710084 and \"number\" <= 217710086)",
                    )
                } else {
                    format!(
                        "select {} from {}.solana_mainnet.transactions where {} and {}",
                        "\"block_number\", \"transaction_index\", \"fee\"",
                        SQD_ID,
                        "((\"block_number\" >= 217710084 and \"transaction_index\" < 10)",
                        "\"block_number\" <= 217710086)",
                    )
                }
            );

            assert_eq!(
                src.blocks,
                if src.table_name == "transactions" {
                    vec![FieldRange::BlockNumber(Range {
                        start: 217710084,
                        end: 217710087,
                    })]
                } else {
                    vec![]
                }
            );
        }
    }
}
