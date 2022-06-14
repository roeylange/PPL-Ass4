/*
;; TExp AST
;; ========
;; Type checking language
;; Syntax with optional type annotations for var declarations and function return types.

;; Type language
;; <texp>         ::= <atomic-te> | <compound-te> | <tvar> | <user-defined-name-te> // L51
;; <atomic-te>    ::= <num-te> | <bool-te> | <void-te> | <str-te>
;; <num-te>       ::= number   // num-te()
;; <bool-te>      ::= boolean  // bool-te()
;; <str-te>       ::= string   // str-te()
;; <void-te>      ::= void     // void-te()
;; <compound-te>  ::= <proc-te> | <tuple-te> | <user-defined-te>  // L51
;; <non-tuple-te> ::= <atomic-te> | <proc-te> | <tvar>
;; <proc-te>      ::= [ <tuple-te> -> <non-tuple-te> ] // proc-te(param-tes: list(te), return-te: te)
;; <tuple-te>     ::= <non-empty-tuple-te> | <empty-te>
;; <non-empty-tuple-te> ::= ( <non-tuple-te> *)* <non-tuple-te> // tuple-te(tes: list(te))
;; <empty-te>     ::= Empty
;; <tvar>         ::= a symbol starting with T // tvar(id: Symbol, contents; Box(string|boolean))
;; L51
;; <user-defined-name-te> ::= a symbol 
;; <field-name> ::= a symbol
;; <field> ::= (<field-name> : <texp>) // field-te(fieldName: string, te: TExp)
;; <record-te> ::= (<user-defined-name-te> <field>*) // record-te(recordName: string, fields: list(field-te))
;; <user-defined-te> ::= <user-defined-name-te> <record-te>+ // user-defined-te(typeName: string, records: list(record-te))

;; Examples of type expressions
;; number
;; boolean
;; void
;; [number -> boolean]
;; [number * number -> boolean]
;; [number -> [number -> boolean]]
;; [Empty -> number]
;; [Empty -> void]
;; L51
;; (define-type Value (Number (val : number)) (Boolean (val : boolean)) (String (val : string)))
;; (define-type Env (EmptyEnv) (ExtendedEnv (var : string) (val : Value)))
*/
import { chain, concat, includes, map, uniq, length } from "ramda";
import { Sexp } from "s-expression";
import { isEmpty, second } from "../shared/list";
import { isArray, isBoolean, isString } from '../shared/type-predicates';
import { makeBox, setBox, unbox, Box } from '../shared/box';
import { cons, first, rest } from '../shared/list';
import { Result, bind, makeOk, makeFailure, mapResult, mapv } from "../shared/result";
import { isCompoundSexp, isToken, parse as p } from "../shared/parser";
import { SymbolSExp } from "./L5-value";

export type TExp =  AtomicTExp | CompoundTExp | TVar | UserDefinedNameTExp; // L51
export const isTExp = (x: any): x is TExp => isAtomicTExp(x) || isCompoundTExp(x) || isTVar(x) || isUserDefinedNameTExp(x); // L51

export type AtomicTExp = NumTExp | BoolTExp | StrTExp | VoidTExp | UserDefinedNameTExp | AnyTExp| SymbolTExp |PairTExp  ; // L51
export const isAtomicTExp = (x: any): x is AtomicTExp =>
    isNumTExp(x) || isBoolTExp(x) || isStrTExp(x) || isVoidTExp(x) || isUserDefinedNameTExp(x) || isAnyTExp(x); // L51

export type CompoundTExp = ProcTExp | TupleTExp | UserDefinedTExp | Record;  // L51
export const isCompoundTExp = (x: any): x is CompoundTExp => isProcTExp(x) || isTupleTExp(x) || isUserDefinedTExp(x); 

export type NonTupleTExp = AtomicTExp | ProcTExp | TVar | UserDefinedNameTExp; // L51
export const isNonTupleTExp = (x: any): x is NonTupleTExp =>
    isAtomicTExp(x) || isProcTExp(x) || isTVar(x) || isUserDefinedNameTExp(x);  // L51

// L51<
export type AnyTExp = {tag: "AnyTExp"};
export const makeAnyTExp = (): AnyTExp => ({tag: "AnyTExp"});
export const isAnyTExp = (x: any): x is AnyTExp => x.tag === "AnyTExp";

export type UserDefinedNameTExp = {tag: "UserDefinedNameTExp"; typeName: string};
export const makeUserDefinedNameTExp = (typeName: string): UserDefinedNameTExp =>
    ({tag: "UserDefinedNameTExp", typeName});
export const isUserDefinedNameTExp = (x: any): x is UserDefinedNameTExp => x.tag === "UserDefinedNameTExp";

export type Field = {tag: "Field"; fieldName: string; te: TExp};
export const makeField = (fieldName: string, te: TExp): Field =>
    ({tag: "Field", fieldName, te});
export const isField = (x: any): x is Field => x.tag === "Field";

export type Record = {tag: "Record"; typeName: string; fields: Field[]};
export const makeRecord = (typeName: string, fields: Field[]): Record =>
    ({tag: "Record", typeName, fields});
export const isRecord = (x: any): x is Record => x.tag === "Record";

export type UserDefinedTExp = {tag: "UserDefinedTExp"; typeName: string; records: Record[]};
export const makeUserDefinedTExp = (typeName: string, records: Record[]): UserDefinedTExp =>
    ({tag: "UserDefinedTExp", typeName, records});
export const isUserDefinedTExp = (x: any): x is UserDefinedTExp => x.tag === "UserDefinedTExp";

// A user defined type - either a disjoint union UserDefinedTExp or a Record
export type UDTExp = UserDefinedTExp | Record;
// L51>
export type PairTExp = { tag: "PairTExp" };
export const makePairTExp = (): PairTExp => ({tag: "PairTExp"});
export const isPairTExp = (x: any): x is PairTExp => x.tag === "PairTExp";

export type SymbolTExp = { tag: "SymbolTExp", val?: SymbolSExp };
export const makeSymbolTExp = (s?: SymbolSExp): SymbolTExp => ({tag: "SymbolTExp", val: s});
export const isSymbolTExp = (x: any): x is SymbolTExp => x.tag === "SymbolTExp";

export type NumTExp = { tag: "NumTExp" };
export const makeNumTExp = (): NumTExp => ({tag: "NumTExp"});
export const isNumTExp = (x: any): x is NumTExp => x.tag === "NumTExp";

export type BoolTExp = { tag: "BoolTExp" };
export const makeBoolTExp = (): BoolTExp => ({tag: "BoolTExp"});
export const isBoolTExp = (x: any): x is BoolTExp => x.tag === "BoolTExp";

export type StrTExp = { tag: "StrTExp" };
export const makeStrTExp = (): StrTExp => ({tag: "StrTExp"});
export const isStrTExp = (x: any): x is StrTExp => x.tag === "StrTExp";

export type VoidTExp = { tag: "VoidTExp" };
export const makeVoidTExp = (): VoidTExp => ({tag: "VoidTExp"});
export const isVoidTExp = (x: any): x is VoidTExp => x.tag === "VoidTExp";

// proc-te(param-tes: list(te), return-te: te)
export type ProcTExp = { tag: "ProcTExp"; paramTEs: TExp[]; returnTE: TExp; };
export const makeProcTExp = (paramTEs: TExp[], returnTE: TExp): ProcTExp =>
    ({tag: "ProcTExp", paramTEs: paramTEs, returnTE: returnTE});
export const isProcTExp = (x: any): x is ProcTExp => x.tag === "ProcTExp";
// Uniform access to all components of a ProcTExp
export const procTExpComponents = (pt: ProcTExp): TExp[] =>
    [...pt.paramTEs, pt.returnTE];

export type TupleTExp = NonEmptyTupleTExp | EmptyTupleTExp;
export const isTupleTExp = (x: any): x is TupleTExp =>
    isNonEmptyTupleTExp(x) || isEmptyTupleTExp(x);

export interface EmptyTupleTExp { tag: "EmptyTupleTExp" }
export const makeEmptyTupleTExp = (): EmptyTupleTExp => ({tag: "EmptyTupleTExp"});
export const isEmptyTupleTExp = (x: any): x is EmptyTupleTExp => x.tag === "EmptyTupleTExp";

// NonEmptyTupleTExp(TEs: NonTupleTExp[])
export interface NonEmptyTupleTExp { tag: "NonEmptyTupleTExp"; TEs: NonTupleTExp[]; }
export const makeNonEmptyTupleTExp = (tes: NonTupleTExp[]): NonEmptyTupleTExp =>
    ({tag: "NonEmptyTupleTExp", TEs: tes});
export const isNonEmptyTupleTExp = (x: any): x is NonEmptyTupleTExp => x.tag === "NonEmptyTupleTExp";

// TVar: Type Variable with support for dereferencing (TVar -> TVar)
export type TVar = { tag: "TVar"; var: string; contents: Box<undefined | TExp>; };
export const isEmptyTVar = (x: any): x is TVar =>
    (x.tag === "TVar") && unbox(x.contents) === undefined;
export const makeTVar = (v: string): TVar =>
    ({tag: "TVar", var: v, contents: makeBox(undefined)});
const makeTVarGen = (): () => TVar => {
    let count: number = 0;
    return () => {
        count++;
        return makeTVar(`T_${count}`);
    }
}
export const makeFreshTVar = makeTVarGen();
export const isTVar = (x: any): x is TVar => x.tag === "TVar";
export const eqTVar = (tv1: TVar, tv2: TVar): boolean => tv1.var === tv2.var;
export const tvarContents = (tv: TVar): undefined | TExp => unbox(tv.contents);
export const tvarSetContents = (tv: TVar, val: TExp): void =>
    setBox(tv.contents, val);
export const tvarIsNonEmpty = (tv: TVar): boolean => tvarContents(tv) !== undefined;
export const tvarDeref = (te: TExp): TExp => {
    if (! isTVar(te)) return te;
    const contents = tvarContents(te);
    if (contents === undefined)
        return te;
    else if (isTVar(contents))
        return tvarDeref(contents);
    else
        return contents;
}

// ========================================================
// TExp Utilities

// Purpose: uniform access to atomic types
export const atomicTExpName = (te: AtomicTExp): string => // L51
    isUserDefinedNameTExp(te) ? te.typeName : te.tag;

export const eqAtomicTExp = (te1: AtomicTExp, te2: AtomicTExp): boolean =>
    atomicTExpName(te1) === atomicTExpName(te2);


// ========================================================
// TExp parser

export const parseTE = (t: string): Result<TExp> =>
    bind(p(t), (te: Sexp) => 
        parseTExp(te, []));


/*
;; Distinguish between user-defined types and TVar identifiers
;; - belongs to the defined udTypeNames
;; - starts with T 
*/
export const isConcreteTVar = (s: Sexp): boolean =>
    isString(s) && s.length > 1 && s[0] === 'T';

export const isConcreteUserDefinedTypeName = (s: Sexp, udTypeNames: string[]): boolean =>
    isString(s) && udTypeNames.includes(s);

 
/*
;; Purpose: Parse a type expression
;; Type: [SExp -> TExp[]]
;; Example:
;; parseTExp("number") => 'num-te
;; parseTExp('boolean') => 'bool-te
;; parseTExp('T1') => '(tvar T1)
;; parseTExp('(T * T -> boolean)') => '(proc-te ((tvar T) (tvar T)) bool-te)
;; parseTExp('(number -> (number -> number)') => '(proc-te (num-te) (proc-te (num-te) num-te))
;; parseTExp('(define-type a (empty))') => '(user-defined-type-te (record-te empty)) 
*/
export const parseTExp = (texp: Sexp, udTypeNames: string[]): Result<TExp> => // L51
    (texp === "number") ? makeOk(makeNumTExp()) :
    (texp === "boolean") ? makeOk(makeBoolTExp()) :
    (texp === "void") ? makeOk(makeVoidTExp()) :
    (texp === "string") ? makeOk(makeStrTExp()) :
    (texp === "any") ? makeOk(makeAnyTExp()) :
    isString(texp) && isConcreteTVar(texp) ? makeOk(makeTVar(texp)) :
    isString(texp) && isConcreteUserDefinedTypeName(texp, udTypeNames) ? makeOk(makeUserDefinedNameTExp(texp)) : // L51
    isString(texp) ? makeFailure(`Expected either TVar or UDType - got ${texp} / ${udTypeNames}`) :
    isCompoundSexp(texp) ? parseCompoundTExp(texp, udTypeNames) :
    makeFailure(`Unexpected TExp - ${JSON.stringify(texp, null, 2)}`);

// L51
// Can be ProcTExp | UserDefinedTExp
// We don't expect a TupleTExp outside of a ProcTExp
// 
const parseCompoundTExp = (texps: Sexp[], udTypeNames: string[]): Result<CompoundTExp> =>
    isEmpty(texps) ? makeFailure('Empty compound type') :
    first(texps) === 'define-type' ? parseUserDefinedType(texps, udTypeNames) :
    parseProcTExp(texps, udTypeNames);

// Expected input: (define-type <typeName> (<recordName> <field>*)+)
// Gather user defined names from the define-type so that they are known before parsing
const parseString = (x: Sexp): Result<string> =>
    isString(x) ? makeOk(x) :
    makeFailure(`Expected string token - got ${x}`);

// Extract the user defined type names from a define-type expression
// (define-type <tn> (<rn> ...) ...) 
// gather tn and rns
export const extractTypeNames = (texps: Sexp): Result<string[]> =>
    isToken(texps) ? makeFailure(`Expected (define-type <tn> (<rn> ...) ...) - got ${texps}`) :
    length(texps) < 3 ? makeFailure(`Expected (define-type <tn> (<rn> ...) ...) - got ${texps}`) :
    bind(parseString(second(texps)), (tn: string) => 
        mapv(mapResult((record: Sexp): Result<string> => 
                    isToken(record) ? makeFailure(`Expected record of the form (rn ...) - got ${record}`) :
                    isEmpty(record) ? makeFailure(`Expected record of the form (rn ...) - got ${record}`) :
                    parseString(first(record)), 
                rest(rest(texps))), (rns: string[]) =>
            cons(tn, rns)));

// Expected: (fieldName : TExp)
const parseField = (field: Sexp, udTypeNames: string[]): Result<Field> =>
    isToken(field) ? makeFailure(`Expected a field of the form (fieldName : TExp) - got ${field}`) :
    length(field) < 3 ? makeFailure(`Expected a field of the form (fieldName : TExp) - got ${field}`) :
    second(field) != ':' ? makeFailure(`Expected a field of the form (fieldName : TExp) - got ${field}`) :
    bind(parseString(first(field)), (fieldName: string) =>
        mapv(parseTExp(field[2], udTypeNames), (te: TExp) =>
            makeField(fieldName, te)));

// Expected: (recordName <field>*)
const parseRecord = (record: Sexp, udTypeNames: string[]): Result<Record> =>
    isToken(record) ? makeFailure(`Expected record to be of the form (recordName <field>*) - got ${record}`) :
    length(record) < 1 ? makeFailure(`Expected record to be of the form (recordName <field>*) - got ${record}`) :
    bind(parseString(first(record)), (typeName: string) =>
        mapv(mapResult((field: Sexp) => parseField(field, udTypeNames), rest(record)), (fields: Field[]) =>
            makeRecord(typeName, fields)));

export const parseUserDefinedType = (texps: Sexp[], udTypeNames: string[]): Result<UserDefinedTExp> =>
    length(texps) < 3 ? makeFailure(`Expected (define-type <typeName> (<recordName> <field>*)+) - got ${texps}`) :
    bind(parseString(second(texps)), (typeName) => 
        bind(extractTypeNames(texps), (newUdTypeNames: string[]) =>
            mapv(mapResult((rec: Sexp) => parseRecord(rec, udTypeNames.concat(newUdTypeNames)), 
                           rest(rest(texps))), (records: Record[]) =>
                    makeUserDefinedTExp(typeName, records))));


/*
;; expected structure: (<params> -> <returnte>)
;; expected exactly one -> in the list
;; We do not accept (a -> b -> c) - must parenthesize
*/
const parseProcTExp = (texps: Sexp[], udTypeNames: string[]): Result<ProcTExp> => {
    const pos = texps.indexOf('->');
    return (pos === -1)  ? makeFailure(`Procedure type expression without -> - ${JSON.stringify(texps, null, 2)}`) :
           (pos === 0) ? makeFailure(`No param types in proc texp - ${JSON.stringify(texps, null, 2)}`) :
           (pos === texps.length - 1) ? makeFailure(`No return type in proc texp - ${JSON.stringify(texps, null, 2)}`) :
           (texps.slice(pos + 1).indexOf('->') > -1) ? makeFailure(`Only one -> allowed in a procexp - ${JSON.stringify(texps, null, 2)}`) :
           bind(parseTupleTExp(texps.slice(0, pos), udTypeNames), (args: TExp[]) =>
               mapv(parseTExp(texps[pos + 1], udTypeNames), (returnTE: TExp) =>
                    makeProcTExp(args, returnTE)));
};

/*
;; Expected structure: <te1> [* <te2> ... * <ten>]?
;; Or: Empty
*/
const parseTupleTExp = (texps: Sexp[], udTypeNames: string[]): Result<TExp[]> => {
    const isEmptyTuple = (texps: Sexp[]): boolean =>
        (length(texps) === 1) && (first(texps) === 'Empty');
    // [x1 * x2 * ... * xn] => [x1,...,xn]
    const splitEvenOdds = (texps: Sexp[]): Result<Sexp[]> =>
        isEmpty(texps) ? makeOk([]) :
        isEmpty(rest(texps)) ? makeOk(texps) :
        texps[1] !== '*' ? makeFailure(`Parameters of procedure type must be separated by '*': ${JSON.stringify(texps, null, 2)}`) :
        mapv(splitEvenOdds(rest(rest(texps))), (sexps: Sexp[]) => 
            cons(first(texps), sexps));

    return isEmptyTuple(texps) ? makeOk([]) : 
           bind(splitEvenOdds(texps), (argTEs: Sexp[]) => 
               mapResult((arg) => parseTExp(arg, udTypeNames), argTEs));
}

/*
;; Purpose: Unparse a type expression Texp into its concrete form
*/
export const unparseTExp = (te: TExp): Result<string> => {

    const unparseTuple = (paramTes: TExp[]): Result<string[]> =>
        isEmpty(paramTes) ? makeOk(["Empty"]) :
        bind(unparseTExp(first(paramTes)), (paramTE: string) =>
            mapv(mapResult(unparseTExp, rest(paramTes)), (paramTEs: string[]) =>
                cons(paramTE, chain(te => ['*', te], paramTEs))));

    const unparseField = (field: Field): Result<string> =>
        mapv(unparseTExp(field.te), (fieldTE: string) =>
            `(${field.fieldName} : ${fieldTE})`);

    const unparseRecord = (record: Record): Result<string> =>
        mapv(mapResult(unparseField, record.fields), (fields: string[]) => 
            `(${record.typeName} ${fields.join(" ")})`);

    const unparseUserDefinedType = (udTExp: UserDefinedTExp): Result<string> =>
        mapv(mapResult(unparseRecord, udTExp.records), (records: string[]) =>
            `(define-type ${udTExp.typeName} ${records.join(" ")})`);

    const up = (x?: TExp): Result<string | string[]> =>
        isNumTExp(x) ? makeOk('number') :
        isBoolTExp(x) ? makeOk('boolean') :
        isStrTExp(x) ? makeOk('string') :
        isVoidTExp(x) ? makeOk('void') :
        isAnyTExp(x) ? makeOk('any') :
        isEmptyTVar(x) ? makeOk(x.var) :
        isUserDefinedNameTExp(x) ? makeOk(x.typeName) :
        isPairTExp(x) ? makeOk('cons') : 
        isSymbolTExp(x) ? x.val ? makeOk(`symbol-${x.val.val}`) : makeOk('symbol') :
        isTVar(x) ? up(tvarContents(x)) :
        isUserDefinedTExp(x) ? unparseUserDefinedType(x) :
        isRecord(x) ? unparseRecord(x) :
        isProcTExp(x) ? bind(unparseTuple(x.paramTEs), (paramTEs: string[]) =>
                            mapv(unparseTExp(x.returnTE), (returnTE: string) =>
                                [...paramTEs, '->', returnTE])) :
        isEmptyTupleTExp(x) ? makeOk("Empty") :
        isNonEmptyTupleTExp(x) ? unparseTuple(x.TEs) :
        x === undefined ? makeFailure("Undefined TVar") :
        x;

    return mapv(up(te), (x: string | string[]) => 
        isString(x) ? x :
        isArray(x) ? `(${x.join(' ')})` :
        x);
}

// ============================================================
// equivalentTEs: 2 TEs are equivalent up to variable renaming.
// For example:
// equivalentTEs(parseTExp('(T1 -> T2)'), parseTExp('(T3 -> T4)'))


// Signature: matchTVarsInTE(te1, te2, succ, fail)
// Type: [Texp * Texp * [List(Pair(Tvar, Tvar)) -> T1] * [Empty -> T2]] |
//       [List(Texp) * List(Texp) * ...]
// Purpose:   Receives two type expressions or list(texps) plus continuation procedures
//            and, in case they are equivalent, pass a mapping between
//            type variable they include to succ. Otherwise, invoke fail.
// Examples:
// matchTVarsInTE(parseTExp('(Number * T1 -> T1)',
//                parseTExp('(Number * T7 -> T5)'),
//                (x) => x,
//                () => false) ==> [[T1, T7], [T1, T5]]
// matchTVarsInTE(parseTExp('(Boolean * T1 -> T1)'),
//                parseTExp('(Number * T7 -> T5)'),
//                (x) => x,
//                () => false)) ==> false

type Pair<T1, T2> = {left: T1; right: T2};

const matchTVarsInTE = <T1, T2>(te1: TExp, te2: TExp,
                                succ: (mapping: Array<Pair<TVar, TVar>>) => T1,
                                fail: () => T2): T1 | T2 =>
    (isTVar(te1) || isTVar(te2)) ? matchTVarsinTVars(tvarDeref(te1), tvarDeref(te2), succ, fail) :
    (isAtomicTExp(te1) || isAtomicTExp(te2)) ?
        ((isAtomicTExp(te1) && isAtomicTExp(te2) && eqAtomicTExp(te1, te2)) ? succ([]) : fail()) :
    matchTVarsInTProcs(te1, te2, succ, fail);

// te1 and te2 are the result of tvarDeref
const matchTVarsinTVars = <T1, T2>(te1: TExp, te2: TExp,
                                    succ: (mapping: Array<Pair<TVar, TVar>>) => T1,
                                    fail: () => T2): T1 | T2 =>
    (isTVar(te1) && isTVar(te2)) ? (eqTVar(te1, te2) ? succ([]) : succ([{left: te1, right: te2}])) :
    (isTVar(te1) || isTVar(te2)) ? fail() :
    matchTVarsInTE(te1, te2, succ, fail);

const matchTVarsInTProcs = <T1, T2>(te1: TExp, te2: TExp,
        succ: (mapping: Array<Pair<TVar, TVar>>) => T1,
        fail: () => T2): T1 | T2 =>
    (isProcTExp(te1) && isProcTExp(te2)) ? matchTVarsInTEs(procTExpComponents(te1), procTExpComponents(te2), succ, fail) :
    fail();

const matchTVarsInTEs = <T1, T2>(te1: TExp[], te2: TExp[],
                                    succ: (mapping: Array<Pair<TVar, TVar>>) => T1,
                                    fail: () => T2): T1 | T2 =>
    (isEmpty(te1) && isEmpty(te2)) ? succ([]) :
    (isEmpty(te1) || isEmpty(te2)) ? fail() :
    // Match first then continue on rest
    matchTVarsInTE(first(te1), first(te2),
                    (subFirst) => matchTVarsInTEs(rest(te1), rest(te2), 
                                        (subRest) => succ(subFirst.concat(subRest)), 
                                        fail),
                    fail);

// Signature: equivalent-tes?(te1, te2)
// Purpose:   Check whether 2 type expressions are equivalent up to
//            type variable renaming.
// Example:  equivalentTEs(parseTExp('(T1 * (Number -> T2) -> T3))',
//                         parseTExp('(T4 * (Number -> T5) -> T6))') => #t
export const equivalentTEs = (te1: TExp, te2: TExp): boolean => {
    // console.log(`EqTEs ${JSON.stringify(te1)} - ${JSON.stringify(te2)}`);
    const tvarsPairs = matchTVarsInTE(te1, te2, (x) => x, () => false);
    // console.log(`EqTEs pairs = ${map(JSON.stringify, tvarsPairs)}`)
    if (isBoolean(tvarsPairs))
        return false;
    else {
        return (uniq(map((p) => p.left.var, tvarsPairs)).length === uniq(map((p) => p.right.var, tvarsPairs)).length);
    }
};
