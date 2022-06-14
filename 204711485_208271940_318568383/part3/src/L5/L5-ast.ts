// ===========================================================
// AST type models for L5
// L5 extends L4 with:
// optional type annotations

import { flatten, includes, join, map, union, zipWith } from "ramda";
import { CompoundSexp, Sexp, Token } from 's-expression';
import { isCompoundSExp, isEmptySExp, isSymbolSExp, makeCompoundSExp, makeEmptySExp, makeSymbolSExp, SExpValue, valueToString } from './L5-value';
import { isTVar, makeFreshTVar, parseTExp, unparseTExp, TExp, extractTypeNames, UserDefinedTExp, parseUserDefinedType } from './TExp';
import { allT, first, rest, second, isEmpty, cons } from '../shared/list';
import { parse as p, isToken, isSexpString, isCompoundSexp } from "../shared/parser";
import { Result, bind, makeFailure, mapResult, makeOk, mapv } from "../shared/result";
import { isArray, isString, isNumericString, isIdentifier } from "../shared/type-predicates";

/*
// =============================================================================
// Examples of type annotated programs
// (define [x : number] 5)
// (define [f : [number -> number]] (lambda ([x : number]) : number (* x x))
// (define [f : [number * number -> number]] (lambda ([x : number] [y : number]) : number (* x x))
// (define f (lambda ([x : number]) (* x x)))  // no type annotation on f and on return value of lambda
// (let (([a : number] 1)
//       ([b : boolean] #t))
//   (if b a (+ a 1)))
// (define [id : (T1 -> T1)] (lambda ([x : T1]) : T1 x))
;;
// The only changes in the syntax of L5 are optional type annotations in var-decl and proc-exp
;;
// <program> ::= (L5 <exp>+)                   / Program(exps:List(exp))
// <exp> ::= <define> | <cexp> | <define-type> / DefExp | CExp | DefTypeExp     ##### L51
// <define> ::= ( define <var-decl> <cexp> )   / DefExp(var:VarDecl, val:CExp)
// <var> ::= <identifier>                      / VarRef(var:string)
// <cexp> ::= <number>                         / NumExp(val:number)
//         |  <boolean>                        / BoolExp(val:boolean)
//         |  <string>                         / StrExp(val:string)
//         |  <var-ref>
//         |  ( lambda ( <var-decl>* ) <TExp>? <cexp>+ ) / ProcExp(args:VarDecl[], body:CExp[], returnTE: TExp))
//         |  ( if <cexp> <cexp> <cexp> )      / IfExp(test: CExp, then: CExp, alt: CExp)
//         |  ( quote <sexp> )                 / LitExp(val:SExp)
//         |  ( <cexp> <cexp>* )               / AppExp(operator:CExp, operands:CExp[]))
//         |  ( let ( <binding>* ) <cexp>+ )   / LetExp(bindings:Binding[], body:CExp[]))
//         |  ( letrec ( binding*) <cexp>+ )   / LetrecExp(bindings:Bindings[], body: CExp)
//         |  ( set! <var> <cexp>)             / SetExp(var: varRef, val: CExp)
//         |  ( type-case <id> <CExp> ( <case-exp> )+  ) / TypeCaseExp(typeName: string, val: CExp, cases: CaseExp[]) #### L51
// <binding>  ::= ( <var> <cexp> )             / Binding(var:VarDecl, val:Cexp)
// <prim-op>  ::= + | - | * | / | < | > | = | not |  eq? | string=?
//                  | cons | car | cdr | list? | number?
//                  | boolean? | symbol? | string?
//                  | display | newline
// <num-exp>  ::= a number token
// <bool-exp> ::= #t | #f
// <var-ref>  ::= an identifier token          / VarRef(var)
// <var-decl> ::= an identifier token | (var : TExp) / VarRef(var, TE: TExp) ##### L5
// <sexp>     ::= symbol | number | bool | string | ( <sexp>* )              ##### L3
// ##### L51
// <define-type> ::= ( define-type <id> [ ( <id> <VarDecl>* ) ]* ) / DefTypeExp(typeName:string, records:Record[])
//                                 / Record(typeName:string, fields:VarDecl[])
// <case-exp> ::= (id (<varDecl>*) <cexp>+) | CaseExp(typeName: string, varDecls: VarDecl[], body: CExp[])
*/

// A value returned by parseL5
export type Parsed = Exp | Program;

export type Exp = DefineExp | CExp | DefineTypeExp; // L51
export const isExp = (x: any): x is Exp => isDefineExp(x) || isCExp(x) || isDefineTypeExp(x); // L51

export type CExp =  AtomicExp | CompoundExp;
export const isCExp = (x: any): x is CExp => isAtomicExp(x) || isCompoundExp(x);

export type AtomicExp = NumExp | BoolExp | StrExp | PrimOp | VarRef;
export const isAtomicExp = (x: any): x is AtomicExp =>
    isNumExp(x) || isBoolExp(x) || isStrExp(x) ||
    isPrimOp(x) || isVarRef(x);

export type CompoundExp = AppExp | IfExp | ProcExp | LetExp | LitExp | LetrecExp | SetExp | TypeCaseExp;  // L51
export const isCompoundExp = (x: any): x is CompoundExp =>
    isAppExp(x) || isIfExp(x) || isProcExp(x) || isLitExp(x) || isLetExp(x) || isLetrecExp(x) || isSetExp(x) || isTypeCaseExp(x); // L51
export const expComponents = (e: Exp): CExp[] =>
    isIfExp(e) ? [e.test, e.then, e.alt] :
    isProcExp(e) ? e.body :
    isLetExp(e) ? [...e.body, ...map((b) => b.val, e.bindings)] :
    isLetrecExp(e) ? [...e.body, ...map((b) => b.val, e.bindings)] :
    isAppExp(e) ? [e.rator, ...e.rands] :
    isSetExp(e) ? [e.val] :
    isDefineExp(e) ? [e.val] :
    isDefineTypeExp(e) ? [] : // L51
    isTypeCaseExp(e) ?  flatten([e.val, ...map((c) => c.body, e.cases)]) :  // L51
    []; // Atomic expressions have no components

// Type definitions
export interface Program {tag: "Program"; exps: Exp[]; }
export const makeProgram = (exps: Exp[]): Program => ({tag: "Program", exps: exps});
export const isProgram = (x: any): x is Program => x.tag === "Program";

export interface DefineExp {tag: "DefineExp"; var: VarDecl; val: CExp; }
export const makeDefineExp = (v: VarDecl, val: CExp): DefineExp =>
    ({tag: "DefineExp", var: v, val: val});
export const isDefineExp = (x: any): x is DefineExp => x.tag === "DefineExp";

// L51
export interface DefineTypeExp {tag: "DefineTypeExp"; typeName: string; udType: UserDefinedTExp};
export const makeDefineTypeExp = (typeName: string, udType: UserDefinedTExp): DefineTypeExp => ({tag: "DefineTypeExp", typeName, udType});
export const isDefineTypeExp = (x: any): x is DefineTypeExp => x.tag === "DefineTypeExp";

export interface NumExp {tag: "NumExp"; val: number; }
export const makeNumExp = (n: number): NumExp => ({tag: "NumExp", val: n});
export const isNumExp = (x: any): x is NumExp => x.tag === "NumExp";

export interface BoolExp {tag: "BoolExp"; val: boolean; }
export const makeBoolExp = (b: boolean): BoolExp => ({tag: "BoolExp", val: b});
export const isBoolExp = (x: any): x is BoolExp => x.tag === "BoolExp";

export interface StrExp {tag: "StrExp"; val: string; }
export const makeStrExp = (s: string): StrExp => ({tag: "StrExp", val: s});
export const isStrExp = (x: any): x is StrExp => x.tag === "StrExp";

export interface PrimOp {tag: "PrimOp"; op: PrimOpKeyword; }
export const makePrimOp = (op: PrimOpKeyword): PrimOp => ({tag: "PrimOp", op: op});
export const isPrimOp = (x: any): x is PrimOp => x.tag === "PrimOp";

export interface VarRef {tag: "VarRef"; var: string; }
export const makeVarRef = (v: string): VarRef => ({tag: "VarRef", var: v});
export const isVarRef = (x: any): x is VarRef => x.tag === "VarRef";

export interface VarDecl {tag: "VarDecl"; var: string; texp: TExp}
export const makeVarDecl = (v: string, te: TExp): VarDecl => ({tag: "VarDecl", var: v, texp: te});
export const isVarDecl = (x: any): x is VarDecl => x.tag === "VarDecl";

export interface AppExp {tag: "AppExp"; rator: CExp; rands: CExp[]; }
export const makeAppExp = (rator: CExp, rands: CExp[]): AppExp =>
    ({tag: "AppExp", rator: rator, rands: rands});
export const isAppExp = (x: any): x is AppExp => x.tag === "AppExp";

export interface IfExp {tag: "IfExp"; test: CExp; then: CExp; alt: CExp; }
export const makeIfExp = (test: CExp, then: CExp, alt: CExp): IfExp =>
    ({tag: "IfExp", test: test, then: then, alt: alt});
export const isIfExp = (x: any): x is IfExp => x.tag === "IfExp";

export interface ProcExp {tag: "ProcExp"; args: VarDecl[], body: CExp[]; returnTE: TExp }
export const makeProcExp = (args: VarDecl[], body: CExp[], returnTE: TExp): ProcExp =>
    ({tag: "ProcExp", args: args, body: body, returnTE: returnTE});
export const isProcExp = (x: any): x is ProcExp => x.tag === "ProcExp";

export interface Binding {tag: "Binding"; var: VarDecl; val: CExp; }
export const makeBinding = (v: VarDecl, val: CExp): Binding =>
    ({tag: "Binding", var: v, val: val});
export const isBinding = (x: any): x is Binding => x.tag === "Binding";

export interface LetExp {tag: "LetExp"; bindings: Binding[]; body: CExp[]; }
export const makeLetExp = (bindings: Binding[], body: CExp[]): LetExp =>
    ({tag: "LetExp", bindings: bindings, body: body});
export const isLetExp = (x: any): x is LetExp => x.tag === "LetExp";

export interface LitExp {tag: "LitExp"; val: SExpValue; }
export const makeLitExp = (val: SExpValue): LitExp => ({tag: "LitExp", val: val});
export const isLitExp = (x: any): x is LitExp => x.tag === "LitExp";

export interface LetrecExp {tag: "LetrecExp"; bindings: Binding[]; body: CExp[]; }
export const makeLetrecExp = (bindings: Binding[], body: CExp[]): LetrecExp =>
    ({tag: "LetrecExp", bindings: bindings, body: body});
export const isLetrecExp = (x: any): x is LetrecExp => x.tag === "LetrecExp";

export interface SetExp {tag: "SetExp"; var: VarRef; val: CExp; }
export const makeSetExp = (v: VarRef, val: CExp): SetExp =>
    ({tag: "SetExp", var: v, val: val});
export const isSetExp = (x: any): x is SetExp => x.tag === "SetExp";

// L51
export interface TypeCaseExp {tag: "TypeCaseExp", typeName: string, val: CExp, cases: CaseExp[]};
export interface CaseExp {tag: "CaseExp", typeName: string, varDecls: VarDecl[], body: CExp[]};
export const makeTypeCaseExp = (typeName: string, val: CExp, cases: CaseExp[]): TypeCaseExp => ({tag:"TypeCaseExp", typeName, val, cases});
export const makeCaseExp = (typeName: string, varDecls: VarDecl[], body: CExp[]): CaseExp => ({tag:"CaseExp", typeName, varDecls, body});
export const isTypeCaseExp = (x: any): x is TypeCaseExp => x.tag === "TypeCaseExp";
export const isCaseExp = (x: any): x is CaseExp => x.tag === "CaseExp";

// To help parser - define a type for reserved key words.
export type SpecialFormKeyword = "lambda" | "let" | "letrec" | "if" | "set!" | "quote" | "type-case";  // L51
const isSpecialFormKeyword = (x: string): x is SpecialFormKeyword =>
    ["if", "lambda", "let", "quote", "letrec", "set!", "type-case"].includes(x);  // L51

/*
    ;; <prim-op>  ::= + | - | * | / | < | > | = | not | and | or | eq? | string=?
    ;;                  | cons | car | cdr | pair? | number? | list
    ;;                  | boolean? | symbol? | string?      ##### L3
*/
export type PrimOpKeyword = "+" | "-" | "*" | "/" | ">" | "<" | "=" | "not" | "and" | "or" | "eq?" | "string=?" | 
        "cons" | "car" | "cdr" | "list" | "pair?" | "list?" | "number?" | "boolean?" | "symbol?" | "string?" |
        "display" | "newline";
const isPrimOpKeyword = (x: string): x is PrimOpKeyword =>
    ["+", "-", "*", "/", ">", "<", "=", "not", "and", "or", 
     "eq?", "string=?", "cons", "car", "cdr", "list", "pair?",
     "list?", "number?", "boolean?", "symbol?", "string?", "display", "newline"].includes(x);

// ========================================================
// Parsing

export const parseL51 = (x: string): Result<Program> =>
    bind(p(x), (s: Sexp) => parseL51Program(s));

export const parseL51Program = (sexp: Sexp): Result<Program> =>
    sexp === "" || isEmpty(sexp) ? makeFailure("Unexpected empty program") :
    isToken(sexp) ? makeFailure(`Program cannot be a single token: ${JSON.stringify(sexp)}`) :
    isArray(sexp) ? parseL5GoodProgram(first(sexp), rest(sexp), []) :
    sexp;

const parseL5GoodProgram = (keyword: Sexp, body: Sexp[], udTypeNames: string[]): Result<Program> =>
    keyword === "L51" && !isEmpty(body) ? mapv(parseL51Seq(body, udTypeNames), (exps: Exp[]) => 
                                            makeProgram(exps)) :
    makeFailure(`Program must be of the form (L51 <exp>+): ${JSON.stringify([keyword, ...body], null, 2)}`);

const isConcreteDefineType = (sexp: Sexp): boolean =>
    isCompoundSexp(sexp) && first(sexp) === 'define-type';

// L51: Threads the user-defined types as they get declared
const parseL51Seq = (body: Sexp[], udTypeNames: string[]): Result<Exp[]> =>
    isEmpty(body) ? makeOk([]) :
    isConcreteDefineType(first(body)) ? bind(extractTypeNames(first(body)), (typeNames: string[]) =>
                                            bind(parseDefineType(first(body), union(udTypeNames, typeNames)), (exp: DefineTypeExp) =>
                                                mapv(parseL51Seq(rest(body), union(udTypeNames, typeNames)), (tail: Exp[]) =>
                                                    cons(exp, tail)))) :
    bind(parseL51Exp(first(body), udTypeNames), (exp1: Exp) =>
        mapv(parseL51Seq(rest(body), udTypeNames), (tail: Exp[]) =>
            cons(exp1, tail)));

export const parseL51Exp = (sexp: Sexp, udTypeNames: string[]): Result<Exp> =>
    isEmpty(sexp) ? makeFailure("Exp cannot be an empty list") :
    isCompoundSexp(sexp) ? parseL51CompoundExp(first(sexp), rest(sexp), udTypeNames) :
    isToken(sexp) ? parseL5Atomic(sexp) :
    sexp;

export const parseL51CompoundExp = (op: Sexp, params: Sexp[], udTypeNames: string[]): Result<Exp> =>
    op === "define" ? parseDefine(params, udTypeNames) :
    op === "define-type" ? parseDefineType(cons(op, params), udTypeNames) :  // L51
    parseL51CompoundCExp(op, params, udTypeNames);

export const parseL51CompoundCExp = (op: Sexp, params: Sexp[], udTypeNames: string[]): Result<CExp> =>
    isString(op) && isSpecialFormKeyword(op) ? parseL5SpecialForm(op, params, udTypeNames) :
    parseAppExp(op, params, udTypeNames);

export const parseL5SpecialForm = (op: SpecialFormKeyword, params: Sexp[], udTypeNames: string[]): Result<CExp> =>
    isEmpty(params) ? makeFailure("Empty args for special form") :
    op === "if" ? parseIfExp(params, udTypeNames) :
    op === "lambda" ? parseProcExp(first(params), rest(params), udTypeNames) :
    op === "let" ? parseLetExp(first(params), rest(params), udTypeNames) :
    op === "quote" ? parseLitExp(first(params)) :
    op === "letrec" ? parseLetrecExp(first(params), rest(params), udTypeNames) :
    op === "set!" ? parseSetExp(params, udTypeNames) :
    op === "type-case" ? parseTypeCaseExp(params, udTypeNames) :  // L51
    op;

export const parseDefine = (params: Sexp[], udTypeNames: string[]): Result<DefineExp> =>
    isEmpty(params) ? makeFailure("define missing 2 arguments") :
    isEmpty(rest(params)) ? makeFailure(`define missing 1 arguments: ${JSON.stringify(params, null, 2)}`) :
    ! isEmpty(rest(rest(params))) ? makeFailure(`define has too many arguments: ${JSON.stringify(params, null, 2)}`) :
    parseGoodDefine(first(params), second(params), udTypeNames);

const parseGoodDefine = (variable: Sexp, val: Sexp, udTypeNames: string[]): Result<DefineExp> =>
    ! isConcreteVarDecl(variable) ? makeFailure(`First arg of define must be an identifier: ${JSON.stringify(variable, null, 2)}`) :
    bind(parseVarDecl(variable, udTypeNames), (varDecl: VarDecl) =>
        mapv(parseL51CExp(val, udTypeNames), (val: CExp) =>
            makeDefineExp(varDecl, val)));

// L51 - invoked with define-type
// (define-type id [record]*)
export const parseDefineType = (params: Sexp, udTypeNames: string[]): Result<DefineTypeExp> => 
    isToken(params) ? makeFailure(`define-type cannot be a token`) :
    isEmpty(params) ? makeFailure("define-type missing 1 argument") :
    parseGoodDefineType(first(rest(params)), rest(rest(params)), udTypeNames);

const parseGoodDefineType = (typeName: Sexp, records: Sexp[], udTypeNames: string[]): Result<DefineTypeExp> =>
    bind(parseUDTypeName(typeName, udTypeNames), (id: string) =>
        mapv(parseUserDefinedType(cons("define-type", cons(typeName, records)), udTypeNames), (udType: UserDefinedTExp) =>
            makeDefineTypeExp(id, udType)));

export const parseL5Atomic = (token: Token): Result<AtomicExp> =>
    token === "#t" ? makeOk(makeBoolExp(true)) :
    token === "#f" ? makeOk(makeBoolExp(false)) :
    isString(token) && isNumericString(token) ? makeOk(makeNumExp(+token)) :
    isString(token) && isPrimOpKeyword(token) ? makeOk(makePrimOp(token)) :
    isString(token) ? makeOk(makeVarRef(token)) :
    makeOk(makeStrExp(token.toString()));

export const parseL51CExp = (sexp: Sexp, udTypeNames: string[]): Result<CExp> =>
    isEmpty(sexp) ? makeFailure("CExp cannot be an empty list") :
    isArray(sexp) ? parseL51CompoundCExp(first(sexp), rest(sexp), udTypeNames) :
    isToken(sexp) ? parseL5Atomic(sexp) :
    sexp;

const parseAppExp = (op: Sexp, params: Sexp[], udTypeNames: string[]): Result<AppExp> =>
    bind(parseL51CExp(op, udTypeNames), (rator: CExp) =>
        mapv(mapResult((p) => parseL51CExp(p, udTypeNames), params), (rands: CExp[]) =>
            makeAppExp(rator, rands)));

const parseIfExp = (params: Sexp[], udTypeNames: string[]): Result<IfExp> =>
    params.length !== 3 ? makeFailure(`Expression not of the form (if <cexp> <cexp> <cexp>): ${JSON.stringify(params, null, 2)}`) :
    mapv(mapResult((p) => parseL51CExp(p, udTypeNames), params), (cexps: CExp[]) => 
        makeIfExp(cexps[0], cexps[1], cexps[2]));

// (lambda (<vardecl>*) [: returnTE]? <CExp>+)
const parseProcExp = (vars: Sexp, rest: Sexp[], udTypeNames: string[]): Result<ProcExp> => {
    if (isCompoundSexp(vars)) {
        const args = mapResult((vd) => parseVarDecl(vd, udTypeNames), vars);
        const body = mapResult((p) => parseL51CExp(p, udTypeNames), first(rest) === ":" ? rest.slice(2) : rest);
        const returnTE = first(rest) === ":" ? parseTExp(rest[1], udTypeNames) : makeOk(makeFreshTVar());
        return bind(args, (args: VarDecl[]) =>
                    bind(body, (body: CExp[]) =>
                        mapv(returnTE, (returnTE: TExp) =>
                            makeProcExp(args, body, returnTE))));
    } else {
        return makeFailure(`Invalid args ${JSON.stringify(vars, null, 2)}`)
    }
}

const isGoodBindings = (bindings: Sexp): bindings is [Sexp, Sexp][] =>
    isArray(bindings) && allT(isArray, bindings);

const parseLetExp = (bindings: Sexp, body: Sexp[], udTypeNames: string[]): Result<LetExp> =>
    isEmpty(body) ? makeFailure('Body of "let" cannot be empty') :
    ! isGoodBindings(bindings) ? makeFailure(`Invalid bindings: ${JSON.stringify(bindings, null, 2)}`) :
    bind(parseBindings(bindings, udTypeNames), (bdgs: Binding[]) =>
        mapv(mapResult((e) => parseL51CExp(e, udTypeNames), body), (body: CExp[]) =>
            makeLetExp(bdgs, body)));

const isConcreteVarDecl = (sexp: Sexp): boolean =>
    isIdentifier(sexp) ||
    (isArray(sexp) && sexp.length > 2 && isIdentifier(sexp[0]) && (sexp[1] === ':'));

export const parseVarDecl = (sexp: Sexp, udTypeNames: string[]): Result<VarDecl> => {
    if (isString(sexp)) {
        return makeOk(makeVarDecl(sexp, makeFreshTVar()));
    } else if (isCompoundSexp(sexp)) {
        const v = first(sexp);
        if (isString(v)) {
            return mapv(parseTExp(sexp[2], udTypeNames), (te: TExp) => makeVarDecl(v, te));
        } else {
            return makeFailure(`Invalid var ${JSON.stringify(sexp[0], null, 2)}`);
        }
    } else {
        return makeFailure(`Var cannot be a SexpString: ${JSON.stringify(sexp)}`);
    }
}

const parseBindings = (bindings: [Sexp, Sexp][], udTypeNames: string[]): Result<Binding[]> =>
    bind(mapResult((vd: Sexp) => parseVarDecl(vd, udTypeNames), map(b => b[0], bindings)), (vds: VarDecl[]) =>
        mapv(mapResult((e: Sexp) => parseL51CExp(e, udTypeNames), map(b => b[1], bindings)), (vals: CExp[]) =>
            zipWith(makeBinding, vds, vals)));

const parseLetrecExp = (bindings: Sexp, body: Sexp[], udTypeNames: string[]): Result<LetrecExp> =>
    isEmpty(body) ? makeFailure('Body of "letrec" cannot be empty') :
    ! isGoodBindings(bindings) ? makeFailure(`Invalid bindings: ${JSON.stringify(bindings, null, 2)}`) :
    bind(parseBindings(bindings, udTypeNames), (bdgs: Binding[]) =>
        mapv(mapResult((e) => parseL51CExp(e, udTypeNames), body), (body: CExp[]) => 
            makeLetrecExp(bdgs, body)));

const parseSetExp = (params: Sexp[], udTypeNames: string[]): Result<SetExp> =>
    isEmpty(params) ? makeFailure("set! missing 2 arguments") :
    isEmpty(rest(params)) ? makeFailure(`set! missing 1 argument: ${JSON.stringify(params, null, 2)}`) :
    ! isEmpty(rest(rest(params))) ? makeFailure(`set! has too many arguments: ${JSON.stringify(params, null, 2)}`) :
    parseGoodSetExp(first(params), second(params), udTypeNames);
    
const parseGoodSetExp = (variable: Sexp, val: Sexp, udTypeNames: string[]): Result<SetExp> =>
    ! isIdentifier(variable) ? makeFailure(`First arg of set! must be an identifier: ${JSON.stringify(variable, null, 2)}`) :
    mapv(parseL51CExp(val, udTypeNames), (val: CExp) => makeSetExp(makeVarRef(variable), val));

// sexps has the shape (quote <sexp>)
export const parseLitExp = (param: Sexp): Result<LitExp> =>
    mapv(parseSExp(param), (sexp: SExpValue) => makeLitExp(sexp));

export const isDottedPair = (sexps: Sexp[]): boolean =>
    sexps.length === 3 && 
    sexps[1] === "."

export const makeDottedPair = (sexps : Sexp[]): Result<SExpValue> =>
    bind(parseSExp(sexps[0]), (val1: SExpValue) =>
        mapv(parseSExp(sexps[2]), (val2: SExpValue) =>
            makeCompoundSExp(val1, val2)));

// sexp is the output of p (sexp parser)
export const parseSExp = (sexp: Sexp): Result<SExpValue> =>
    sexp === "#t" ? makeOk(true) :
    sexp === "#f" ? makeOk(false) :
    isString(sexp) && isNumericString(sexp) ? makeOk(+sexp) :
    isSexpString(sexp) ? makeOk(sexp.toString()) :
    isString(sexp) ? makeOk(makeSymbolSExp(sexp)) :
    sexp.length === 0 ? makeOk(makeEmptySExp()) :
    isArray(sexp) && isDottedPair(sexp) ? makeDottedPair(sexp) :
    isArray(sexp) ? (
        // fail on (x . y z)
        sexp[0] === '.' ? makeFailure(`Bad dotted sexp: ${JSON.stringify(sexp, null, 2)}`) : 
        bind(parseSExp(first(sexp)), (val1: SExpValue) =>
            mapv(parseSExp(rest(sexp)), (val2: SExpValue) =>
                makeCompoundSExp(val1, val2)))
    ) :
    sexp;

// L51
const parseUDTypeName = (param: Sexp, udTypeNames: string[]): Result<string> =>
    ! isString(param) ? makeFailure(`Param must be a string: ${param}`) :
    includes(param, udTypeNames) ? makeOk(param) :
    makeFailure(`Unknown user defined type name: ${param} - expected ${udTypeNames}`);
    

const parseList = (param: Sexp): Result<CompoundSexp> =>
    isCompoundSexp(param) ? makeOk(param) :
    makeFailure(`Param must be a list: ${param}`);

// (type-case id val (id (vardecl...) body...)...)
const parseTypeCaseExp = (params: Sexp[], udTypeNames: string[]): Result<TypeCaseExp> =>
    isEmpty(params) ? makeFailure('type-case must have at least 3 params') :
    isEmpty(rest(params)) ? makeFailure(`type-case must have at least 3 params`) : 
    bind(parseL51CExp(second(params), udTypeNames), (val: CExp) =>
        bind(parseUDTypeName(first(params), udTypeNames), (id: string) =>
            mapv(mapResult((p) => parseCaseExp(p, udTypeNames), rest(rest(params))), (cases: CaseExp[]) =>
                makeTypeCaseExp(id, val, cases))));

// (id (vardecl...) body...)
const parseCaseExp = (caseParams: Sexp, udTypeNames: string[]): Result<CaseExp> =>
    isToken(caseParams) ? makeFailure(`case expression in type-case must be a list`) :
    caseParams.length < 3 ? makeFailure(`Case expression in type-case must be of the form (id (vars...) body...): ${caseParams}`) : 
    bind(parseUDTypeName(first(caseParams), udTypeNames), (id: string) =>
        bind(parseList(second(caseParams)), (vardecls: CompoundSexp) => 
            bind(mapResult((vd) => parseVarDecl(vd, udTypeNames), vardecls), (vds: VarDecl[]) =>
                mapv(mapResult((p) => parseL51CExp(p, udTypeNames), rest(rest(caseParams))), (body: CExp[]) =>
                    makeCaseExp(id, vds, body)))));


// ==========================================================================
// Unparse: Map an AST to a concrete syntax string.

export const unparse = (e: Parsed): Result<string> =>
    // NumExp | StrExp | BoolExp | PrimOp | VarRef
    isNumExp(e) ? makeOk(`${e.val}`) :
    isStrExp(e) ? makeOk(`"${e.val}"`) :
    isBoolExp(e) ? makeOk(e.val ? "#t" : "#f") :
    isPrimOp(e) ? makeOk(e.op) :
    isVarRef(e) ? makeOk(e.var) :
    // AppExp | IfExp | ProcExp | LetExp | LitExp | LetrecExp | SetExp | TypeCaseExp
    isAppExp(e) ? bind(unparse(e.rator), (rator: string) =>
                    mapv(mapResult(unparse, e.rands), (rands: string[]) =>
                        `(${rator} ${join(" ", rands)})`)) :
    isIfExp(e) ? bind(unparse(e.test), (test: string) =>
                    bind(unparse(e.then), (then: string) =>
                        mapv(unparse(e.alt), (alt: string) => 
                            `(if ${test} ${then} ${alt})`))) :
    isLetExp(e) ? unparseLetExp(e) :
    isLetrecExp(e) ? unparseLetrecExp(e) :
    isProcExp(e) ? unparseProcExp(e) :
    isLitExp(e) ? makeOk(unparseLitExp(e)) :
    isSetExp(e) ? unparseSetExp(e) :
    isTypeCaseExp(e) ? unparseTypeCaseExp(e) :  // L51
    // DefineExp | DefineTypeExp | Program
    isDefineExp(e) ? bind(unparseVarDecl(e.var), (vd: string) =>
                        mapv(unparse(e.val), (val: string) =>
                            `(define ${vd} ${val})`)) :
    isDefineTypeExp(e) ? unparseTExp(e.udType) : // L51
    isProgram(e) ? mapv(unparseLExps(e.exps), (exps: string) => `(L51 ${exps})`) :   // l51
    e;

const unparseReturn = (te: TExp): Result<string> =>
    isTVar(te) ? makeOk("") :
    mapv(unparseTExp(te), (te: string) => ` : ${te}`);

const unparseBindings = (bindings: Binding[]): Result<string> =>
    mapv(mapResult(bdg => bind(unparseVarDecl(bdg.var), (vd: string) =>
                            mapv(unparse(bdg.val), (val: string) => `(${vd} ${val})`)),
                   bindings), (bdgs: string[]) => 
            join(" ", bdgs));

const unparseVarDecl = (vd: VarDecl): Result<string> =>
    isTVar(vd.texp) ? makeOk(vd.var) :
    mapv(unparseTExp(vd.texp), te => `(${vd.var} : ${te})`);

// Add a quote for symbols, empty and compound sexp - strings and numbers are not quoted.
const unparseLitExp = (le: LitExp): string =>
    isEmptySExp(le.val) ? `'()` :
    isSymbolSExp(le.val) ? `'${valueToString(le.val)}` :
    isCompoundSExp(le.val) ? `'${valueToString(le.val)}` :
    `${le.val}`;

const unparseLExps = (les: Exp[]): Result<string> =>
    mapv(mapResult(unparse, les), (les: string[]) => join(" ", les));

const unparseProcExp = (pe: ProcExp): Result<string> =>
    bind(mapResult(unparseVarDecl, pe.args), (vds: string[]) =>
        bind(unparseReturn(pe.returnTE), (ret: string) =>
            mapv(unparseLExps(pe.body), (body: string) =>
            `(lambda (${join(" ", vds)})${ret} ${body})`)));

const unparseLetExp = (le: LetExp) : Result<string> => 
    bind(unparseBindings(le.bindings), (bdgs: string) =>
        mapv(unparseLExps(le.body), (body: string) => 
            `(let (${bdgs}) ${body})`));

const unparseLetrecExp = (le: LetrecExp): Result<string> =>
    bind(unparseBindings(le.bindings), (bdgs: string) => 
        mapv(unparseLExps(le.body), (body: string) =>
            `(letrec (${bdgs}) ${body})`));

const unparseSetExp = (se: SetExp): Result<string> =>
    mapv(unparse(se.val), (val: string) => `(set! ${se.var.var} ${val})`);

// L51
const unparseTypeCaseExp = (te: TypeCaseExp): Result<string> =>
    bind(unparse(te.val), (val) =>
        mapv(mapResult(unparseCaseExp, te.cases), (cases) =>
            `(type-case ${te.typeName} ${val}\n  ${join('\n  ', cases)})`));

const unparseCaseExp = (ce: CaseExp): Result<string> => 
    bind(unparseLExps(ce.body), (body) =>
        mapv(mapResult(unparseVarDecl, ce.varDecls), (vds) =>
            `(${ce.typeName} (${join(" ", vds)}) ${body})`));


