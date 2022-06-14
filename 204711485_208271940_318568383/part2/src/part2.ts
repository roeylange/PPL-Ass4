export const MISSING_KEY = '___MISSING_KEY___'
export const MISSING_TABLE_SERVICE = '___MISSING_TABLE_SERVICE___'

export type Table<T> = Readonly<Record<string, Readonly<T>>>

export type TableService<T> = {
    get(key: string): Promise<T>;
    set(key: string, val: T): Promise<void>;
    delete(key: string): Promise<void>;
}

// Q 2.1 (a)
export function makeTableService<T>(sync: (table?: Table<T>) => Promise<Table<T>>): TableService<T> {
    // optional initialization code
    return {
        get(key: string): Promise<T> {
            return sync()
                    .then((table)=>{
                        if (table[key]) {
                            return Promise.resolve(table[key]);
                        } else {
                            throw new Error("Exception message");
                        }
                    })
                    .catch((err)=> Promise.reject(MISSING_KEY))
        },
        set(key: string, val: T): Promise<void> {
            return sync()
                .then((table)=>{
                    let newTable: Record<string, Readonly<T>> = {};
                    let isFound: Boolean = false;
                    for (let k in table) {
                    if (key == k) {
                        newTable[k] = val;
                        isFound = true;
                    } else {
                        newTable[k] = table[k];
                    }        
                    }
                    if (!isFound) {
                        newTable[key] = val;
                    }
                    return sync(newTable).then((x)=>Promise.resolve())
                    })            
        },
        delete(key: string): Promise<void> {
            return sync()
                    .then((table)=>{
                        let newTable: Record<string, Readonly<T>> = {};
                        for (let k in table) {
                            if (key != k) {
                              newTable[k] = table[k];
                            }
                        }
                        return sync(newTable).then(()=> Promise.resolve())                       
                    })
                    .catch((err)=>Promise.reject(MISSING_KEY))
        }
    }
}

// Q 2.1 (b)
export function getAll<T>(store: TableService<T>, keys: string[]): Promise<T[]> {
    const promises = keys.map(key=>store.get(key))
    return Promise.all(promises)
}


// Q 2.2
export type Reference = { table: string, key: string }

export type TableServiceTable = Table<TableService<object>>

export function isReference<T>(obj: T | Reference): obj is Reference {
    return typeof obj === 'object' && 'table' in obj
}

export async function constructObjectFromTables(
    tables: TableServiceTable,
    ref: Reference
  ) {
    async function deref(ref: Reference) {
      let value = await tables[ref.table].get(ref.key);
      let entries: any[] = await Promise.all(
        Object.entries(value).map(async (entry) => {
          if (isReference(entry[1])) {
            return [entry[0], await deref(entry[1])];
          } else {
            return entry;
          }
        })
      );
      return Object.fromEntries(entries);
    }
  
    if (!(ref.table in tables)) {
      throw MISSING_TABLE_SERVICE;
    }
  
    try {
      return deref(ref);
    } catch {
      throw MISSING_KEY;
    }
  }

// Q 2.3

export function lazyProduct<T1, T2>(g1: () => Generator<T1>, g2: () => Generator<T2>): () => Generator<[T1, T2]> {
    return function* () {
        for (const n1 of g1()) {
            for (const n2 of g2()) {
              yield [n1, n2];
            }
        }
    }
}

function* secondGen<T2>(g: () => Generator<T2>) {
    for (const n of g()) {
      yield n;
    }
}

export function lazyZip<T1, T2>(g1: () => Generator<T1>, g2: () => Generator<T2>): () => Generator<[T1, T2]> {
    return function* () {
        const gen2 = secondGen(g2);
        for (const n of g1()) {
            let n2 = gen2.next().value;
            if (n2) {
                yield [n, n2];
            }
        }
    }
}

// Q 2.4
export type ReactiveTableService<T> = {
    get(key: string): T;
    set(key: string, val: T): Promise<void>;
    delete(key: string): Promise<void>;
    subscribe(observer: (table: Table<T>) => void): void
}

export async function makeReactiveTableService<T>(sync: (table?: Table<T>) => Promise<Table<T>>, optimistic: boolean): Promise<ReactiveTableService<T>> {
    // optional initialization code
    
    let observers: ((table: Table<T>) => void)[] = [];
    let _table: Table<T> = await sync()

    const handleMutation = async (newTable: Table<T>) => {
        observers.forEach((observer) => observer(newTable));
    }
    return {
        get(key: string): T {
            if (key in _table) {
                return _table[key]
            } else {
                throw MISSING_KEY
            }
        },
        async set(key: string, val: T): Promise<void> {
            let currentTable: Table<T> = await sync();
            let newTable: Record<string, Readonly<T>> = {};
            let isFound: Boolean = false;
            for (let k in currentTable) {
                if (key == k) {
                    newTable[k] = val;
                    isFound = true;
                } else {
                    newTable[k] = currentTable[k];
                }
            }
            if (!isFound) {
            newTable[key] = val;
            }
  
            try {
                if (optimistic) {
                    await handleMutation(newTable);
                }
                await sync(newTable);
            } catch (err) {
                await handleMutation(currentTable);
                throw err;
            }
  
            if (!optimistic) {
                await handleMutation(newTable);
            }
        },
        async delete(key: string): Promise<void> {
            let currentTable: Table<T> = await sync();
            let newTable: Record<string, Readonly<T>> = {};
            let isFound: Boolean = false;
    
            for (let k in currentTable) {
                if (key != k) {
                    newTable[k] = currentTable[k];
                } else {
                    isFound = true;
                }
            }
  
            if (!isFound) {
                throw MISSING_KEY;
            }
    
            try {
                if (optimistic) {
                    await handleMutation(newTable);
                }
                await sync(newTable);
            } catch (err) {
                await handleMutation(currentTable);
                throw err;
            }
            if (!optimistic) {
                await handleMutation(newTable);
            }
        },
        subscribe(observer: (table: Table<T>) => void): void {
            observers.push(observer);
        }
    }
}
