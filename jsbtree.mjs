/**
 * Js BTree - Just for Fun
 *
 * @module JsBtree
 * @author Ken
 */
import util from 'node:util'
import { Buffer } from 'node:buffer'
import fs from 'node:fs'
import assert from 'node:assert'
/**
 * Helper function
 *
 * @param {object} obj  to inspect
 * @returns {string}  formatted object string
 */
function inspect(obj) {
  return util.inspect(obj, { depth: null, breakLength: 240 })
}
/**
 * Static counters to help understand performance
 *
 * @type {{ inserts: number; insertFails: number; deletes: number; deleteFails: number; finds: number; findFails: number; dbHeaderReads: number; dbHeaderWrites: number; dbBucketReads: number; dbBucketWrites: number; recordReads: { ...; }; recordWrites: { ...; }; maxDepth: number; }}
 */
let statistic = {
  inserts: 0,
  insertFails: 0,
  deletes: 0,
  deleteFails: 0,
  finds: 0,
  findFails: 0,
  dbHeaderReads: 0,
  dbHeaderWrites: 0,
  dbBucketReads: 0,
  dbBucketWrites: 0,
  recordReads: { d: 0, i: 0 },
  recordWrites: { d: 0, i: 0 },
  maxDepth: 1
}
/**
 *  Pack/Unpack object to/from Buffer
 *
 * @class RecordPacker
 * @property {Buffer} buffer
 */
class RecordPacker {
  /**
   * Creates an instance of RecordPacker.
   *
   * @constructor
   * @param {property[]} manifest
   */
  constructor(manifest) {
    this.offset = 0
    this.buffer = Buffer.alloc(0)
    this.offset = 0
    this.manifest = this.compile(manifest)
  }
  /**
   * Set the buffer size
   *
   * Implements database structure
   *
   * @param {number} size
   */
  set bufSize(size) {
    this.buffer = Buffer.alloc(size)
  }
  /**
   * property types supported with their implementation of readBuf/writeBuf
   *
   * @type { char | string | uint | int | array | record }
   */
  types = {
    char: {
      writeBuf: (data) => {
        this.buffer.writeUint8(data.charCodeAt(0), this.offset)
        this.offset += 1
      },
      readBuf: () => {
        this.offset += 1
        return String.fromCharCode(this.buffer.readUint8(this.offset - 1))
      }
    },
    string: {
      writeBuf: (data) => {
        let len = Buffer.byteLength(data)
        this.buffer.writeUint32LE(len, this.offset)
        this.offset += 4
        this.buffer.write(data, this.offset, len, 'utf8')
        this.offset += len
      },
      readBuf: () => {
        let len = this.buffer.readUint32LE(this.offset)
        this.offset += len + 4
        return this.buffer.toString('utf8', this.offset - len, this.offset)
      }
    },
    uint: {
      writeBuf: (data) => {
        this.buffer.writeUint32LE(data, this.offset)
        this.offset += 4
      },
      readBuf: () => {
        this.offset += 4
        return this.buffer.readUint32LE(this.offset - 4)
      }
    },
    int: {
      writeBuf: (data) => {
        this.buffer.writeInt32LE(data, this.offset)
        this.offset += 4
      },
      readBuf: () => {
        this.offset += 4
        return this.buffer.readInt32LE(this.offset - 4)
      }
    },
    array: {
      writeBuf: (data, of) => {
        this.buffer.writeUint32LE(data.length, this.offset)
        this.offset += 4
        for (let item of data) {
          this.pack(item, of)
        }
      },
      readBuf: (of) => {
        let data = Array(this.buffer.readUint32LE(this.offset)).fill()
        this.offset += 4
        for (let item in data) {
          data[item] = this.unpack(of)
        }
        return data
      }
    },
    record: {
      writeBuf: (data, of) => {
        this.pack(data, of)
      },
      readBuf: (of) => {
        return this.unpack(of)
      }
    }
  }
  /**
   * Create internal copy of object packer definition
   *
   * @param {item[]} manifest
   * @returns {item[]} compiled manifest
   */
  compile(manifest) {
    let cManifest = []
    for (let item of manifest) {
      assert(this.types[item.type] != undefined, 'Unsupported type: ' + item.type + ' in property definition')
      let cItem = {
        name: item.name,
        type: item.type,
        readBuf: this.types[item.type].readBuf,
        writeBuf: this.types[item.type].writeBuf
      }
      if (item.of) cItem.of = this.compile(item.of)
      cManifest.push(cItem)
    }
    return cManifest
  }
  /**
   * Deconstruct partial object into buffer
   *
   * @param {{}} data
   * @param {item[]} manifest
   */
  pack(data, manifest) {
    for (let item of manifest) {
      item.writeBuf(data[item.name] ?? data, item.of)
    }
  }
  /**
   * Construct an object from a buffer
   *
   * @param {property[]} manifest
   * @returns {{}} constructed object
   */
  unpack(manifest) {
    let data = {}
    if (manifest.length == 1 && manifest[0].name == undefined) {
      // Array element
      data = manifest[0].readBuf(manifest[0].of)
    } else {
      for (let item of manifest) {
        data[item.name] = item.readBuf(item.of)
      }
    }
    return data
  }
  /**
   * Pack data into buffer
   *
   * @param {{}} data
   * @returns {Buffer}
   */
  toBuffer(data) {
    this.offset = 0
    this.buffer.fill(0)
    this.pack(data, this.manifest)
    return this.buffer
  }
  /**
   * Unpack buffer into object
   *
   * @returns {{}}
   */
  fromBuffer() {
    this.offset = 0
    return this.unpack(this.manifest)
  }
}
/**
 * RecPtr - Immutable (can be shared) pointer to a record
 *
 * @class RecPtr
 */
class RecPtr {
  #bucketIdx
  #recIdx
  constructor(bucketIdx, recIdx) {
    assert(Number.isInteger(bucketIdx) && Number.isInteger(recIdx), 'Invalid RecPtr args')
    this.#bucketIdx = bucketIdx
    this.#recIdx = recIdx
  }
  /**
   * Null Pointer
   *
   * @static Singleton
   * @type {RecPtr}
   */
  static nullPtr = new RecPtr(-1, 0)
  /**
   * Property definition of RecPtr state
   *
   * @static
   * @type {item[]}
   */
  static rpManifest = [
    { name: 'bkt', type: 'int' },
    { name: 'rec', type: 'int' }
  ]
  /**
   * Create a RecPtr from its external state
   *
   * @static
   * @param {{bkt: number; rec: number}} state
   * @returns {RecPtr}
   */
  static fromState(state) {
    if (state.bkt < 0) {
      return this.nullPtr
    } else {
      return new RecPtr(state.bkt, state.rec)
    }
  }
  /**
   * return external state
   *
   * @readonly
   * @type {{ bkt: number; rec: number; }}
   */
  get state() {
    return { bkt: this.#bucketIdx, rec: this.#recIdx }
  }
  /**
   * Test if this RecPtr is a null pointer
   *
   * @returns {boolean}
   */
  isNull() {
    return this.#bucketIdx < 0
  }
  /**
   * Bucket index referred to by this pointer
   *
   * @readonly
   * @type {number}
   */
  get bucketIdx() {
    return this.#bucketIdx
  }
  /**
   * Record index referred to by this pointer
   *
   * @readonly
   * @type {number}
   */
  get recIdx() {
    return this.#recIdx
  }
}
/**
 *
 * Contains the state and structure of the database
 *
 * @class DbHeader
 */
class DbHeader {
  constructor(dbFile) {
    this.dbFile = dbFile
    this.bucketMap = []
    this.rootBtNodePtr = RecPtr.nullPtr
  }
  /**
   * Property definition of DbHeader state
   *
   * @static
   * @type {property[]}
   */
  static rpManifest = [
    { name: 'rootBtNodePtr', type: 'record', of: RecPtr.rpManifest },
    { name: 'bucketCount', type: 'uint' },
    {
      name: 'structure',
      type: 'record',
      of: [
        { name: 'header', type: 'uint' },
        { name: 'blksize', type: 'uint' },
        {
          name: 'bucket',
          type: 'record',
          of: [
            { name: 'header', type: 'uint' },
            { name: 'size', type: 'uint' }
          ]
        },
        {
          name: 'record',
          type: 'record',
          of: [
            { name: 'f', type: 'int' },
            { name: 'i', type: 'uint' },
            { name: 'd', type: 'uint' }
          ]
        },
        {
          name: 'key',
          type: 'record',
          of: [
            { name: 'maxLen', type: 'uint' },
            { name: 'maxNodeKeys', type: 'uint' }
          ]
        }
      ]
    }
  ]
  /**
   * Create a DbHeader object from its state read from storage
   *
   * @static
   * @param {DbFile} dbFile
   * @returns {DbHeader}
   */
  static fromFile(dbFile) {
    const dbHeader = new DbHeader(dbFile)
    dbHeader.state = dbFile.readDbHeader()
    return dbHeader
  }
  /**
   * return external state
   *
   * @readonly
   * @type {{ structure: DbStructure; rootBtNodePtr: RecPtr; bucketCount: number}}
   */
  get state() {
    return {
      structure: this.dbFile.structure,
      rootBtNodePtr: this.rootBtNodePtr.state,
      bucketCount: this.bucketMap.length
    }
  }
  /**
   * Configure DbHeader from external state
   */
  set state(state) {
    this.bucketMap = 'f'.repeat(state.bucketCount).split('')
    this.rootBtNodePtr = RecPtr.fromState(state.rootBtNodePtr)
    this.dbFile.structure = state.structure
  }
  /**
   * Save state to storage
   */
  write() {
    this.dbFile.writeDbHeader(this.state)
  }
  /**
   * Set the type of bucket[index]
   *
   * @param {number} index
   * @param {('f' | 'i' | 'd')} state
   */
  setBucketType(index, state) {
    while (this.bucketMap.length <= index) this.bucketMap.push('')
    this.bucketMap[index] = state
  }
}
/**
 * Bucket holds a set of records and their state (free/allocated)
 *
 * @class DbBucket
 */
class DbBucket {
  constructor(dbHeader, dbFile, index) {
    this.dbFile = dbFile
    this.dbHeader = dbHeader
    this.index = index
    this.recMap = []
    this.type = 'f'
  }
  /**
   * Property definition of DbBucket state
   *
   * @static
   * @type {property[]}
   */
  static rpManifest = [
    { name: 'type', type: 'char' },
    { name: 'index', type: 'uint' },
    {
      name: 'recMap',
      type: 'array',
      of: [{ name: undefined, type: 'char' }]
    }
  ]
  /**
   * Construct a DbBucket from external storage
   *
   * @static
   * @param {DbHeader} dbHeader
   * @param {DbFile} dbFile
   * @param {number} index
   * @returns {DbBucket}
   */
  static fromFile(dbHeader, dbFile, index) {
    const bucket = new DbBucket(dbHeader, dbFile, index)
    bucket.state = dbFile.readBucketHeader(index)
    return bucket
  }
  write() {
    this.dbFile.writeBucketHeader(this.state, this.index)
  }
  /**
   * External state
   *
   * @type {{ type: string; index: any; recMap: {}; }}
   */
  set state(state) {
    this.type = state.type
    this.index = state.index
    this.recMap = state.recMap
  }
  get state() {
    return {
      type: this.type,
      index: this.index,
      recMap: this.recMap
    }
  }
  setType(type) {
    if (this.type !== 'f' && type !== 'f') {
      throw new Error('Invalid bucket type')
    }
    this.dbHeader.setBucketType(this.index, type)
    this.type = type
    let recLen = this.dbFile.structure.record[type]
    if (recLen == undefined) throw new Error('invalid record type')
    if (recLen > 0) {
      this.recMap = 'f'.repeat(Math.max(((this.dbFile.structure.bucket.size - this.dbFile.structure.bucket.header) / recLen) >> 0, 0)).split('')
    } else {
      this.recMap = []
    }
  }
  /**
   * Find a free record in appropriate bucket or allocate a fresh one
   *
   * @returns {number} index of record in bucket
   */
  getFreeRecord() {
    let recIndex = this.recMap.indexOf('f')
    assert(recIndex >= 0, 'No Free Record in DbBucket.getFreeRecord')
    this.recMap[recIndex] = this.type
    if (this.recMap.indexOf('f') < 0) {
      this.dbHeader.setBucketType(this.index, this.type.toUpperCase())
    }
    return recIndex
  }
  /**
   * Free a record in bucket for future use
   *
   * @param {number} recIndex
   */
  freeRecord(recIndex) {
    this.recMap[recIndex] = 'f'
    if (this.recMap.join('').replace(/f/g, '') == '') {
      this.type = 'f'
      this.recMap = []
    }
    this.dbHeader.setBucketType(this.index, this.type)
  }
}
/**
 * BtKey - contains the key (presently string) and a pointer to BtNodes containing comparatively larger keys
 *
 * @class BtKey
 */
class BtKey {
  /**
   * Creates an instance of BtKey.
   *
   * @constructor
   * @param {string} key
   * @param {RecPtr} recPtr
   */
  constructor(key, recPtr) {
    this.key = key
    this.recPtr = recPtr
    this.nextPtr = RecPtr.nullPtr
  }
  /**
   * Property definition of BtKey state
   *
   * @static
   * @type {property[]}
   */
  static rpManifest = [
    { name: 'nextPtr', type: 'record', of: RecPtr.rpManifest },
    { name: 'recPtr', type: 'record', of: RecPtr.rpManifest },
    { name: 'key', type: 'string' }
  ]
  /**
   * Construct a BtKey from external state
   *
   * @static
   * @param {{}} state
   * @returns {BtKey}
   */
  static fromState(state) {
    let btKey = new BtKey('', RecPtr.nullPtr)
    btKey.state = state
    return btKey
  }
  /**
   * External state
   *
   * @type {{ key: string; recPtr: RecPtr; nextPtr: RecPtr }}
   */
  set state(state) {
    this.key = state.key
    this.recPtr = RecPtr.fromState(state.recPtr)
    this.nextPtr = RecPtr.fromState(state.nextPtr)
  }
  get state() {
    return {
      key: this.key,
      recPtr: this.recPtr.state,
      nextPtr: this.nextPtr.state
    }
  }
}
/**
 * Path Key a point in a path to a key
 */
class PKey {
  constructor(node, parent = undefined, index = 0) {
    this.node = node
    this.parent = parent
    this.index = index
  }
}
/**
 * A node in a Btree containing a collection of keys and a pointer to a previous node container lesser keys
 *
 * @class BtNode
 */
class BtNode {
  /**
   * Creates an instance of BtNode.
   *
   * @constructor
   * @param {DbFile} dbFile
   * @param {RecPtr} selfPtr
   */
  constructor(dbFile, selfPtr) {
    this.prevPtr = RecPtr.nullPtr
    this.btKeys = []
    this.dbFile = dbFile
    this.selfPtr = selfPtr
  }
  /**
   * Property definition of BtNode state
   *
   * @static
   * @type {property[]}
   */
  static rpManifest = [
    { name: 'prevPtr', type: 'record', of: RecPtr.rpManifest },
    { name: 'btKeys', type: 'array', of: BtKey.rpManifest }
  ]
  /**
   * Construct a BtNode from its external state
   *
   * @static
   * @param {DbFile} dbFile
   * @param {RecPtr} nodePtr
   * @returns {BtNode}
   */
  static fromFile(dbFile, nodePtr) {
    let btNode = new BtNode(dbFile, nodePtr)
    btNode.state = dbFile.readRecord('i', nodePtr)
    return btNode
  }
  /**
   * External state of BtNode
   *
   * @type {{ prevPtr: RecPtr; btKeys: BtKey[]; }}
   */
  set state(state) {
    this.prevPtr = RecPtr.fromState(state.prevPtr)
    this.btKeys = state.btKeys.map((btKey) => {
      return BtKey.fromState(btKey)
    })
  }
  get state() {
    return {
      prevPtr: this.prevPtr.state,
      btKeys: this.btKeys.slice().map((btKey) => {
        return btKey.state
      })
    }
  }
  /**
   * Save external state to storage
   */
  write() {
    this.dbFile.writeRecord(this.state, 'i', this.selfPtr)
  }
  /**
   * True if this is a leaf node
   *
   * @returns {boolean}
   */
  isLeaf() {
    return this.prevPtr.isNull()
  }
  /**
   * Construct a PKey for this step in the path to the key,
   * adding itself a the child of the parent PKey
   *
   * @param {(string | any)} key
   * @param {PKey} parentPKey
   * @returns {PKey}
   */
  nextPKey(key, parentPKey) {
    let nextPKey = new PKey(this, parentPKey, 0)
    nextPKey.prevPtr = this.prevPtr
    for (let btKey of this.btKeys) {
      nextPKey.nextPtr = btKey.nextPtr
      nextPKey.cmp = btKey.key.localeCompare(key)
      if (nextPKey.cmp >= 0) {
        break
      } else {
        nextPKey.prevPtr = nextPKey.nextPtr
        nextPKey.index += 1
      }
    }
    return nextPKey
  }
}
/**
 * An in memory cache of BtNodes used in making an update to the database noting modified nodes
 * so they can be written in one step
 *
 * @class BtNodeCache
 */
class BtNodeCache {
  /**
   * Creates an instance of BtNodeCache.
   *
   * @constructor
   * @param {DbFile} dbFile
   * @param {Database} db
   * @param {{}} opts to tune the operation of the database
   */
  constructor(dbFile, db, opts) {
    this.db = db
    this.dbFile = dbFile
    this.cache = new Map()
    this.maxCacheSize = opts.maxCacheSize ?? 0
  }
  /**
   * Retrieve a BtNode. Read from storage if not in cache
   *
   * @param {RecPtr} nodePtr
   * @param {boolean} [readOnly=false] flushed out to storage on commit if writeable
   * @returns {BtNode}
   */
  get(nodePtr, readOnly = false) {
    assert(!nodePtr.isNull(), 'Null Node pointer in get')
    let btNodeEntry = this.cache.get(nodePtr)
    if (btNodeEntry == undefined) {
      // deal with max cache
      btNodeEntry = {
        btNode: BtNode.fromFile(this.dbFile, nodePtr),
        readOnly: readOnly
      }
      this.cache.set(nodePtr, btNodeEntry)
    }
    btNodeEntry.readOnly &&= readOnly
    return btNodeEntry.btNode
  }
  /**
   * Obtain a free BtNode
   *
   * @returns {BtNode}
   */
  getNew() {
    let selfPtr = this.db.newRecord('i')
    let btNode = new BtNode(this.dbFile, selfPtr)
    assert(!btNode.selfPtr.isNull(), 'Null Node pointer in getNew')
    this.cache.set(btNode.selfPtr, { btNode: btNode, readOnly: false })
    return btNode
  }
  /**
   * Remove a BtNode from cache and mark it as free
   *
   * @param {RecPtr} nodePtr
   */
  delete(nodePtr) {
    assert(!nodePtr.isNull(), 'Null Node pointer in delete')
    this.db.freeRecord(nodePtr)
    this.cache.delete(nodePtr)
  }
  /**
   * Flush out writeable BtNodes
   */
  commit() {
    this.cache.forEach((btNodeEntry) => {
      if (!btNodeEntry.readOnly) {
        assert(!btNodeEntry.btNode.selfPtr.isNull(), 'Null Node pointer in commit')
        btNodeEntry.btNode.write()
        btNodeEntry.readOnly = true
      }
    })
    if (this.maxCacheSize > 0) {
      while (this.cache.size > this.maxCacheSize) {
        this.cache.delete(this.cache.keys().next().value)
      }
    } else {
      this.cache.clear()
    }
  }
  /**
   * Discard all cached BtNodes including writeable nodes
   */
  rollback() {
    this.cache.clear()
  }
}
/**
 * Manages the consistency of the structure settings.
 * Provides a default structure with reasonable settings.
 * And a minimal bootstrap structure for accessing a database before its structure is known
 *
 * @class DbStructure
 */
class DbStructure {
  #structure
  constructor(structure) {
    this.#structure = structure
  }
  /**
   * Default structure for database
   *
   * @static
   * @memberof DbStructure
   */
  static defaultStructure = new DbStructure({
    header: 1024,
    bucket: {
      header: 512,
      size: 8182
    },
    record: {
      f: -1,
      i: 512,
      d: 512
    },
    key: {
      maxLen: 40,
      maxNodeKeys: 10
    },
    blksize: 512
  })
  /**
   * Minimal Structure to bootstrap opening of database
   *
   * @static
   * @memberof DbStructure
   */
  static bootstrapStructure = new DbStructure({
    header: 8192
  })
  /**
   * Fixed elements of structure
   *
   * @static
   * @type {{}}
   */
  static fixedStructure = {
    record: { f: -1 }
  }
  get structure() {
    return JSON.parse(JSON.stringify(this.#structure)) //clone
  }
  /**
   *
   * A sparse structure can be applied to a complete structure
   *
   * @param {{}} structure A sparse structure to modify current structure
   * @memberof DbStructure
   */
  apply(structure) {
    let apply = (def, opt) => {
      for (let optKey of Object.keys(def)) {
        if (opt[optKey] != undefined) {
          if (typeof def[optKey] == 'object') {
            apply(def[optKey], opt[optKey])
          } else {
            def[optKey] = opt[optKey]
          }
        }
      }
    }
    let roundUp = (value) => {
      return Math.ceil(value / this.#structure.blksize) * this.#structure.blksize
    }
    apply(this.#structure, structure)
    apply(this.#structure, DbStructure.fixedStructure)
    this.#structure.key.maxNodeKeys = structure?.key?.maxNodeKeys || ((this.#structure.record.i - 40) / (this.#structure.key.maxLen + 20)) >> 0
    this.#structure.header = roundUp(this.#structure.header)
    this.#structure.bucket.header = roundUp(this.#structure.bucket.header)
    this.#structure.bucket.size = roundUp(this.#structure.bucket.size)
    this.#structure.record.i = roundUp(this.#structure.record.i)
    this.#structure.record.d = roundUp(this.#structure.record.d)
  }
}
/**
 * Read/Write internal state to external storage\
 * DbStructure manipulates the structure definition\
 * RecordPacker converts internal objects to external flat data
 *
 * Database structure\
 * DbHeader contains the structure of the file
 * followed by fixed size buckets\
 * Each bucket has a DbHeader followed by fixed size records.\
 * The number of records in a bucket depends on the record type
 *
 * - DbHeader
 *  - Bucket [ ]
 *    - BucketHeader
 *    - Record [ ]
 *      - index | data
 *
 * ## Log file
 *
 * A series of headers-data pairs
 *
 * header
 *    +----------+----------+----------+----------+
 *    |  TXno    |  Offset  |  Length  | Type     |
 *    +----------+----------+----------+----------+
 *    |     x    |     x    |     x    | data     |
 *    +----------+----------+----------+----------+
 *    |     x    |    -1    |     0    | BegTx    |
 *    +----------+----------+----------+----------+
 *    |     x    |    -2    |     0    | EndTx    |
 *    +----------+----------+----------+----------+
 *    |     0    |     0    |     0    | Eol      |
 *    +----------+----------+----------+----------+
 *
 * On open a consistent db will have an Eol header.
 *
 * If the header is a BegTx then the file is scanned for a EndTx with the same tx number.
 * If found the all the records are written to the db else the log file is reset
 *
 *
 *
 * @class DbFile
 */
class DbFile {
  #structure
  constructor() {
    this.fd
    this.fdLog
    this.logMap = new Map()
    this.logPos = 0
    this.txNumber = 0
    this.txDepth = 0
    this.recordPackers = {
      d: new RecordPacker([{ name: undefined, type: 'string' }]),
      i: new RecordPacker(BtNode.rpManifest),
      dbHeader: new RecordPacker(DbHeader.rpManifest),
      dbBucket: new RecordPacker(DbBucket.rpManifest),
      logEntry: new RecordPacker([
        { name: 'txNo', type: 'uint' },
        { name: 'ofs', type: 'int' },
        { name: 'len', type: 'int' }
      ])
    }
    this.structure = DbStructure.bootstrapStructure.structure
    this.recordPackers.logEntry.bufSize = 24
  }
  set structure(structure) {
    this.#structure = structure
    this.recordPackers.dbHeader.bufSize = structure.header
    if (structure?.bucket?.header) {
      this.recordPackers.dbBucket.bufSize = structure.bucket.header
      this.recordPackers.d.bufSize = structure.record.d
      this.recordPackers.i.bufSize = structure.record.i
      this.logBuffer = Buffer.alloc(Math.max(structure.header, structure.bucket.header, structure.record.d, structure.record.i))
    }
  }
  static txHeader = {
    begTx: { txNo: 0, ofs: -1, len: 0 },
    endTx: { txNo: 0, ofs: -2, len: 0 },
    eol: { txNo: 0, ofs: 0, len: 0 }
  }
  /**
   *
   * @type {{}}
   */
  get structure() {
    return this.#structure
  }
  cloneStructure() {
    return JSON.parse(JSON.stringify(this.#structure))
  }
  open(fileName) {
    this.#structure = DbStructure.bootstrapStructure.structure
    this.fd = fs.openSync(fileName, 'r+')
    try {
      this.fdLog = fs.openSync(fileName + '.log', 'r+')
      this.scanLog()
    } catch (error) {
      this.fdLog = fs.openSync(fileName + '.log', 'w+')
    }
    //    this.applyLog()
  }
  create(fileName, structure = {}) {
    this.fd = fs.openSync(fileName, 'w+')
    this.fdLog = fs.openSync(fileName + '.log', 'w+')
    let dbStructure = DbStructure.defaultStructure
    dbStructure.apply(structure)
    if (!dbStructure.structure.blksize) dbStructure.apply({ blksize: fs.statSync(fileName).blksize })
    this.structure = dbStructure.structure
  }
  close() {
    fs.closeSync(this.fd)
    fs.closeSync(this.fdLog)
  }
  write(buffer, position) {
    if (this.txDepth <= 0) {
      fs.writeSync(this.fd, buffer, 0, buffer.byteLength, position)
    } else {
      fs.writeSync(this.fdLog, this.recordPackers.logEntry.toBuffer({ txNo: this.txNumber, ofs: position, len: buffer.byteLength }), 0, 12, this.logPos)
      this.logPos += 12
      this.logMap.set(position, { pos: this.logPos, len: buffer.byteLength })
      fs.writeSync(this.fdLog, buffer, 0, buffer.byteLength, this.logPos)
      this.logPos += buffer.byteLength
      fs.writeSync(this.fdLog, this.recordPackers.logEntry.toBuffer({ txNo: 0, ofs: 0, len: 0 }), 0, 12, this.logPos)
    }
  }

  read(buffer, position) {
    /**
     * check log map for data
     * if found read log else read db
     */
    let logEntry = this.logMap.get(position)
    if (logEntry != undefined) {
      fs.readSync(this.fdLog, buffer, 0, buffer.byteLength, logEntry.pos)
    } else {
      fs.readSync(this.fd, buffer, 0, buffer.byteLength, position)
    }
  }
  writeDbHeader(state) {
    statistic.dbHeaderWrites += 1
    this.write(this.recordPackers.dbHeader.toBuffer(state), 0)
  }
  readDbHeader() {
    statistic.dbHeaderReads += 1
    this.read(this.recordPackers.dbHeader.buffer, 0)
    return this.recordPackers.dbHeader.fromBuffer()
  }
  writeBucketHeader(state, index) {
    statistic.dbBucketWrites += 1
    this.write(this.recordPackers.dbBucket.toBuffer(state), this.#structure.header + this.#structure.bucket.size * index)
  }
  readBucketHeader(index) {
    statistic.dbBucketReads += 1
    this.read(this.recordPackers.dbBucket.buffer, this.#structure.header + this.#structure.bucket.size * index)
    return this.recordPackers.dbBucket.fromBuffer()
  }
  writeRecord(state, type, ptr) {
    statistic.recordWrites[type] += 1
    this.write(this.recordPackers[type].toBuffer(state), this.#structure.header + this.#structure.bucket.size * ptr.bucketIdx + this.#structure.bucket.header + this.#structure.record[type] * ptr.recIdx)
  }
  readRecord(type, ptr) {
    statistic.recordReads[type] += 1
    this.read(this.recordPackers[type].buffer, this.#structure.header + this.#structure.bucket.size * ptr.bucketIdx + this.#structure.bucket.header + this.#structure.record[type] * ptr.recIdx)
    return this.recordPackers[type].fromBuffer()
  }
  scanLog() {
    /**
     * if first entry not begTx then done
     * collect entries
     * if entry is endTx then apply
     */
    this.logPos = 0
    let logLength = fs.fstatSync(this.fdLog).size
    while (this.logPos < logLength) {
      fs.readSync(this.fdLog, this.recordPackers.logEntry.buffer, { position: this.logPos })
      let txEntry = this.recordPackers.logEntry.fromBuffer()
      this.logPos += 12
      if (txEntry.len == 0) {
        if (txEntry.ofs == -1) {
          this.logMap.clear()
        } else if (txEntry.ofs == -2) {
          this.txNumber = txEntry.txNo
          this.applyLog()
          break
        } else {
          break // eol
        }
      } else {
        this.logMap.set(txEntry.ofs, { pos: this.logPos, len: txEntry.len })
        this.logPos += txEntry.len
      }
    }
  }
  applyLog() {
    assert(this.txDepth == 0, 'Apply log with open transaction')
    for (let entry of this.logMap.entries()) {
      let dbPos = entry[0]
      let logPos = entry[1].pos
      let len = entry[1].len
      fs.readSync(this.fdLog, this.logBuffer, 0, len, logPos)
      fs.writeSync(this.fd, this.logBuffer, 0, len, dbPos)
    }
    fs.ftruncateSync(this.fdLog)
    this.logPos = 0
    this.logMap.clear()
  }
  startTransaction() {
    if (this.txDepth == 0) {
      this.txNumber += 1
      fs.ftruncateSync(this.fdLog)
      fs.writeSync(this.fdLog, this.recordPackers.logEntry.toBuffer({ txNo: this.txNumber, ofs: -1, len: 0 }), 0, 24, 0)
      this.logPos = 12
      this.logMap.clear()
    }
    this.txDepth += 1
  }
  commitTransaction() {
    this.txDepth -= 1
    if (this.txDepth == 0) {
      fs.writeSync(this.fdLog, this.recordPackers.logEntry.toBuffer({ txNo: this.txNumber, ofs: -2, len: 0 }), 0, 24, this.logPos)
      //      fs.fsyncSync(this.fdLog)
      this.applyLog()
    } else if (this.txDepth < 0) {
      throw new Error('Commit transaction without begin')
    }
  }
}
/**
 * manipulates db logical structure
 */
class Database {
  constructor(opts = { maxCacheSize: 0 }) {
    this.dbFile = new DbFile(opts)
    this.dbHeader = new DbHeader(this.dbFile)
    this.btNodeCache = new BtNodeCache(this.dbFile, this, opts)
  }
  //Internal
  getBtNode(nodePtr, readOnly) {
    assert(!nodePtr.isNull(), 'Null pointer in getBtNode')
    return this.btNodeCache.get(nodePtr, readOnly)
  }
  freeBtNode(nodePtr) {
    assert(!nodePtr.isNull(), 'Null recPtr in freeBtNode')
    this.freeRecord(nodePtr, 'i')
    return this.btNodeCache.delete(nodePtr)
  }
  newBucket() {
    let index = this.dbHeader.bucketMap.length
    const bucket = new DbBucket(this.dbHeader, this.dbFile, index)
    bucket.setType('f')
    bucket.write()
    return bucket
  }
  /**
   * Return root BtNode
   *
   * @readonly
   * @type {*}
   */
  get rootBtNode() {
    return this.getBtNode(this.dbHeader.rootBtNodePtr)
  }
  /**
   * Get a list of nodes from rootNode or fromPKey node to node holding key
   * terminating at a leaf if not found.
   *
   * Used to control splitting / merging for tree balancing
   *
   * @param {*} key  Key to find
   * @param {*} fromPKey Starting node to find replacement key. Root node if not specified
   * @returns PKey  specifying node and key index with parent PKeys
   */
  getKeyPath(key, fromPKey) {
    let pKey = fromPKey
    if (!pKey) {
      pKey = this.rootBtNode.nextPKey(key)
    } else if (!pKey.nextPtr.isNull()) {
      pKey = this.getBtNode(fromPKey.nextPtr).nextPKey(key, fromPKey)
    }
    while (!pKey.node.isLeaf()) {
      if (pKey.cmp == 0) {
        break
      } else {
        pKey = this.getBtNode(pKey.prevPtr).nextPKey(key, pKey)
        if (fromPKey) {
          /* looking for replacement */
          this.index = 0
        }
      }
    }
    return pKey
  }
  /**
   * Merge next node and its parent key into previous node,
   * retuning a new parent key if total keys exceeds maxNodeKeys or
   * free left node. Caller for replacing parent key with returned key
   * or removing key if no key returned
   *
   * @param {PKey} pKey Node / index
   * @returns True if parent key removed
   */
  merge(pKey) {
    let merged = false
    let index = Math.min(pKey.index, pKey.node.btKeys.length - 1)
    let prevPtr = (pKey.node.btKeys[index - 1] && pKey.node.btKeys[index - 1].nextPtr) || pKey.node.prevPtr
    let lfNode = this.getBtNode(prevPtr)
    let parentBtKey = pKey.node.btKeys[index]
    let rtNode = this.getBtNode(parentBtKey.nextPtr)
    let btKeys = lfNode.btKeys.concat(parentBtKey).concat(rtNode.btKeys)
    parentBtKey.nextPtr = rtNode.prevPtr
    if (btKeys.length > this.dbFile.structure.key.maxNodeKeys) {
      let split = (btKeys.length / 2) >> 0
      rtNode.btKeys = btKeys.slice(split + 1)
      lfNode.btKeys = btKeys.slice(0, split)
      parentBtKey = btKeys[split]
      rtNode.prevPtr = parentBtKey.nextPtr
      parentBtKey.nextPtr = rtNode.selfPtr
      pKey.node.btKeys[index] = parentBtKey
    } else {
      pKey.node.btKeys.splice(index, 1)
      lfNode.btKeys = btKeys
      this.freeBtNode(rtNode.selfPtr)
      merged = true
    }
    return merged
  }
  /**
   *   BTree API
   *
   */
  /**
   *  Open a database file and initialize internal structure from state in file
   *
   * @param {String} fileName
   * @memberof Database
   */
  open(fileName) {
    this.dbFile.open(fileName)
    this.dbHeader = DbHeader.fromFile(this.dbFile)
    for (let index in this.dbHeader.bucketMap) {
      let bucket = DbBucket.fromFile(this.dbHeader, this.dbFile, index)
      if (bucket.type != 'f' && bucket.recMap.indexOf('f') < 0) {
        this.dbHeader.bucketMap[index] = bucket.type.toUpperCase()
      } else {
        this.dbHeader.bucketMap[index] = bucket.type
      }
    }
  }
  /**
   * Write any outstanding buffers
   * Close file and delete internal structure
   *
   * @memberof Database
   */
  close() {
    this.dbFile.startTransaction()
    this.btNodeCache.commit()
    this.dbHeader.write()
    this.dbFile.commitTransaction()
    this.dbFile.close()
    this.dbHeader = undefined
  }
  /**
   * Initialize database file
   *
   * @param {{}} structure Overrides of default structure
   * @param {Number} allocation Number of buckets to preallocate
   */
  init(fileName, structure = {}) {
    this.dbFile.create(fileName, structure)
    this.dbFile.startTransaction()
    const btNode = this.btNodeCache.getNew()
    this.dbHeader.rootBtNodePtr = btNode.selfPtr
    this.dbHeader.write()
    this.btNodeCache.commit()
    this.dbFile.commitTransaction()
    this.close()
  }
  /**
   * Get a copy of the database structure object
   *
   * @readonly
   * @memberof Database
   */
  get structure() {
    return this.dbFile.cloneStructure()
  }
  /**
   * @description Start a transaction
   * @return {number} transaction level
   * @memberof Database
   */
  startTransaction() {
    return this.dbFile.startTransaction()
  }
  /**
   * @description
   * @return {number} transaction level
   * @memberof Database
   */
  commitTransaction() {
    return this.dbFile.commitTransaction()
  }
  /**
   * Lookup a key an return associated RecPtr
   *
   * @param {String} key
   * @return {RecPtr}
   * @memberof Database
   */
  find(key) {
    let pKey = this.getKeyPath(key)
    let recPtr = this.nullPtr
    if (pKey.cmp == 0) {
      statistic.finds += 1
      recPtr = pKey.node.btKeys[pKey.index].recPtr
    } else {
      statistic.findFails += 1
    }
    return recPtr
  }
  /**
   * Insert a key and it's associated RecPtr
   *
   * @param {String} key Index Key
   * @param {RecPtr} recPtr Returned from BTree.newRecord
   * @returns Response
   */
  insert(key, recPtr) {
    assert(key.length <= this.dbFile.structure.key.maxLen, 'Key too large: ' + key)
    statistic.inserts += 1
    let insertBtKey = (btNode, btKey, index) => {
      btNode.btKeys.splice(index, 0, btKey)
      let parentBtKey
      if (btNode.btKeys.length > this.dbFile.structure.key.maxNodeKeys) {
        const rtNode = this.btNodeCache.getNew()
        let split = (btNode.btKeys.length / 2) >> 0
        parentBtKey = btNode.btKeys[split]
        rtNode.btKeys = btNode.btKeys.slice(split + 1)
        rtNode.prevPtr = parentBtKey.nextPtr
        parentBtKey.nextPtr = rtNode.selfPtr
        btNode.btKeys = btNode.btKeys.slice(0, split)
      }
      return parentBtKey
    }
    let pKey = this.getKeyPath(key)
    let found = pKey.cmp == 0
    if (!found) {
      this.dbFile.startTransaction()
      let newBtKey = new BtKey(key, recPtr)
      while (newBtKey) {
        newBtKey = insertBtKey(pKey.node, newBtKey, pKey.index)
        if (pKey.parent) {
          pKey = pKey.parent
        } else if (newBtKey) {
          let rootNode = this.btNodeCache.getNew()
          rootNode.prevPtr = pKey.node.selfPtr
          this.dbHeader.rootBtNodePtr = rootNode.selfPtr
          this.dbHeader.write()
          insertBtKey(rootNode, newBtKey, 0)
          newBtKey = undefined
          let depth = 0
          pKey = this.getKeyPath(key)
          while (pKey) {
            pKey = pKey.parent
            depth += 1
          }
          statistic.maxDepth = Math.max(statistic.maxDepth, depth)
        }
      }
      this.btNodeCache.commit()
      this.dbFile.commitTransaction()
    } else {
      statistic.insertFails += 1
    }
    return { key: key, recPtr: recPtr, success: !found }
  }
  /**
   * Delete a key and it's associated RecPtr
   *
   * @param {String} key
   * @return {undefined}
   * @memberof Database
   */
  delete(key) {
    statistic.deletes += 1
    let pKey = this.getKeyPath(key)
    let found = pKey.cmp == 0
    let recPtr
    if (found) {
      this.dbFile.startTransaction()
      let btKey = pKey.node.btKeys[pKey.index]
      recPtr = btKey.recPtr
      if (!pKey.node.isLeaf()) {
        let replPKey = this.getKeyPath(key, pKey)
        replPKey.index = 0
        let rbtKey = replPKey.node.btKeys[0]
        btKey.key = rbtKey.key
        btKey.recPtr = rbtKey.recPtr
        pKey = replPKey
      }
      pKey.node.btKeys.splice(pKey.index, 1)
      // merge ancestors
      pKey = pKey.parent
      while (pKey && this.merge(pKey)) {
        pKey = pKey.parent
      }
      if (this.rootBtNode.btKeys.length <= 0 && !this.rootBtNode.isLeaf()) {
        // empty root with children
        let prevPtr = this.rootBtNode.prevPtr
        this.freeBtNode(this.rootBtNode.selfPtr)
        this.dbHeader.rootBtNodePtr = prevPtr
        this.dbHeader.write()
      }
      this.dbFile.commitTransaction()
    } else {
      statistic.deleteFails += 1
    }
    return { key: key, recPtr: recPtr, success: found }
  }
  /**
   * Allocate a new record
   *
   * @param {undefined} [] Record type
   * @return {RecPtr} Handel of the record
   * @memberof Database
   */
  newRecord(type = 'd') {
    let bucket
    let bucketIndex = this.dbHeader.bucketMap.indexOf(type)
    this.dbFile.startTransaction()
    if (bucketIndex >= 0) {
      bucket = DbBucket.fromFile(this.dbHeader, this.dbFile, bucketIndex)
    } else {
      bucketIndex = this.dbHeader.bucketMap.indexOf('f')
      if (bucketIndex >= 0) {
        bucket = DbBucket.fromFile(this.dbHeader, this.dbFile, bucketIndex)
      } else {
        bucket = this.newBucket()
      }
      bucket.setType(type)
    }
    let index = bucket.getFreeRecord()
    bucket.write()
    this.dbFile.commitTransaction()
    return new RecPtr(bucket.index, index)
  }
  /**
   * Free a record from the database
   *
   * @param {RecPtr} recPtr
   * @param {char} [type=undefined]
   * @memberof Database
   */
  freeRecord(recPtr, type = undefined) {
    let bucket = DbBucket.fromFile(this.dbHeader, this.dbFile, recPtr.bucketIdx)
    assert(!type || bucket.type == type, 'Pointer wrong type')
    this.dbFile.startTransaction()
    bucket.freeRecord(recPtr.recIdx)
    bucket.write()
    this.dbFile.commitTransaction()
  }
  /**
   * Return the contents of a record for given handel
   *
   * @param {RecPtr} recPtr
   * @return {undefined}
   * @memberof Database
   */
  readRecord(recPtr) {
    return this.dbFile.readRecord('d', recPtr)
  }
  /**
   * Write the contents of a record for a given handel
   *
   * @param {Object} data JSON Object
   * @param {RecPtr} recPtr
   * @return {undefined}
   * @memberof Database
   */
  writeRecord(data, recPtr) {
    return this.dbFile.writeRecord(data, 'd', recPtr)
  }
  /**
   * Get/Set record packer manifest for data records
   *
   * @memberof Database
   */
  set rpManifest(manifest) {
    this.dbFile.recordPackers.d = new RecordPacker(manifest)
    if (this.dbFile.structure?.record?.d) this.dbFile.recordPackers.d.bufSize = this.dbFile.structure.record.d
  }
  get rpManifest() {
    return JSON.parse(JSON.stringify(this.dbFile.recordPackers.d.manifest))
  }
  /**
   * Return an iterator over all keys
   *
   * @return {Iterable}
   * @memberof Database
   */
  keys() {
    let pKey = new PKey(this.rootBtNode)
    pKey.index = -1
    let nextPtr
    let db = this
    return {
      [Symbol.iterator]() {
        return {
          next: () => {
            while (pKey && pKey.index >= pKey.node.btKeys.length) {
              pKey = pKey.parent
            }
            if (pKey == undefined || (!pKey.parent && pKey.node.btKeys.length <= 0)) {
              return { done: true, value: undefined }
            }
            if (pKey.index < 0) {
              pKey.index = 0
              while (!pKey.node.isLeaf()) {
                pKey = new PKey(db.getBtNode(pKey.node.prevPtr, true), pKey, 0)
              }
            }
            let btKey = pKey.node.btKeys[pKey.index]
            nextPtr = btKey.nextPtr
            pKey.index += 1
            if (!nextPtr.isNull()) {
              pKey = new PKey(db.getBtNode(nextPtr, true), pKey, -1)
            }
            return { done: false, value: btKey.key.slice(0) }
          }
        }
      }
    }
  }
  /**
   * Create a new copy of the database with a modified structure
   *
   * @param {String} newFileName
   * @param {{}} struct
   * @return {undefined}
   * @memberof Database
   */
  reStructure(newFileName, struct, opts) {
    let newDb = new Database(opts)
    let dbStructure = DbStructure.defaultStructure
    dbStructure.apply(this.structure)
    dbStructure.apply(struct)
    newDb.init(newFileName, dbStructure.structure)
    newDb.rpManifest = this.rpManifest
    newDb.open(newFileName)
    for (let key of this.keys()) {
      let recPtr = this.find(key)
      let newRecPtr = newDb.newRecord()
      newDb.insert(key, newRecPtr)
      newDb.writeRecord(this.readRecord(recPtr), newRecPtr)
    }
    newDb.close()
    return newDb
  }
  /**
   * #Testing
   */
  /**
   * Check the structural integrity of the database throwing error on anomaly
   *
   * @memberof Database
   */
  checkIntegrity() {
    const rootNode = this.rootBtNode
    const btKeyMap = new Map()
    const nodeStack = []
    let lastKey = ''
    let ctx = { node: rootNode, index: -1 }
    nodeStack.push(ctx)
    while (nodeStack.length > 0) {
      ctx = nodeStack.pop()
      if (ctx.index < 0) {
        ctx.index += 1
        nodeStack.push(ctx)
        if (!ctx.node.isLeaf) {
          nodeStack.push({ node: this.getBtNode(node.prevPtr, true), index: -1 })
        }
      } else {
        assert(ctx.node == rootNode || ctx.node.btKeys.length > 0, 'Empty node')
        if (ctx.index < ctx.node.btKeys.length) {
          let btKey = ctx.node.btKeys[ctx.index]
          assert(btKey.key > lastKey, 'key out of order: ' + btKey.key)
          lastKey = btKey.key
          assert(!btKeyMap.has(btKey.key), 'Duplicate key: ' + btKey.key)
          assert(
            (!btKey.nextPtr.isNull() && !ctx.node.isLeaf()) || (btKey.nextPtr.isNull() && ctx.node.isLeaf()),
            'Null next key not in leaf: ' + btKey.key + ' ' + ctx.node.prevPtr.state.toString() + ' ' + ctx.node.prevPtr.state.toString()
          )
          btKeyMap.set(btKey.key, btKey)
          ctx.index += 1
          nodeStack.push(ctx)
          if (!ctx.node.isLeaf()) {
            nodeStack.push({ node: this.getBtNode(btKey.nextPtr, true), index: -1 })
          }
        }
      }
    }
  }
  /**
   * Print an description of bucket contents
   *
   * @memberof Database
   */
  dumpBuckets() {
    console.log('Db Header')
    console.log(inspect(this.dbHeader.state))
    console.log(inspect({ bucketMap: this.dbHeader.bucketMap.join('') }))
    console.log('Buckets')
    this.dbHeader.bucketMap.forEach((_, idx) => {
      const bucket = DbBucket.fromFile(this.dbHeader, this.dbFile, idx)
      let state = bucket.state
      state.recMap = state.recMap.join('')
      if (bucket.type != 'f') console.log(inspect(state))
    })
  }
  /**
   * Print a formatted list of BtNode contents
   *
   * @memberof Database
   */
  dumpIndex() {
    let keyLen = this.dbFile.structure.key.maxLen
    let dump = (btNode, level) => {
      let format = (ptr) => {
        let id = '     null'
        if (ptr.bucketIdx >= 0) id = ('0000000000' + ptr.bucketIdx).slice(-4) + '.' + ('0000000000' + ptr.recIdx).slice(-4)
        return id
      }
      if (!btNode.isLeaf()) {
        dump(this.getBtNode(btNode.prevPtr, true), level + 1)
      }
      for (let btKey of btNode.btKeys) {
        console.log(format(btNode.selfPtr) + ' |'.repeat(level) + ' ' + (btKey.key + ' '.repeat(keyLen)).slice(0, keyLen) + ' : ' + format(btKey.recPtr) + ' : ' + format(btKey.nextPtr))
        if (!btKey.nextPtr.isNull()) {
          dump(this.getBtNode(btKey.nextPtr, true), level + 1)
        }
      }
    }
    dump(this.rootBtNode, 1)
  }
}
/**
 * Read/Write internal state to external storage\
 *
 * @class JsBtree
 */
export default class JsBtree {
  #database
  constructor(opts = { maxCacheSize: 0 }) {
    this.#database = new Database(opts)
  }
  /**
   *  Open a database file and initialize internal structure from state in file
   *
   * @param {String} fileName
   * @memberof JsBtree
   */
  open(fileName) {
    return this.#database.open(fileName)
  }
  /**
   * Write any outstanding buffers
   * Close file and delete internal structure
   *
   * @memberof JsBtree
   */
  close() {
    return this.#database.close()
  }
  /**
   * Initialize JsBtree file
   *
   * @param {{}} structure Overrides of default structure
   * @param {Number} allocation Number of buckets to preallocate
   */
  init(fileName, structure = {}) {
    return this.#database.init(fileName, structure)
  }
  /**
   * Get a copy of the JsBtree structure object
   *
   * @readonly
   * @memberof JsBtree
   */
  get structure() {
    return this.#database.structure
  }
  /**
   * @description Start a transaction
   * @return {number} transaction level
   * @memberof Database
   */
  startTransaction() {
    return this.#database.startTransaction()
  }
  /**
   * @description
   * @return {number} transaction level
   * @memberof Database
   */
  commitTransaction() {
    return this.#database.commitTransaction()
  }
  /**
   * Lookup a key an return associated RecPtr
   *
   * @param {String} key
   * @return {RecPtr}
   * @memberof JsBtree
   */
  find(key) {
    return this.#database.find(key)
  }
  /**
   * Insert a key and it's associated RecPtr
   *
   * @param {String} key Index Key
   * @param {RecPtr} recPtr Returned from BTree.newRecord
   * @memberof JsBtree
   * @returns Response
   */
  insert(key, recPtr) {
    return this.#database.insert(key, recPtr)
  }
  /**
   * Delete a key and it's associated RecPtr
   *
   * @param {String} key
   * @return {undefined}
   * @memberof JsBtree
   */
  delete(key) {
    return this.#database.delete(key)
  }
  /**
   * Allocate a new record
   *
   * @return {RecPtr} Handel of a new data record
   * @memberof JsBtree
   */
  newRecord() {
    return this.#database.newRecord('d')
  }
  /**
   * Free a record from the JsBtree
   *
   * @param {RecPtr} recPtr
   * @param {char} [type=undefined]
   * @memberof JsBtree
   */
  freeRecord(recPtr) {
    return this.#database.freeRecord(recPtr, 'd')
  }
  /**
   * Return the contents of a record for given handel
   *
   * @param {RecPtr} recPtr
   * @return {undefined}
   * @memberof JsBtree
   */
  readRecord(recPtr) {
    return this.#database.readRecord(recPtr)
  }
  /**
   * Write the contents of a record for a given handel
   *
   * @param {Object} data JSON Object
   * @param {RecPtr} recPtr
   * @return {undefined}
   * @memberof JsBtree
   */
  writeRecord(data, recPtr) {
    return this.#database.writeRecord(data, recPtr)
  }
  /**
   * Get/Set record packer manifest for data records
   *
   *
   * @memberof JsBtree
   */
  set rpManifest(manifest) {
    this.#database.rpManifest = manifest
  }
  get rpManifest() {
    return this.#database.rpManifest
  }
  /**
   * Return an iterator over all keys
   *
   * @return {Iterable}
   * @memberof JsBtree
   */
  keys() {
    return this.#database.keys()
  }
  /**
   * Create a new copy of the JsBtree with a modified structure
   *
   * @param {String} newFileName
   * @param {{}} struct
   * @return {undefined}
   * @memberof JsBtree
   */
  reStructure(newFileName, struct, opts) {
    return this.#database.reStructure(newFileName, struct, opts)
  }
  /**
   * Check the structural integrity of the database throwing error on anomaly
   *
   * @memberof JsBtree
   */
  checkIntegrity() {
    return this.#database.checkIntegrity()
  }
  /**
   * Print an description of bucket contents
   *
   * @memberof JsBtree
   */
  dumpBuckets() {
    return this.#database.dumpBuckets()
  }
  /**
   * Print a formatted list of BtNode contents
   *
   * @memberof JsBtree
   */
  dumpIndex() {
    return this.#database.dumpIndex()
  }
}
/**
 * Unit Testing
 *
 */
/**
 * Poor quality Pseudo Random number generator\
 * The poor quality of the generator causes duplicate keys
 * which test insert/delete fail cases
 */
class PRandom {
  constructor(seed) {
    this.seed = seed || 1
  }
  /**
   * get next pseudo random number
   *
   * @param {number} range max value
   * @returns {number}
   */
  random(range) {
    this.seed += Math.abs(Math.sin(this.seed))
    this.seed -= this.seed >> 0
    return (this.seed * range) >> 0
  }
  /**
   * get next random key
   *
   * @param {number} size
   * @returns {string}
   */
  randomKey(size) {
    let key = ''
    const characters = 'abcdefghijklmnopqrstuvwxyz'
    while (size > 0) {
      key += characters.charAt(this.random(characters.length))
      size -= 1
    }
    return key
  }
}
/**
 * Unit Test JsBtree
 *
 * @export
 * @param {string} [dbFilePath='UnitTest.dat']
 * @param {number} [numRecs=40]
 * @param {number} [detail=0]
 * @param {{}} [struct=DbStructure.defaultStructure.structure]
 * @param {{ maxCacheSize: number; }} [opts={ maxCacheSize: 0 }]
 * @param {{}} [reStruct=undefined]
 * @param {{}} [rsOpts=undefined]
 */
export function unitTest(dbFilePath = 'UnitTest.dat', numRecs = 40, detail = 0, struct = DbStructure.defaultStructure.structure, opts = { maxCacheSize: 0 }, reStruct = undefined, rsOpts = undefined) {
  let start = Date.now()
  let elapsed = () => {
    return (
      ' '.repeat(20) +
      ((Date.now() - start) / 1000).toLocaleString(undefined, {
        minimumFractionDigits: 3
      }) +
      ': '
    ).slice(-12)
  }
  let log = (message, level) => {
    if (level <= detail) {
      console.log(elapsed() + message)
    }
  }
  let rpManifest = [
    { name: 'id', type: 'uint' },
    { name: 'key', type: 'string' }
  ]
  log('Initialize db', 0)
  let db = new JsBtree(opts)
  db.init(dbFilePath, struct, 5)
  log('Open db', 0)
  db = new JsBtree(opts)
  db.rpManifest = rpManifest
  db.open(dbFilePath)
  const keySize = db.structure.key.maxLen
  if (detail > 2) {
    db.dumpBuckets()
    db.dumpIndex()
  }
  let pr = new PRandom(1)
  log('Insert Keys', 0)
  for (let idx = 0; idx < numRecs; idx += 1) {
    let key = pr.randomKey(keySize)
    db.startTransaction()
    let recPtr = db.newRecord()
    log(key, 2)
    let result = db.insert(key, recPtr)
    if (result.success) {
      db.writeRecord({ id: idx, key: key }, result.recPtr)
    } else {
      log('Failed to insert key', 2)
      db.freeRecord(result.recPtr)
    }
    if (detail > 3) {
      db.dumpIndex()
    }
    if (detail > 3) {
      db.checkIntegrity()
    }
    db.commitTransaction()
  }
  if (detail > 2) {
    db.dumpIndex()
  }
  if (detail > 0) {
    db.dumpBuckets()
  }
  log('Close db', 0)
  db.close()
  log('Re-open db', 0)
  db = new JsBtree(opts)
  db.rpManifest = rpManifest
  db.open(dbFilePath)
  log('Check Integrity', 0)
  db.checkIntegrity()
  log('Iterate keys', 0)
  for (let key of db.keys()) {
    log(inspect(db.readRecord(db.find(key))), 1)
  }
  log('Statistics', 0)
  log(inspect(statistic), 1)
  log(inspect(db.structure), 1)
  if (reStruct) {
    log('Restructure', 0)
    dbFilePath += '.dat'
    opts = rsOpts || opts
    db.reStructure(dbFilePath, reStruct, opts)
    db.close()
  }
  log(inspect(process.memoryUsage()), 1)
  log('Open new JsBtree', 0)
  db = new JsBtree(opts)
  db.rpManifest = rpManifest
  db.open(dbFilePath)
  pr.seed = 1
  log('Delete Keys', 0)
  for (let idx = 0; idx < numRecs; idx += 1) {
    let key = pr.randomKey(keySize)
    log(key, 2)
    let job = db.delete(key)
    if (job.recPtr) {
      db.freeRecord(job.recPtr)
    }
    if (detail > 2) {
      db.dumpIndex()
    }
    if (detail > 3) {
      db.checkIntegrity()
    }
  }
  if (detail > 2) {
    db.dumpIndex()
  }
  if (detail > 0) {
    log('Buckets', 0)
    db.dumpBuckets()
  }
  log('Statistics', 0)
  log(inspect(statistic), 0)
  log(inspect(db.structure), 1)
  db.close()
}
