# JsBTree

Js implementation of BTree database

>### just for fun 
>
>This package has no real world use.
>

## Interface

The JsBtree class provides basic insert, delete, find, iterate operations on the btree. 

Keys are strings.

Data records are user defined and are converted to/from external storage according to a record packer manifest.

[API](./jsbtree.md)


## Database Structure
The file layout of the database consists of fixed size blocks.

There is a single header which holds the structure of the database. Followed by 
fixed size buckets. Each bucket has a header holding the state of the 
records in the bucket. Records hold either data or index. The number of records depends on the record type. 
There may be unused space at the end of the bucket corresponding to a fractional record.


    +------------------+
    |  DbHeader        |
    +------------------+
      |  DbBucket      |
      +----------------+
        |  Record      |
        +--------------+
        ...         
        +--------------+
        |  Record      |
      +----------------+
      |  DbBucket      |
      +----------------+
        |  Record      |
        +--------------+
        ...
        +--------------+
        |  Record      |
        +--------------+
      ...


## Record Packer

Internal state of the btree is save to external storage.

Each class instance provides a state object from which it may be re-instantiated.

The state object is a simple object consisting of int, uint, char, string, array types as well as named child objects.

The record packer packs/unpacks these objects to a buffer which is written to external storage according to a manifest provided by the object classes.

The following object will be packed using the manifest. Note the order of fields in the packed object need not match the given object
    

    Object

    { name: "box",
      dim: {
        x: 0,
        y:1
      }
    }

    Manifest

    [
      { name: 'dim', type: 'record', of: [
        { name: 'x', type: 'int' }
        { name: 'y', type: 'int' }
      ] },
      { name: 'name', type: 'string' }
    ]
