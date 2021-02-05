# The DAG Archive format (DAR)

A DAG ARchive (DAR) is a stream of serialized IPLD nodes forming content-addressed DAGs, 
with an optional index for random block access.
The DAR format is a successor the CAR format 
([spec](https://github.com/ipld/specs/blob/master/block-layer/content-addressable-archives.md),
[implementation](https://github.com/ipld/go-car)),
with some stricter requirements on block ordering enabling simpler access patterns and some space savings.

Use DAR if:
- you need to serialize one or more DAGs for transmission or storage
- you wish to extract portions of an archive with mechanical sympathy or while streaming
- you need an index for random-access block lookups

Don't use DAR if you just need a container for a bunch of content-addressed blocks
that don't form connected DAGs. Use CAR instead.

## Target use cases
The DAR format was motivated by two distinct but related use cases:
1. Enabling serving of arbitrary IPLD DAGs [from HTTP gateways](https://github.com/ipfs/in-web-browsers/issues/170)
and similar APIs
2. Supporting efficient partial extraction from [Filecoin pieces](https://spec.filecoin.io/#section-systems.filecoin_files.piece), 
while maintaining a streaming write path

## Features
- Streaming write: output a DAR in one pass to a stream or socket
- Built-in block de-duplication: just send your blocks (in order), the writer will figure it out
- Supports partial DAGs, dangling links
- Depth-first traversal order when reading archive
- Multiple DAGs in one archive, including overlapping ones
- About 6% smaller than a CAR for mostly-connected graphs, or ~same size with an index (free!)
- Optional index at the end of the archive (not yet implemented)
- Streaming read for simple traversal (not yet implemented)

## Format overview
The DAR format writes blocks in strict depth-first search traversal order. Unlike CAR,
blocks are not preceeded by their CID because (with the exception of DAG roots) that CID has always
been observed previously in a parent block.

Each block is written only the first time it appears in DFS order. Subsequent links to the same
block are written as an offset to the stream where the block (and all its children) first appeared.

Most metadata is written into a trailer, after the block data, rather than header, supporting the streaming write.
This includes CIDs and offsets of roots.

An index of CIDs and offsets of the corresponding blocks, sorted by CID, optionally appears before the trailer.
Note that, thanks to the strict traversal order, 
extracting a sub-DAG requires only a single index lookup for the top node.