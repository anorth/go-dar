package dar

import (
	"bytes"
	"encoding/binary"
	"hash"
	"io"
	"os"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	multihash "github.com/multiformats/go-multihash"
	"golang.org/x/crypto/sha3"
	"golang.org/x/xerrors"
)

// TODO:
// - wrap errors
// - handle RAW CIDs: no block data or indirection
// - spill allCids and offsets to disk when too large for memory

///// Writer

type Writer struct {
	// Whether to accumulate and write an index at the end of the archive.
	index bool
	// Stream to which archive is written.
	writer countingWriter
	// Optional function to call when the archive is closed.
	closer io.Closer

	// CIDs of DAG roots, in order of insertion. Elements are distinct.
	roots []cid.Cid
	// CIDs of all blocks (including roots), in order of insertion. Elements are distinct.
	allCids []cid.Cid

	// Offset in the stream of the start of the blocks section.
	blocksOffset int64

	// Maps CIDs of all blocks written to their offset in the stream.
	// The map enforces distinctness; each CID/block appears only once
	offsets map[cid.Cid]int64

	// A digest of the bytes of the CIDs of all blocks written, in order.
	digest     hash.Hash
	digestCode uint64

	// Stack of blocks expected to be written next, based on a DFS traversal of
	// the links of blocks already written.
	expectedBlocks []cidlink.Link
}

func NewWriter(index bool) *Writer {
	// Ensure the function and code match.
	// The multihash library doesn't provide pre-registered table for streaming hash functions.
	return &Writer{
		index:      index,
		offsets:    map[cid.Cid]int64{},
		digest:     sha3.New512(),
		digestCode: multihash.SHA2_256,
	}
}

// Creates or truncates a file at path and writes the header.
// The file will be closed when archive is closed.
func (wr *Writer) OpenArchiveFile(path string) error {
	if wr.writer.underlying != nil {
		return xerrors.Errorf("already opened")
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	wr.closer = f
	return wr.OpenArchive(f)
}

// Initializes an archive writer with a stream, writing the header to the stream and preparing to write blocks.
func (wr *Writer) OpenArchive(iow io.Writer) error {
	wr.writer = countingWriter{
		underlying:   iow,
		bytesWritten: 0,
	}
	if err := wr.writeHeader(); err != nil {
		return err
	}
	wr.blocksOffset = wr.writer.bytesWritten
	return nil
}

// Writes the index (if configured) and trailer to the stream.
func (wr *Writer) CloseArchive() error {
	// Optionally write the index
	indexOffset := int64(-1)
	if wr.index {
		indexOffset = wr.writer.bytesWritten
		if err := wr.writeIndex(); err != nil {
			return err
		}
	}

	if err := wr.writeTrailer(indexOffset); err != nil {
		return err
	}

	if wr.closer != nil {
		if err := wr.closer.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Appends a new root block to the archive.
// Returns the links discovered in this node, in order, which are eligible to be
// written next.
func (wr *Writer) BeginDAG(rootCID cid.Cid, rootBlock ipld.Node, enc codec.Encoder) ([]ipld.Link, error) {
	// Require the new root to be distinct from any block already seen.
	// Note that this rejects an attempt to define a block already written as a root of a new DAG.
	// If the DAGs had been written in the opposite order, both could succeed (and the child -DAG would be
	// referenced from the parent DAG).
	if _, found := wr.offsets[rootCID]; found {
		return nil, xerrors.Errorf("block %v already written", rootCID)
	}

	// Empty the stack of expected blocks from the prior DAG (if any).
	wr.expectedBlocks = nil

	// Record the root CID.
	wr.roots = append(wr.roots, rootCID)

	// Write the block.
	return wr.appendBlock(rootCID, rootBlock, enc)
}

// Appends a block to the archive.
// Returns the links discovered in this node, in order, which are eligible to be written next.
// Links returned here must be provided before any links returned from previous calls
// (i.e. a depth-first traversal).
// TODO: more docs about this
// - if block has been written previously, no links will be returned. decision about what sub-dag to write
//   must have been made the first time.
func (wr *Writer) AppendBlock(c cid.Cid, block ipld.Node, enc codec.Encoder) ([]ipld.Link, error) {
	// Pop expected blocks from the stack until we find this one.
	for len(wr.expectedBlocks) > 0 {
		top := len(wr.expectedBlocks) - 1
		popped := wr.expectedBlocks[top]
		wr.expectedBlocks = wr.expectedBlocks[:top]
		if popped.Cid == c {
			break
		}
	}

	// TODO: can we restore state so that an error here is recoverable?
	if len(wr.expectedBlocks) == 0 {
		return nil, xerrors.Errorf("unexpected cid %v in depth-first traversal, not linked from ancestor block", c)
	}

	return wr.appendBlock(c, block, enc)
}

//
// Internal implementation.
//

func (wr *Writer) writeHeader() error {
	// TODO: consider moving this magic inside the DAG-CBOR object, as first element in the list.
	// Write magic bytes for sniffing.
	_, err := wr.writer.Write([]byte("dar\x01"))
	if err != nil {
		return err
	}

	// Write metadata object as DAG-CBOR
	if metadata, err := buildMetadata(wr.index); err != nil {
		return err
	} else if err := dagcbor.Encoder(metadata, &wr.writer); err != nil {
		return err
	}
	return nil
}

func (wr *Writer) appendBlock(c cid.Cid, block ipld.Node, enc codec.Encoder) ([]ipld.Link, error) {
	// If the block (and hence its sub-DAG) has already been written in this archive,
	// insert a pointer to it.
	if offset, found := wr.offsets[c]; found {
		// Write the absolute offset at which the block is already written, negated.
		_, err := wr.writeVarint(-offset)
		return nil, err
	}

	// Record the block CID and offset at which it will be written.
	wr.allCids = append(wr.allCids, c)
	wr.offsets[c] = wr.writer.bytesWritten

	// Encode to a buffer, so we can calculate the serialized length.
	// TODO: avoid this buffer when the underlying node already knows its serialised length.
	var blockBuf bytes.Buffer
	if err := enc(block, &blockBuf); err != nil {
		return nil, err
	}
	// TODO: optionally verify the CID
	// Should be be computing the CID here, or accepting a function argument?

	// Write length and block data.
	if _, err := wr.writeVarint(int64(blockBuf.Len())); err != nil {
		return nil, err
	}
	// Write block data.
	if _, err := wr.writer.Write(blockBuf.Bytes()); err != nil {
		return nil, err
	}

	// Accumulate digest
	if _, err := wr.digest.Write(c.Bytes()); err != nil {
		return nil, err
	}

	// Push links to children onto the stack.
	links, err := wr.pushLinks(c, block)
	if err != nil {
		return nil, err
	}

	return links, nil
}

func (wr *Writer) writeIndex() error {
	panic("not implemented")
}

// Writes the archive trailer to the stream.
func (wr *Writer) writeTrailer(indexOffset int64) error {
	trailerOffset := wr.writer.bytesWritten
	// Write the number of root CIDs to follow.
	if _, err := wr.writeVarint(int64(len(wr.roots))); err != nil {
		return err
	}
	// Write sequence of root CIDs, with no delimiter
	for _, r := range wr.roots {
		if _, err := wr.writer.Write(r.Bytes()); err != nil {
			return err
		}
	}

	// Write digest of block CIDs (fixed width)
	if _, err := wr.writer.Write(wr.digest.Sum([]byte{})); err != nil {
		return err
	}

	// Write offset of the index section (fixed width)
	if err := wr.writeInt64(indexOffset); err != nil {
		return err
	}

	//Write offset of this trailer section (fixed width)
	if err := wr.writeInt64(trailerOffset); err != nil {
		return err
	}
	return nil
}

// Writes a varint to the underlying stream.
func (wr *Writer) writeVarint(i int64) (int, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, i)
	return wr.writer.Write(buf[:n])
}

// Writes an int64 to the underlying stream (always 8 bytes).
func (wr *Writer) writeInt64(i int64) error {
	return binary.Write(&wr.writer, binary.BigEndian, i)
}

// Extracts links from a block and pushes them onto the the stack of expected blocks.
// The new links will appear at the top of the stack, in traversal order.
func (wr *Writer) pushLinks(c cid.Cid, block ipld.Node) ([]ipld.Link, error) {
	links, err := traversal.SelectLinks(block)
	if err != nil {
		return nil, err
	}
	for _, lnk := range links {
		cidLnk, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, xerrors.Errorf("in block %v expected CID link, got %v", c, lnk)
		}
		wr.expectedBlocks = append(wr.expectedBlocks, cidLnk)
	}
	return links, nil
}

//
// Helpers
//

// Wraps a writer to count the bytes written.
// This structure is necessary to count bytes when passing a writer to another
// function if that method does not return the number of bytes written.
type countingWriter struct {
	underlying   io.Writer
	bytesWritten int64
}

// Writes bytes to the underlying stream and updates the count of bytes written.
func (c *countingWriter) Write(bs []byte) (n int, err error) {
	if n, err := c.underlying.Write(bs); err != nil {
		return 0, err
	} else {
		c.bytesWritten = c.bytesWritten + int64(n)
	}
	return n, err
}
