package dar

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash"
	"io"
	"os"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"golang.org/x/xerrors"
)

// TODO:
// - wrap errors
// - handle RAW CIDs: no block data or indirection
// - spill allCids and blocksOffset to disk when too large for memory

///// Writer

// Writer writes IPLD blocks to an archive file.
//
// DAGs are written in strict depth-first order from some root. The writer enforces this, keeping track
// of the links seen in nodes written so far, and accepting subsequent blocks only in accordance with a
// depth-first traversal.
// A DAG need not be complete: the caller need not provide a block for every link, but those that are provided
// must be in the expected order.
// Blocks are deduplicated internally. The same block may be provided multiple times, assuming it is linked
// multiple times, and should generally be provided each time (but will only be encoded and written once).
//
// An archive may contain multiple DAGs. The DAG roots must be distinct. A block that has been written as
// part of one DAG may not subsequently be used as a root. Thus, to the extent that any DAGs are contained
// within another, they must be provided in depth-first order.
//
// The archive writer optionally writes an index at the end of the file, enabling subsequent rapid look-up
// of any block by a Reader.
type Writer struct {
	// Whether to accumulate and write an index at the end of the archive.
	makeIndex bool
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

// Creates or truncates an archive file at a path, and writes the header.
// The file will be closed when archive is closed.
func NewFileWriter(path string, index bool) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	wr, err := NewWriter(f, index)
	if err != nil {
		return nil, err
	}
	wr.closer = f
	return wr, err
}

// Initializes an archive writer with a stream, writing the header and preparing to write blocks.
func NewWriter(iow io.Writer, index bool) (*Writer, error) {
	wr := &Writer{
		makeIndex: index,
		writer: countingWriter{
			underlying:   iow,
			bytesWritten: 0,
		},
		offsets:    map[cid.Cid]int64{},
		digest:     digestFactory(),
		digestCode: digestMultiHashType,
	}

	if err := wr.writeHeader(); err != nil {
		return nil, err
	}
	wr.blocksOffset = wr.writer.bytesWritten
	return wr, nil
}

// Appends a new DAG root block to the archive.
// Any expected blocks from a prior DAG are forgotten and may no longer be appended, unless they are contained in.
// of this or a subsequent DAG.
// Returns the links discovered in this node, in order, which are eligible to be
// written next.
func (wr *Writer) BeginDAG(ctx context.Context, rootBlock ipld.Node, enc cid.Prefix) (cidlink.Link, []cidlink.Link, error) {
	if err := wr.checkOpen(); err != nil {
		return cidlink.Link{}, nil, err
	}

	// Write the block.
	link, links, err := wr.appendBlock(ctx, rootBlock, enc, true)
	if err != nil {
		return cidlink.Link{}, nil, err
	}

	// Empty the stack of expected blocks from the prior DAG (if any).
	wr.expectedBlocks = nil

	return link, links, nil
}

// Appends a block to the archive.
// Returns the links discovered in this node, in order, which are eligible to be written next.
// Links returned here must be provided before any links returned from previous calls
// (i.e. a depth-first traversal).
// TODO: more docs about this
// - if block has been written previously, no links will be returned. decision about what sub-dag to write
//   must have been made the first time.
func (wr *Writer) AppendBlock(ctx context.Context, block ipld.Node, enc cid.Prefix) (cidlink.Link, []cidlink.Link, error) {
	if err := wr.checkOpen(); err != nil {
		return cidlink.Link{}, nil, err
	}

	return wr.appendBlock(ctx, block, enc, false)
}

func (wr *Writer) AppendRawBlock(ctx context.Context, link cidlink.Link, data []byte) ([]cidlink.Link, error) {
	if err := wr.checkOpen(); err != nil {
		return nil, err
	}

	return wr.appendRawBlock(ctx, link, data, false)
}

// Writes the index (if configured) and trailer to the stream.
func (wr *Writer) Finish() error {
	if err := wr.checkOpen(); err != nil {
		return err
	}

	// Optionally write the index
	indexOffset := int64(-1)
	if wr.makeIndex {
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

	wr.writer.underlying = nil
	wr.closer = nil
	return nil
}

func (wr *Writer) checkOpen() error {
	if wr.writer.underlying == nil {
		return xerrors.Errorf("writer is closed")
	}
	return nil
}

//
// Internal implementation.
//

func (wr *Writer) writeHeader() error {
	// TODO: consider moving this magic inside the DAG-CBOR object, as first element in the list.
	// Write magic bytes for sniffing.
	_, err := wr.writer.Write([]byte(magic))
	if err != nil {
		return err
	}

	// Write metadata object as DAG-CBOR
	meta := archiveMeta{hasIndex: wr.makeIndex}
	if err := meta.Serialize(&wr.writer); err != nil {
		return err
	}
	return nil
}

func (wr *Writer) appendBlock(ctx context.Context, block ipld.Node, enc cid.Prefix, root bool) (cidlink.Link, []cidlink.Link, error) {
	inLink, data, err := encodeBlock(ctx, block, enc)
	if err != nil {
		return cidlink.Link{}, nil, err
	}

	if err := wr.receiveBlockData(inLink.Cid, data, root); err != nil {
		return cidlink.Link{}, nil, err
	}

	// Push links to children onto the stack.
	outLinks, err := wr.pushLinks(block)
	if err != nil {
		return cidlink.Link{}, nil, err
	}

	return inLink, outLinks, nil
}

func (wr *Writer) appendRawBlock(ctx context.Context, lnk cidlink.Link, data []byte, root bool) ([]cidlink.Link, error) {
	block, err := decodeBlock(ctx, lnk, data)
	if err != nil {
		return nil, err
	}

	if err := wr.receiveBlockData(lnk.Cid, data, root); err != nil {
		return nil, err
	}

	// Push links to children onto the stack.
	outLinks, err := wr.pushLinks(block)
	if err != nil {
		return nil, err
	}

	return outLinks, nil
}

func (wr *Writer) receiveBlockData(c cid.Cid, data []byte, root bool) error {
	// For root blocks, check uniqueness before writing (rather than write a pointer).
	if root {
		// Require a new root to be distinct from any block already seen.
		// Note that this rejects an attempt to define a block already written as a root of a new DAG.
		// If the DAGs had been written in the opposite order, both could succeed (and the child DAG would be
		// referenced from the parent DAG).
		if _, found := wr.offsets[c]; found {
			return xerrors.Errorf("block %v proposed as root already written", c)
		}

		// Record the root CID.
		wr.roots = append(wr.roots, c)
	}

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
		return xerrors.Errorf("unexpected cid %v in depth-first traversal, not linked from ancestor block", c)
	}

	// Write block data to the stream.
	if err := wr.writeBlock(c, data); err != nil {
		return err
	}
	return nil
}

func (wr *Writer) writeBlock(c cid.Cid, data []byte) error {
	// If the block (and hence its sub-DAG) has already been written in this archive,
	// insert a pointer to it.
	if offset, found := wr.offsets[c]; found {
		// Write the absolute offset at which the block is already written, negated.
		_, err := wr.writeVarint(-offset)
		return err
	}

	// Record the block CID and offset at which it will be written.
	wr.allCids = append(wr.allCids, c)
	wr.offsets[c] = wr.writer.bytesWritten

	// Write length and block data.
	if _, err := wr.writeVarint(int64(len(data))); err != nil {
		return err
	}
	// Write block data.
	if _, err := wr.writer.Write(data); err != nil {
		return err
	}
	// Accumulate digest
	if _, err := wr.digest.Write(c.Bytes()); err != nil {
		return err
	}
	return nil
}

func (wr *Writer) writeIndex() error {
	panic("not yet implemented")
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

	// Write digest of block CIDs (fixed width, may depend on version)
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

// Writes a varint to the stream.
func (wr *Writer) writeVarint(i int64) (int, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, i)
	return wr.writer.Write(buf[:n])
}

// Writes an int64 to the stream (always 8 bytes).
func (wr *Writer) writeInt64(i int64) error {
	return binary.Write(&wr.writer, binary.BigEndian, i)
}

// Pushes pushes them onto the the stack of expected blocks.
// The new links will appear at the top of the stack, in traversal order.
// Returns the links as CID links, in the same order they were provided.
func (wr *Writer) pushLinks(block ipld.Node) ([]cidlink.Link, error) {
	links, err := traversal.SelectLinks(block)
	if err != nil {
		return nil, err
	}
	cidLinks := make([]cidlink.Link, len(links))

	// Traverse links backwards to push onto the stack.
	for i := len(links) - 1; i >= 0; i-- {
		lnk := links[i]
		cidLnk, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, xerrors.Errorf("expected CID link, got %v", lnk)
		}
		cidLinks[i] = cidLnk
		wr.expectedBlocks = append(wr.expectedBlocks, cidLnk)
	}
	return cidLinks, nil
}

//
// Helpers
//

// Encodes an IPLD node according to the spec given by a CID prefix.
// Returns the CID and encoded data buffer.
func encodeBlock(ctx context.Context, block ipld.Node, encspec cid.Prefix) (cidlink.Link, []byte, error) {
	// It's kinda weird to go through the link builder to encode the data and then create a CID for it,
	// but the multicodec tables are not exported so this is the only way that avoids duplicating them.
	// It's much more complex than what we're really trying to do.
	encoder := cidlink.LinkBuilder{
		Prefix: encspec,
	}

	// Serialize into a buffer first because we need to calculate the length before writing
	// the block data to stream. This buffer is extra unfortunate because there's also an unwanted
	// buffer in LinkBuilder, and we could at least have hoped to use the same buffer for calculating
	// both the hash and the length :'-(
	var dataBuf bytes.Buffer
	storer := func(lnkCtx ipld.LinkContext) (io.Writer, ipld.StoreCommitter, error) {
		return &dataBuf, nilCommitter, nil
	}

	lnk, err := encoder.Build(ctx, ipld.LinkContext{}, block, storer)
	if err != nil {
		return cidlink.Link{}, nil, err
	}
	return lnk.(cidlink.Link), dataBuf.Bytes(), nil
}

// Decodes an IPLD code according to the spec give by a CID link.
// Returns the inflated node.
func decodeBlock(ctx context.Context, lnk cidlink.Link, data []byte) (ipld.Node, error) {
	// Using the link loader for this is weird and inefficient, but the codec tables are not exposed directly.
	// See analogous comment about encoding.
	loader := func(loadLnk ipld.Link, lnkCtx ipld.LinkContext) (io.Reader, error) {
		if loadLnk != lnk {
			return nil, xerrors.Errorf("can't load %v", loadLnk)
		}
		return bytes.NewBuffer(data), nil
	}
	// Inflating the entire structure just to be able to select the links out of it is a big waste of allocations,
	// but the boilerplate to do so with a custom NodeAssembler is too much to stomach.
	builder := basicnode.Prototype.Any.NewBuilder()
	// Loading the link validates that the content matches the CID.
	if err := lnk.Load(ctx, ipld.LinkContext{}, builder, loader); err != nil {
		return nil, err
	}
	return builder.Build(), nil
}

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

var nilCommitter ipld.StoreCommitter = func(link ipld.Link) error { return nil }
