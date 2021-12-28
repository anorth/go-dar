package dar

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

// TODO
// - Add a stream-only reader function that does not require Seeker, traverses the archive just once
// - Consider a breadth-first traversal option, that could be better for tipset-blockchains like Filecoin

// Reader reads IPLD blocks from an archive file.
//
// This reader requires a seekable stream. It reads the header and trailer up front and is then
// capable of seeking to read a DAG from a root, or any indexed block (if an index is present).
type Reader struct {
	reader byteReadSeeker
	closer io.Closer

	blocksOffset  int64
	indexOffset   int64
	trailerOffset int64
	digest        []byte

	roots []cid.Cid
}

// Opens an archive file at path and reads the header and trailer.
// The file will be closed when archive is closed.
func NewFileReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	reader, err := NewReader(f)
	if err != nil {
		return nil, err
	}
	reader.closer = f
	return reader, nil
}

// Initializes an archive reader with a stream, reading the header and trailer.
func NewReader(r io.ReadSeeker) (*Reader, error) {
	rd := &Reader{
		reader: byteReadSeeker{r},
	}

	// Read header
	buf := make([]byte, len(magic))
	_, err := io.ReadFull(rd.reader, buf)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte(magic), buf) {
		return nil, xerrors.Errorf("invalid magic bytes (not a DAR file?)")
	}
	if err := rd.readHeader(); err != nil {
		return nil, err
	}
	// Record the offset of the blocks section, which starts here.
	if rd.blocksOffset, err = rd.reader.Seek(0, io.SeekCurrent); err != nil {
		return nil, err
	}

	// Read trailer and index offsets from end of file.
	if err := rd.readTrailer(); err != nil {
		return nil, err
	}

	return rd, nil
}

func (rd *Reader) HasIndex() bool {
	return rd.indexOffset > 0
}

// Iterates the roots of this archive in the order they were written, passing each in turn to a callback.
// If the callback returns an error, iteration halts and that error is propagated from this method.
func (rd *Reader) IterRoots(cb func(c cidlink.Link) error) error {
	// Seek to start of the trailer, where the roots are.
	if _, err := rd.reader.Seek(rd.trailerOffset, io.SeekStart); err != nil {
		return err
	}

	nRoots, err := rd.readVarint()
	if err != nil {
		return err
	}

	for i := int64(0); i < nRoots; i++ {
		c, err := rd.readCID()
		if err != nil {
			return err
		}
		offset, err := rd.readVarint()
		if err != nil {
			return err
		}

		// TODO: return a block iterator
		_ = offset

		if err := cb(cidlink.Link{Cid: c}); err != nil {
			return err
		}
	}
	return nil
}

func (rd *Reader) Roots() ([]cidlink.Link, error) {
	var roots []cidlink.Link
	if err := rd.IterRoots(func(c cidlink.Link) error {
		roots = append(roots, c)
		return nil
	}); err != nil {
		return nil, err
	}
	return roots, nil
}

func (rd *Reader) Close() error {
	if rd.closer != nil {
		return rd.closer.Close()
	}
	return nil
}

func (rd *Reader) readHeader() error {
	var meta archiveMeta
	if err := meta.Deserialize(rd.reader); err != nil {
		return err
	}
	return nil
}

func (rd *Reader) readTrailer() error {
	digestLen := digestFactory().Size()
	offset := int64(8 + 8 + digestLen)
	_, err := rd.reader.Seek(-offset, io.SeekEnd)
	if err != nil {
		return err
	}

	// Read digest.
	rd.digest = make([]byte, digestLen)
	if _, err := io.ReadFull(rd.reader, rd.digest); err != nil {
		return err
	}

	// Read index section offset
	if rd.indexOffset, err = rd.readInt64(); err != nil {
		return err
	}

	// Read trailer section offset
	if rd.trailerOffset, err = rd.readInt64(); err != nil {
		return err
	}

	// The roots are not loaded, in case that's a long list and the caller doesn't need them
	// (because they're using an index).
	return nil
}

// Reads a varint from the stream.
func (rd *Reader) readVarint() (int64, error) {
	return binary.ReadVarint(rd.reader)
}

// Reads an int64 from the stream (always 8 bytes).
func (rd *Reader) readInt64() (int64, error) {
	var v int64
	err := binary.Read(rd.reader, binary.BigEndian, &v)
	return v, err
}

func (rd *Reader) readCID() (cid.Cid, error) {
	// Yuck: adapted from go-car. This belongs in the go-cid package.

	// Peek first two bytes to distinguish CIDv0
	var peekBuf [34]byte
	if _, err := io.ReadFull(rd.reader, peekBuf[:2]); err != nil {
		return cid.Undef, err
	} else if bytes.Equal(peekBuf[:2], cidv0Prefix) {
		if _, err := io.ReadFull(rd.reader, peekBuf[2:]); err != nil {
			return cid.Undef, err
		}
		return cid.Cast(peekBuf[:])
	} else {
		// Rewind to try again as v1
		if _, err := rd.reader.Seek(-2, io.SeekCurrent); err != nil {
			return cid.Undef, err
		}
	}

	// assume cidv1
	vers, err := binary.ReadUvarint(rd.reader)
	if err != nil {
		return cid.Undef, err
	}

	// TODO: the go-cid package allows version 0 here as well
	if vers != 1 {
		return cid.Undef, fmt.Errorf("invalid cid version number")
	}
	codec, err := binary.ReadUvarint(rd.reader)
	if err != nil {
		return cid.Undef, err
	}

	mhr := multihash.NewReader(rd.reader)
	h, err := mhr.ReadMultihash()
	if err != nil {
		return cid.Cid{}, err
	}

	return cid.NewCidV1(codec, h), nil
}

// Extends a ReadSeeker to ByteReader.
// Note that these reads are all unbuffered. It may be advantageous to add buffering here,
// like bufio.NewReader, but also handling seeks.
type byteReadSeeker struct {
	io.ReadSeeker
}

func (r byteReadSeeker) ReadByte() (byte, error) {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}
