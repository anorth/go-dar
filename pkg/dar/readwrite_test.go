package dar_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anorth/go-dar/pkg/dar"
)

// TODO:
// - test raw nodes
// - test DAG de-duping
// - test iterating blocks
// - test invalid writes: out-of-order, duplicate root

var encSpec = cid.Prefix{
	Version:  1,
	Codec:    cid.DagCBOR,
	MhType:   multihash.SHA2_256,
	MhLength: -1,
}

var encoder = cidlink.LinkBuilder{
	Prefix: encSpec,
}

func TestNodes(t *testing.T) {
	ctx := context.Background()
	aScalar := basicnode.NewString("scalar")
	aNode, err := qp.BuildMap(basicnode.Prototype.Any, 4, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "string", qp.String("a string"))
		qp.MapEntry(ma, "number", qp.Int(3))
		qp.MapEntry(ma, "list", qp.List(3, func(la ipld.ListAssembler) {
			qp.ListEntry(la, qp.Bytes([]byte{1, 2, 3, 4}))
			qp.ListEntry(la, qp.Float(3.14))
			qp.ListEntry(la, qp.Null())
		}))
	})
	require.NoError(t, err)

	t.Run("empty archive", func(t *testing.T) {
		buf, wr := bufWriter(t)

		require.NoError(t, wr.Finish())

		rd := bufReader(t, buf)
		assert.False(t, rd.HasIndex())
		roots, err := rd.Roots()
		require.NoError(t, err)
		assert.Empty(t, roots)
	})

	t.Run("singleton scalar", func(t *testing.T) {
		buf, wr := bufWriter(t)
		root, links, err := wr.BeginDAG(ctx, aScalar, encSpec)
		require.NoError(t, err)
		assert.True(t, root.Cid.Defined())
		assert.Equal(t, uint64(1), root.Cid.Version())
		assert.Equal(t, uint64(cid.DagCBOR), root.Cid.Type())
		assert.Empty(t, links)
		require.NoError(t, wr.Finish())

		rd := bufReader(t, buf)
		assert.False(t, rd.HasIndex())

		roots, err := rd.Roots()
		require.NoError(t, err)
		assert.Len(t, roots, 1)
		assert.Equal(t, root, roots[0])

	})

	t.Run("singleton node", func(t *testing.T) {
		buf, wr := bufWriter(t)
		root, links, err := wr.BeginDAG(ctx, aNode, encSpec)
		require.NoError(t, err)
		assert.True(t, root.Cid.Defined())
		assert.Equal(t, uint64(1), root.Cid.Version())
		assert.Equal(t, uint64(cid.DagCBOR), root.Cid.Type())
		assert.Empty(t, links)
		require.NoError(t, wr.Finish())

		rd := bufReader(t, buf)
		assert.False(t, rd.HasIndex())

		roots, err := rd.Roots()
		require.NoError(t, err)
		assert.Len(t, roots, 1)
		assert.Equal(t, root, roots[0])
	})
}

func TestDags(t *testing.T) {
	ctx := context.Background()

	leftLeaf := basicnode.NewString("scalar")
	rightLeaf, err := qp.BuildMap(basicnode.Prototype.Any, 4, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "string", qp.String("a string"))
		qp.MapEntry(ma, "list", qp.List(3, func(la ipld.ListAssembler) {
			qp.ListEntry(la, qp.Bytes([]byte{1, 2, 3, 4}))
		}))
	})
	require.NoError(t, err)

	leftLink, _ := encode(ctx, t, leftLeaf)
	rightLink, _ := encode(ctx, t, rightLeaf)
	top, err := qp.BuildMap(basicnode.Prototype.Any, 4, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "left", qp.Link(leftLink))
		qp.MapEntry(ma, "right", qp.Link(rightLink))
	})
	require.NoError(t, err)

	t.Run("dag root only", func(t *testing.T) {
		buf, wr := bufWriter(t)
		root, links, err := wr.BeginDAG(ctx, top, encSpec)
		require.NoError(t, err)
		assert.Equal(t, []cidlink.Link{leftLink, rightLink}, links)
		require.NoError(t, wr.Finish())

		rd := bufReader(t, buf)

		roots, err := rd.Roots()
		require.NoError(t, err)
		assert.Len(t, roots, 1)
		assert.Equal(t, root, roots[0])
	})

	t.Run("simple dag", func(t *testing.T) {
		buf, wr := bufWriter(t)

		root, links, err := wr.BeginDAG(ctx, top, encSpec)
		require.NoError(t, err)
		assert.Equal(t, []cidlink.Link{leftLink, rightLink}, links)

		{
			lnk, links, err := wr.AppendBlock(ctx, leftLeaf, encSpec)
			require.NoError(t, err)
			assert.Equal(t, leftLink, lnk)
			assert.Empty(t, links)
		}
		{
			lnk, links, err := wr.AppendBlock(ctx, rightLeaf, encSpec)
			require.NoError(t, err)
			assert.Equal(t, rightLink, lnk)
			assert.Empty(t, links)
		}
		require.NoError(t, wr.Finish())

		rd := bufReader(t, buf)

		roots, err := rd.Roots()
		require.NoError(t, err)
		assert.Len(t, roots, 1)
		assert.Equal(t, root, roots[0])
	})
}

func encode(ctx context.Context, t *testing.T, leftLeaf ipld.Node) (cidlink.Link, []byte) {
	buf, storer := bufStorer()
	link, err := encoder.Build(ctx, ipld.LinkContext{}, leftLeaf, storer)
	require.NoError(t, err)
	return link.(cidlink.Link), buf.Bytes()
}

func bufWriter(t *testing.T) (*bytes.Buffer, *dar.Writer) {
	var buf bytes.Buffer
	wr, err := dar.NewWriter(&buf, false)
	require.NoError(t, err)
	return &buf, wr
}

func bufReader(t *testing.T, buf *bytes.Buffer) *dar.Reader {
	rd, err := dar.NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	return rd
}

func bufStorer() (bytes.Buffer, ipld.Storer) {
	var buf bytes.Buffer
	storer := func(lnkCtx ipld.LinkContext) (io.Writer, ipld.StoreCommitter, error) {
		return &buf, nilCommitter, nil
	}
	return buf, storer
}

var nilCommitter ipld.StoreCommitter = func(link ipld.Link) error { return nil }
