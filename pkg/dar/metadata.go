package dar

import (
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

// DAR metadata object.
// This is a placeholder for future information that might be placed in the archive/stream header,
// such as the selector(s) which generated the DAG(s).
// Serialized as a DAG-CBOR list ("representation tuple").
type archiveMeta struct {
}

func (m *archiveMeta) Serialize(wr io.Writer) error {
	if node, err := m.ToNode(); err != nil {
		return err
	} else if err := dagcbor.Encoder(node, wr); err != nil {
		return err
	}
	return nil
}

func (m *archiveMeta) Deserialize(rd io.Reader) error {
	bld := basicnode.Prototype.List.NewBuilder()
	if err := dagcbor.Decoder(bld, rd); err != nil {
		return err
	}
	if err := m.FromNode(bld.Build()); err != nil {
		return err
	}
	return nil
}

func (m *archiveMeta) ToNode() (ipld.Node, error) {
	bld := basicnode.Prototype.List.NewBuilder()
	listAssembler, err := bld.BeginList(0)
	if err != nil {
		return nil, err
	}
	err = listAssembler.Finish()
	if err != nil {
		return nil, err
	}
	return bld.Build(), nil
}

func (m *archiveMeta) FromNode(_ ipld.Node) error {
	return nil
}
