package dar

import (
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

// DAR metadata object.
type archiveMeta struct {
	hasIndex bool
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
	listAssembler, err := bld.BeginList(1)
	if err != nil {
		return nil, err
	}
	err = listAssembler.AssembleValue().AssignBool(m.hasIndex)
	if err != nil {
		return nil, err
	}
	err = listAssembler.Finish()
	if err != nil {
		return nil, err
	}
	return bld.Build(), nil
}

func (m *archiveMeta) FromNode(node ipld.Node) error {
	hasIndexNode, err := node.LookupByIndex(0)
	if err != nil {
		return err
	}
	m.hasIndex, err = hasIndexNode.AsBool()
	if err != nil {
		return err
	}
	return nil
}
