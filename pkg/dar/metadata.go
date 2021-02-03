package dar

import (
	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

// DAR metadata object.
// The metadata is an IPLD list, containing:
// - 0: whether there is an index or not (bool)


// Builds an IPLD list containing DAR metadata.
func buildMetadata(hasIndex bool) (ipld.Node, error) {
	bld := basicnode.Prototype.List.NewBuilder()
	listAssembler, err := bld.BeginList(1)
	if err != nil {
		return nil, err
	}
	err = listAssembler.AssembleValue().AssignBool(hasIndex)
	if err != nil {
		return nil, err
	}
	err = listAssembler.Finish()
	if err != nil {
		return nil, err
	}
	metadataNode := bld.Build()
	return metadataNode, nil
}

