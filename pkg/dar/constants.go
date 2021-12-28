package dar

import (
	"github.com/multiformats/go-multihash"
	"golang.org/x/crypto/sha3"
)

// Magic tokens introducing a DAR file, for content sniffing.
// The fourth byte is intended as a version number.
const magic = "dar\x01"

// Ensure the function and code match.
// The multihash library doesn't provide pre-registered table for streaming hash functions.
const digestMultiHashType = multihash.SHA2_512
var digestFactory = sha3.New512

var cidv0Prefix = []byte{0x12, 0x20}
