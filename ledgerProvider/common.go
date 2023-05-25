package ledgerprovider

var (
	// ledgerKeyPrefix is the prefix for each ledger key in idStore db
	ledgerKeyPrefix = []byte{'l'}
	// ledgerKeyStop is the end key when querying idStore db by ledger key
	//ledgerKeyStop = []byte{'l' + 1}
	// metadataKeyPrefix is the prefix for each metadata key in idStore db
	metadataKeyPrefix = []byte{'s'}
	// metadataKeyStop is the end key when querying idStore db by metadata key
	//metadataKeyStop = []byte{'s' + 1}
)

func EncodeLedgerKey(ledgerID string, prefix []byte) []byte {
	return append(prefix, []byte(ledgerID)...)
}
