package peer

import (
	"encoding/json"
	"fmt"

	configHistory "testLevelDB/configHistory"
	"testLevelDB/index"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	cb "github.com/hyperledger/fabric-protos-go/common"
	mb "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type MSPPrincipal_Classification int32

const (
	MSPPrincipal_ROLE MSPPrincipal_Classification = 0
	// one of a member of MSP network, and the one of an
	// administrator of an MSP network
	MSPPrincipal_ORGANIZATION_UNIT MSPPrincipal_Classification = 1
	// groupping of entities, per MSP affiliation
	// E.g., this can well be represented by an MSP's
	// Organization unit
	MSPPrincipal_IDENTITY MSPPrincipal_Classification = 2
	// identity
	MSPPrincipal_ANONYMITY MSPPrincipal_Classification = 3
	// an identity to be anonymous or nominal.
	MSPPrincipal_COMBINED MSPPrincipal_Classification = 4
)

var MSPPrincipal_Classification_name = map[int32]string{
	0: "ROLE",
	1: "ORGANIZATION_UNIT",
	2: "IDENTITY",
	3: "ANONYMITY",
	4: "COMBINED",
}

var MSPPrincipal_Classification_value = map[string]int32{
	"ROLE":              0,
	"ORGANIZATION_UNIT": 1,
	"IDENTITY":          2,
	"ANONYMITY":         3,
	"COMBINED":          4,
}

type CollectionConfigPackage struct {
	Config []*CollectionConfig `protobuf:"bytes,1,rep,name=config,proto3" json:"config,omitempty"`
}

func (m *CollectionConfigPackage) Reset()         { *m = CollectionConfigPackage{} }
func (m *CollectionConfigPackage) String() string { return proto.CompactTextString(m) }
func (*CollectionConfigPackage) ProtoMessage()    {}

type CollectionConfig struct {
	Payload *CollectionConfigPayload `protobuf_oneof:"payload"`
}

type CollectionConfigPayload struct {
	StaticCollectionConfig *StaticCollectionConfig `protobuf:"bytes,1,opt,name=static_collection_config,json=staticCollectionConfig,proto3,oneof"`
}

type StaticCollectionConfig struct {
	Name              string                  `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	MemberOrgsPolicy  *CollectionPolicyConfig `protobuf:"bytes,2,opt,name=member_orgs_policy,json=memberOrgsPolicy,proto3" json:"member_orgs_policy,omitempty"`
	RequiredPeerCount int32                   `protobuf:"varint,3,opt,name=required_peer_count,json=requiredPeerCount,proto3" json:"required_peer_count,omitempty"`
	MaximumPeerCount  int32                   `protobuf:"varint,4,opt,name=maximum_peer_count,json=maximumPeerCount,proto3" json:"maximum_peer_count,omitempty"`
	BlockToLive       uint64                  `protobuf:"varint,5,opt,name=block_to_live,json=blockToLive,proto3" json:"block_to_live,omitempty"`
	MemberOnlyRead    bool                    `protobuf:"varint,6,opt,name=member_only_read,json=memberOnlyRead,proto3" json:"member_only_read,omitempty"`
	MemberOnlyWrite   bool                    `protobuf:"varint,7,opt,name=member_only_write,json=memberOnlyWrite,proto3" json:"member_only_write,omitempty"`
	EndorsementPolicy *ApplicationPolicy      `protobuf:"bytes,8,opt,name=endorsement_policy,json=endorsementPolicy,proto3" json:"endorsement_policy,omitempty"`
}

type CollectionPolicyConfig struct {
	Payload *SignaturePayload `protobuf_oneof:"payload"`
}

type SignaturePayload struct {
	SignaturePolicy *SignaturePolicy `protobuf:"bytes,1,opt,name=signature_policy,json=signaturePolicy,proto3,oneof"`
}

type SignaturePolicy struct {
	Version    int32                 `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	Rule       *SignaturePolicy_type `protobuf:"bytes,2,opt,name=rule,proto3" json:"rule,omitempty"`
	Identities []*MSPPrincipal       `protobuf:"bytes,3,rep,name=identities,proto3" json:"identities,omitempty"`
}

type SignaturePolicy_type struct {
	// Types that are valid to be assigned to Type:
	//	*SignaturePolicy_SignedBy
	//	*SignaturePolicy_NOutOf_
	Type *SignaturePolicy_NType `protobuf_oneof:"Type"`
}

type SignaturePolicy_NType struct {
	NOutOf *SignaturePolicy_NOutOf `protobuf:"bytes,2,opt,name=n_out_of,json=nOutOf,proto3,oneof"`
}

type SignaturePolicy_NOutOf struct {
	N     int32                    `protobuf:"varint,1,opt,name=n,proto3" json:"n,omitempty"`
	Rules []*SignatureNOutOfPolicy `protobuf:"bytes,2,rep,name=rules,proto3" json:"rules,omitempty"`
}

type SignatureNOutOfPolicy struct {
	Type *SignedBy `protobuf_oneof:"Type"`
}

type SignedBy struct {
	SignedBy int32 `protobuf:"varint,1,opt,name=signed_by,json=signedBy,proto3,oneof"`
}

type MSPPrincipal struct {
	PrincipalClassification MSPPrincipal_Classification `protobuf:"varint,1,opt,name=principal_classification,json=principalClassification,proto3,enum=common.MSPPrincipal_Classification" json:"principal_classification,omitempty"`
	Principal               []byte                      `protobuf:"bytes,2,opt,name=principal,proto3" json:"principal,omitempty"`
}

type ApplicationPolicy struct {
}

type Type struct {
}

func ParseConfigHistoryKV(db *leveldb.DB, channel, chaincode string) {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		keys := iter.Key()
		if len(channel) == 0 && len(chaincode) == 0 {
			fmt.Println("parse configHistory key error")
			return
		}
		prefixKeyLen := len(channel) + 1
		compositeKey := keys[prefixKeyLen:]
		p := configHistory.DecodeCompositeKey(compositeKey)
		k, err := json.Marshal(p)
		if err != nil {
			fmt.Println("json marshal err", err)
		}
		fmt.Println("compositeKey struct string=", string(k))
		key := fmt.Sprintf("%s %s%d", p.NS, p.Key, p.BlockNum)
		// conf := &peer.CollectionConfigPackage{}
		// if err := proto.Unmarshal(iter.Value(), conf); err != nil {
		// 	fmt.Printf("%s error unmarshalling compositeKV to collection config", err)
		// 	return
		// }
		// for _, collConfig := range conf.Config {
		// 	staticCollConfig := collConfig.GetStaticCollectionConfig()
		// 	fmt.Printf("staticCollConfig.RequiredPeerCount=: %d\n", staticCollConfig.RequiredPeerCount)
		// 	fmt.Printf("staticCollConfig.MaximumPeerCount=: %d\n", staticCollConfig.MaximumPeerCount)
		// 	// EndorsementPolicy
		// 	//peer_Applicat_Policy := staticCollConfig.EndorsementPolicy
		// 	//peer_Applicat_Policy.
		// 	orgsPolicy := staticCollConfig.MemberOrgsPolicy
		// 	signaturePolicy := orgsPolicy.GetSignaturePolicy()
		// 	fmt.Printf("staticCollConfig.signaturePolicy.Version=: %d\n", signaturePolicy.Version)

		// 	signRule := signaturePolicy.GetRule()
		// 	signRuleNOutOf := signRule.GetNOutOf()
		// 	signRuleNOutOf_N := signRuleNOutOf.N
		// 	fmt.Printf("staticCollConfig.signaturePolicy.rule.NOutOf.N=: %d\n", signRuleNOutOf_N)
		// 	signRuleNOutOf_Rules := signRuleNOutOf.Rules
		// 	for r := 0; r < len(signRuleNOutOf_Rules); r++ {
		// 		snr := signRuleNOutOf_Rules[r]
		// 		signRuleNOutOf_Rules_Type_signedBy := snr.GetSignedBy()
		// 		fmt.Printf("staticCollConfig.signaturePolicy.rule.NOutOf.rules[%d].type.SignedBy=: %d\n", r, signRuleNOutOf_Rules_Type_signedBy)
		// 	}

		// 	signIdentities := signaturePolicy.GetIdentities()
		// 	for s := 0; s < len(signIdentities); s++ {
		// 		identities := signIdentities[s]
		// 		PrincipalClassification := identities.PrincipalClassification
		// 		fmt.Printf("staticCollConfig.signaturePolicy.identities[%d].PrincipalClassification: %d\n", s, PrincipalClassification)

		// 		mspRole := &mb.MSPRole{}
		// 		proto.Unmarshal(identities.Principal, mspRole)
		// 		Principal, _ := json.Marshal(mspRole)
		// 		fmt.Printf("staticCollConfig.signaturePolicy.identities[%d].Principal: %s\n", s, string(Principal))
		// 	}
		// 	v, err := json.Marshal(collConfig)
		// 	if err != nil {
		// 		fmt.Println("json marshal err", err)
		// 	}

		// 	fmt.Printf("key[hxyz s%s]==value[%s]", string(key), string(v))

		// }
		v, err := ParseConfigValue(iter.Value())
		if err != nil {
			//return "",fmt.Errorf("")
		}
		fmt.Printf("key[hxyz s%s]==value[%s]\n", string(key), v)
	}
}

func GetConfigHistoryKV(db *leveldb.DB, channel, chaincode string) {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		keys := iter.Key()
		if len(channel) == 0 && len(chaincode) == 0 {
			fmt.Println("parse configHistory key error")
			return
		}
		prefixKeyLen := len(channel) + 1
		compositeKey := keys[prefixKeyLen:]
		p := configHistory.DecodeCompositeKey(compositeKey)
		k, err := json.Marshal(p)
		if err != nil {
			fmt.Println("json marshal err", err)
		}
		fmt.Println("compositeKey struct string=", string(k))
		key := fmt.Sprintf("%s %s%d", p.NS, p.Key, p.BlockNum)
		conf := &peer.CollectionConfigPackage{}
		if err := proto.Unmarshal(iter.Value(), conf); err != nil {
			fmt.Printf("%s error unmarshalling compositeKV to collection config", err)
			return
		}
		for _, collConfig := range conf.Config {
			// staticCollConfig := collConfig.GetStaticCollectionConfig()
			// fmt.Printf("staticCollConfig.RequiredPeerCount=: %d\n", staticCollConfig.RequiredPeerCount)
			// fmt.Printf("staticCollConfig.MaximumPeerCount=: %d\n", staticCollConfig.MaximumPeerCount)
			// // EndorsementPolicy
			// //peer_Applicat_Policy := staticCollConfig.EndorsementPolicy
			// //peer_Applicat_Policy.
			// orgsPolicy := staticCollConfig.MemberOrgsPolicy
			// signaturePolicy := orgsPolicy.GetSignaturePolicy()
			// fmt.Printf("staticCollConfig.signaturePolicy.Version=: %d\n", signaturePolicy.Version)

			// signRule := signaturePolicy.GetRule()
			// signRuleNOutOf := signRule.GetNOutOf()
			// signRuleNOutOf_N := signRuleNOutOf.N
			// fmt.Printf("staticCollConfig.signaturePolicy.rule.NOutOf.N=: %d\n", signRuleNOutOf_N)
			// signRuleNOutOf_Rules := signRuleNOutOf.Rules
			// for r := 0; r < len(signRuleNOutOf_Rules); r++ {
			// 	snr := signRuleNOutOf_Rules[r]
			// 	signRuleNOutOf_Rules_Type_signedBy := snr.GetSignedBy()
			// 	fmt.Printf("staticCollConfig.signaturePolicy.rule.NOutOf.rules[%d].type.SignedBy=: %d\n", r, signRuleNOutOf_Rules_Type_signedBy)
			// }

			// signIdentities := signaturePolicy.GetIdentities()
			// for s := 0; s < len(signIdentities); s++ {
			// 	identities := signIdentities[s]
			// 	PrincipalClassification := identities.PrincipalClassification
			// 	fmt.Printf("staticCollConfig.signaturePolicy.identities[%d].PrincipalClassification: %d\n", s, PrincipalClassification)

			// 	mspRole := &mb.MSPRole{}
			// 	proto.Unmarshal(identities.Principal, mspRole)
			// 	Principal, _ := json.Marshal(mspRole)
			// 	fmt.Printf("staticCollConfig.signaturePolicy.identities[%d].Principal: %s\n", s, string(Principal))
			// }
			v, err := json.Marshal(collConfig)
			if err != nil {
				fmt.Println("json marshal err", err)
			}

			fmt.Printf("key[hxyz s%s]==value[%s]", string(key), string(v))

		}
		// v, err := ParseConfigValue(iter.Value())
		// if err != nil {
		// 	//return "",fmt.Errorf("")
		// }
		//fmt.Printf("key[hxyz s%s]==value[%s]\n", string(key), v)
	}
}

func GetPeerConfigHistory(db *leveldb.DB, channel, chaincode string, blockNum uint64) error {

	configKey := configHistory.ConstructCollectionConfigKey(chaincode)

	compositeKey := configHistory.EncodeCompositeKey(configHistory.CollectionConfigNamespace, configKey, blockNum)
	key := index.ConstructLevelKey(channel, compositeKey)
	value, err := db.Get(key, &opt.ReadOptions{})
	if err != nil {
		fmt.Println("err", err)
	}
	v, err := ParseConfigValue(value)
	if err != nil {
		return fmt.Errorf("%s", err)
	}
	fmt.Printf("key[%s]==value[%s]", string(key), v)
	return nil
}

func ParseConfigValue(vaule []byte) ([]string, error) {
	var sArrary []string
	conf := &peer.CollectionConfigPackage{}
	if err := proto.Unmarshal(vaule, conf); err != nil {
		fmt.Printf("%s error unmarshalling compositeKV to collection config", err)
		return nil, err
	}

	for _, collConfig := range conf.Config {
		staticCollConfig := collConfig.GetStaticCollectionConfig()
		fmt.Printf("staticCollConfig.RequiredPeerCount=: %d\n", staticCollConfig.RequiredPeerCount)
		fmt.Printf("staticCollConfig.MaximumPeerCount=: %d\n", staticCollConfig.MaximumPeerCount)
		// EndorsementPolicy
		//peer_Applicat_Policy := staticCollConfig.EndorsementPolicy
		//peer_Applicat_Policy.
		orgsPolicy := staticCollConfig.MemberOrgsPolicy
		signaturePolicy := orgsPolicy.GetSignaturePolicy()
		fmt.Printf("staticCollConfig.signaturePolicy.Version=: %d\n", signaturePolicy.Version)

		signRule := signaturePolicy.GetRule()
		signRuleNOutOf := signRule.GetNOutOf()
		signRuleNOutOf_N := signRuleNOutOf.N
		fmt.Printf("staticCollConfig.signaturePolicy.rule.NOutOf.N=: %d\n", signRuleNOutOf_N)
		signRuleNOutOf_Rules := signRuleNOutOf.Rules
		for r := 0; r < len(signRuleNOutOf_Rules); r++ {
			snr := signRuleNOutOf_Rules[r]
			signRuleNOutOf_Rules_Type_signedBy := snr.GetSignedBy()
			fmt.Printf("staticCollConfig.signaturePolicy.rule.NOutOf.rules[%d].type.SignedBy=: %d\n", r, signRuleNOutOf_Rules_Type_signedBy)
		}

		signIdentities := signaturePolicy.GetIdentities()
		for s := 0; s < len(signIdentities); s++ {
			identities := signIdentities[s]
			PrincipalClassification := identities.PrincipalClassification
			fmt.Printf("staticCollConfig.signaturePolicy.identities[%d].PrincipalClassification: %d\n", s, PrincipalClassification)

			mspRole := &mb.MSPRole{}
			proto.Unmarshal(identities.Principal, mspRole)
			Principal, _ := json.Marshal(mspRole)
			fmt.Printf("staticCollConfig.signaturePolicy.identities[%d].Principal: %s\n", s, string(Principal))
		}
		v, err := json.Marshal(collConfig)
		if err != nil {
			fmt.Println("json marshal err", err)
		}
		sArrary = append(sArrary, string(v))
		fmt.Printf("value[%s]\n", string(v))
	}
	return sArrary, nil
}

// func UpdateConfigHistory(db *leveldb.DB, channel, chaincode string, blockNum uint64) error {
// 	configKey := configHistory.ConstructCollectionConfigKey(chaincode)

// 	compositeKey := configHistory.EncodeCompositeKey(configHistory.CollectionConfigNamespace, configKey, blockNum)
// 	key := index.ConstructLevelKey(channel, compositeKey)
// 	mspRole := &mb.MSPRole{}
// 	mspRole.Role = mb.MSPRole_MEMBER
// 	mspRole.MspIdentifier = fmt.Sprintf("Org%dMSP", 1)
// 	Principal, _ := proto.Marshal(mspRole)
// 	fmt.Println("key value", string(key), string(Principal))
// 	// collConfigPackage := &CollectionConfigPackage{}
// 	// collConfigArrary := []*CollectionConfig{}
// 	// for c := 0; c < 1; c++ {
// 	// 	collConfig := &CollectionConfig{}
// 	// 	collConfigPayload := &CollectionConfigPayload{}
// 	// 	StaticCollConfig := &StaticCollectionConfig{}
// 	// 	StaticCollConfig.Name = "archiveCollection"
// 	// 	StaticCollConfig.RequiredPeerCount = 0
// 	// 	StaticCollConfig.MaximumPeerCount = 3
// 	// 	StaticCollConfig.MemberOnlyRead = true
// 	// 	StaticCollConfig.MemberOnlyWrite = true
// 	// 	StaticCollConfig.BlockToLive = 1000000
// 	// 	//endorsementPolicy := &ApplicationPolicy{}
// 	// 	StaticCollConfig.EndorsementPolicy = nil
// 	// 	orgsPolicy := &CollectionPolicyConfig{}
// 	// 	orgsPolicyPayload := &SignaturePayload{}
// 	// 	signaturePolicy := &SignaturePolicy{}
// 	// 	signaturePolicy.Version = 0
// 	// 	signRule := &SignaturePolicy_type{}
// 	// 	signRuleNY := &SignaturePolicy_NType{}
// 	// 	signRuleNY_NOutOf := &SignaturePolicy_NOutOf{}
// 	// 	signRuleNY_NOutOf.N = 1
// 	// 	signRuleNY_NOutOf_Rules := []*SignatureNOutOfPolicy{}
// 	// 	for i := 0; i < 2; i++ {
// 	// 		signRuleNY_NOutOf_Rules_policy := &SignatureNOutOfPolicy{}
// 	// 		signRuleNY_NOutOf_Rules_policy_Type := &SignedBy{}
// 	// 		signRuleNY_NOutOf_Rules_policy_Type.SignedBy = int32(i)
// 	// 		signRuleNY_NOutOf_Rules_policy.Type = signRuleNY_NOutOf_Rules_policy_Type
// 	// 		signRuleNY_NOutOf_Rules = append(signRuleNY_NOutOf_Rules, signRuleNY_NOutOf_Rules_policy)
// 	// 	}
// 	// 	signRuleNY_NOutOf.Rules = signRuleNY_NOutOf_Rules
// 	// 	signRuleNY.NOutOf = signRuleNY_NOutOf
// 	// 	signRule.Type = signRuleNY
// 	// 	signaturePolicy.Rule = signRule
// 	// 	signaturePolicy_Identities := []*MSPPrincipal{}
// 	// 	for i := 1; i < 3; i++ {
// 	// 		signaturePolicy_Identities_Pricipal := &MSPPrincipal{}
// 	// 		signaturePolicy_Identities_Pricipal.PrincipalClassification = 0
// 	// 		mspRole := &mb.MSPRole{}
// 	// 		mspRole.Role = mb.MSPRole_MEMBER
// 	// 		mspRole.MspIdentifier = fmt.Sprintf("Org%dMSP", i)
// 	// 		Principal, _ := proto.Marshal(mspRole)
// 	// 		signaturePolicy_Identities_Pricipal.Principal = Principal
// 	// 		signaturePolicy_Identities = append(signaturePolicy_Identities, signaturePolicy_Identities_Pricipal)
// 	// 	}
// 	// 	signaturePolicy.Identities = signaturePolicy_Identities
// 	// 	orgsPolicyPayload.SignaturePolicy = signaturePolicy
// 	// 	orgsPolicy.Payload = orgsPolicyPayload
// 	// 	StaticCollConfig.MemberOrgsPolicy = orgsPolicy
// 	// 	collConfigPayload.StaticCollectionConfig = StaticCollConfig
// 	// 	collConfig.Payload = collConfigPayload

// 	// 	collConfigArrary = append(collConfigArrary, collConfig)
// 	// }
// 	// collConfigPackage.Config = collConfigArrary
// 	// var configBytes []byte
// 	// var err error
// 	// if configBytes, err = proto.Marshal(collConfigPackage); err != nil {
// 	// 	return errors.WithStack(err)
// 	// }
// 	// fmt.Println("key == end ==", string(key), string(configBytes))
// 	//value := configBytes
// 	// err = db.Put(key, value, &opt.WriteOptions{})
// 	// if err != nil {
// 	// 	return fmt.Errorf("", err)

// 	// }
// 	return nil
// }
func UpdateConfigHistory(db *leveldb.DB, channel, chaincode string, blockNum uint64) error {
	var orgMSPID []string = []string{"Org1MSP", "Org2MSP"}
	configKey := configHistory.ConstructCollectionConfigKey(chaincode)
	compositeKey := configHistory.EncodeCompositeKey(configHistory.CollectionConfigNamespace, configKey, blockNum)
	key := index.ConstructLevelKey(channel, compositeKey)
	implicitCollections := make([]*peer.StaticCollectionConfig, 0, len(orgMSPID))
	//for _, org := range orgMSPID {
	implicitCollections = append(implicitCollections, GenerateImplicitCollectionForOrg(orgMSPID))
	//}
	var combinedColls []*peer.CollectionConfig
	for _, implicitColl := range implicitCollections {
		c := &peer.CollectionConfig{}
		c.Payload = &peer.CollectionConfig_StaticCollectionConfig{StaticCollectionConfig: implicitColl}
		combinedColls = append(combinedColls, c)
	}
	collectionConfigPackage := &peer.CollectionConfigPackage{
		Config: combinedColls,
	}
	var configBytes []byte
	var err error
	if configBytes, err = proto.Marshal(collectionConfigPackage); err != nil {
		return errors.WithStack(err)
	}
	err = db.Put(key, configBytes, &opt.WriteOptions{})
	if err != nil {
		return fmt.Errorf("configHistory db update failed:%s", err)
	}
	fmt.Printf("key[%s] == value[%s]", string(key), string(configBytes))
	// conf := &peer.CollectionConfigPackage{}
	// if err := proto.Unmarshal(configBytes, conf); err != nil {
	// 	fmt.Printf("%s error unmarshalling compositeKV to collection config", err)

	// }
	// for _, collConfig := range conf.Config {
	// 	v, err := json.Marshal(collConfig)
	// 	if err != nil {
	// 		fmt.Println("json marshal err", err)
	// 	}

	// 	fmt.Printf("key[hxyz s%s]==value[%s]", string(key), string(v))

	// }
	return nil
}

func GenerateImplicitCollectionForOrg(mspid []string) *peer.StaticCollectionConfig {
	// set Required/MaxPeerCount to 0 if it is other org's implicit collection (mspid does not match peer's local mspid)
	// set Required/MaxPeerCount to the config values if it is the peer org's implicit collection (mspid matches peer's local mspid)
	requiredPeerCount := 0
	maxPeerCount := 3

	return &peer.StaticCollectionConfig{
		Name: "archiveCollection",
		MemberOrgsPolicy: &peer.CollectionPolicyConfig{
			Payload: &peer.CollectionPolicyConfig_SignaturePolicy{
				SignaturePolicy: SignedByMspMember(mspid),
			},
		},
		BlockToLive:       uint64(1000000),
		MemberOnlyRead:    true,
		MemberOnlyWrite:   true,
		RequiredPeerCount: int32(requiredPeerCount),
		MaximumPeerCount:  int32(maxPeerCount),
	}
}

func SignedByMspMember(mspId []string) *cb.SignaturePolicyEnvelope {
	return signedByFabricEntity(mspId, mb.MSPRole_MEMBER)
}
func signedByFabricEntity(mspId []string, role mb.MSPRole_MSPRoleType) *cb.SignaturePolicyEnvelope {
	// specify the principal: it's a member of the msp we just found
	//identities := make([]*mb.MSPPrincipal, len(mspId))
	identities := []*mb.MSPPrincipal{}
	for _, orgMSP := range mspId {
		principal := &mb.MSPPrincipal{
			PrincipalClassification: mb.MSPPrincipal_ROLE,
			Principal:               protoMarshalOrPanic(&mb.MSPRole{Role: role, MspIdentifier: orgMSP})}

		identities = append(identities, principal)
	}

	// create the policy: it requires exactly 1 signature from the first (and only) principal
	p := &cb.SignaturePolicyEnvelope{
		Version:    0,
		Rule:       NOutOf(1, []*cb.SignaturePolicy{NOutOfSignedBy(0), NOutOfSignedBy(1)}),
		Identities: identities,
	}

	return p
}

func protoMarshalOrPanic(pb proto.Message) []byte {
	data, err := proto.Marshal(pb)
	if err != nil {
		panic(err)
	}

	return data
}

func NOutOfSignedBy(index int32) *cb.SignaturePolicy {
	return &cb.SignaturePolicy{
		Type: &cb.SignaturePolicy_SignedBy{
			SignedBy: index,
		},
	}
}

func NOutOf(n int32, policies []*cb.SignaturePolicy) *cb.SignaturePolicy {
	return &cb.SignaturePolicy{
		Type: &cb.SignaturePolicy_NOutOf_{
			NOutOf: &cb.SignaturePolicy_NOutOf{
				N:     n,
				Rules: policies,
			},
		},
	}
}
