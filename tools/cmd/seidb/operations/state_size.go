package operations

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/sc/memiavl"
	"github.com/spf13/cobra"
)

var ProjectContracts = map[string][]string{
	"Anome":                        {"0x009fef3ede4bd31948ca1e6b34d6974d42bd8ea5", "0x0562feec4fc594c9d2d11c6ecf52f118c32bc7f1", "0x08806ebb1fce70bd66c30d7bde5971a116e999c2", "0x08b9b03dd228137f7357b242aa4428c57931e009", "0x0b273497c6a594902146869abf991ef0e6141e01", "0x0bbda0f76e205fc6a160b90a09975fa443b3fe44", "0x0cabb5016a5594b5126da60b74e508e1b6a6db9f", "0x0da873afe60eb4b2837214799ade24f5e32c9528", "0x0f3c2735769823f7161640af9e69afb95f709aee", "0x114230129dabe4ed69c6390eec8682aa2cc4a6a6", "0x12d01392df6d0097e4f7d1bb4fb346d546d71420", "0x1ac7e0ea8d82efd4c9e1f786893f0de54e8ff195", "0x1e387b7809223af6a758023ffd6d4273cf88f957", "0x1f199af73498a57287f754c14a12883c08885e62", "0x21604de00a0300ebcc347a294a7b21a58dd0b9c5", "0x25bb949c057acaed9e2600453b10b887ac69370f", "0x25d3edab9467a68affc696ef8d6dcce3e46fbb25", "0x27897af92bda3258bd4ac33b126926d3a1772b3b", "0x289dc5212db18049c8144c671d1a72ff090d9842", "0x28d3b076b60607b00d0f8cc7bfe7ad40d2a74bce", "0x2954be4579734b4f6c3e0402d66ad65a1ac4b930", "0x299f91a93d9cb90608c49ab9d3b5c90ecde1b589", "0x30132a39c04738b8db956f280f8a4a95a747c0c0", "0x307b36f915e6b5f8a72d4ba95705b657dc80c4bc", "0x314554a6e1d86728911a4c212b85ba7fff8a9aed", "0x327893fb4c75eee97fb0faf74aaaaee79dfe5652", "0x36b7b6ceae6fd45eab46db8896bb806ef65320e9", "0x3fa22ddbb63018d24d9c6151c9394d913bfbabf5", "0x3fc0f1b0171639e746d62b6a1e132d447464ae57", "0x438c25b8a4ddbd9a3601b65671de080a487351cf", "0x44774f159c7a83c08039c963125ad6aaf3cb90ef", "0x46832f6a6a533813aea33f323cc99d1968499420", "0x47e0d62407b68c122dcd432acc4be8fe421cfe3d", "0x4b2caf0c130ebed6a6511e945b092f4f24eed3a7", "0x4c34348d75b743240c4b488fe4691abbff50421d", "0x4cf9f1dad7a5fa46db1709f99f7602c1b9b15580", "0x50fd23a950b7fffde33b6e7bfe9688fa7c5991d5", "0x53a5bc8ef6843396158cf2e5e5acc1c21c6a1be7", "0x53d0702e227a691231e7c7818b2087c99ade6ba0", "0x563ae16edddf6a2fc930464cab9c3a81125621e4", "0x5708039983b2abd1176af631c1717c5d21681845", "0x5867f5f586827317b25821f5538390b4fe0ba485", "0x5a281b038513f020aa7a9c30daebf752ff8dba97", "0x5f2b081ea4475d90edfcde1f597c4d0c7ff3bf20", "0x614c75a386b9057b83698a4bb82e558459ab9d52", "0x617c60f5e7f90cd05bd4f491fc2fd0f2f4051f02", "0x6282410e230f16f3ea2f1372e9c437d30208290d", "0x692e3bae4659d8bd89f812119d31143d6a396803", "0x69e2443d565d21d27b251da1fcc9eeb273b8ada8", "0x6ae2a95ab5defe8507a428d60243061cde638f8d", "0x6c6d7dd9e5c1c73d6ac880816b46eb645bce9622", "0x6c8505e60633e921e3a9f59de0e9cf4dadef1e2d", "0x6d49447779e6431a8be0449066f4306274e6e202", "0x6db9b0674139894da05d5504a406016dcbcc3250", "0x6e44307c8dd24ea4dedfc5993dd9e7c3758cd74a", "0x7277ca789c72ed6a7b05118da6b9d06184fb1958", "0x73d4bd5fbbb5ff8cf09e526f7250ffa4ba249c9f", "0x73f9e16dbc2f4c6a4ce78479327924b7b0373bb4", "0x79d274ba7755c8efb7faabe8ba71cefeed5606ac", "0x7b41f1efb5522d11ededd1f278557e1008428d70", "0x7f91de3fc52869427f1184ddec23ecfda2c089b5", "0x80b09a209a26cf5bce87e170aaeff8d3ac5f8186", "0x8246646b3dabf41a4bd8efdcfd2812e97c3b7edc", "0x843c4635aa97cbd46b176dc23b4c930330ae9276", "0x8841c89b00c66e0a0778eb864b7d393e5939e0b1", "0x89ac7b9748dc50e848e9fc8fbb03e37c7f59f7c8", "0x8a5a0b0710100aa03d085e66c9be44898e01797f", "0x8b1b146fbef0c3a4c9361b1ae7e46b5dfd89ce0a", "0x8e49050476579e91935d65e6dfae57dcfeae2aaa", "0x8fa8deaf63384b6173ee3ca67b84b0c92d6cd44e", "0x90b212ddcc7821e8e55811633da0b9e2dc6d3084", "0x97bf0c551a6eb32ca0aac572cc16b168e982e720", "0x9828a395c8576f879cfdd9624f829726d14407ea", "0x99159f8dadf8dca228af953d9a824f8672dbe8dc", "0x9a0919d9b532c9f087e741262606efe18c40fd4c", "0x9e2861408779d4c6f8b78c97cd0de7810a7bd830", "0xa21afc518c5b05413bab3ce8d7e0f77cad884dea", "0xa309fab84376e7dcace7f74a25a21a7fecfcac89", "0xa4292ae869219a34ce0e89d4516a9550ee18a29e", "0xa5e478ea261dbd62199b487c32ae4a5353c56c68", "0xa66788153e9b006dbcf4cb8982b16a2c4b222db0", "0xa7a245423558aeb0c8b720a47e2fcfc2d243fb6f", "0xa84b03e9fd38c5831b8db5baa0f46e04ae78b23a", "0xac13b426928b0bbf0984457f6407742bc8b9bbcf", "0xaf07eb50bacc713fcee137b80b203855b74e9e0c", "0xafd7f2f9538f8e726472325e14ea14fbcd4e0b36", "0xb6dd9169af999fbf3f152c5b30c815fd50f02e02", "0xb6fc9f8dc63e389d2566b7d3757d1410a53049e6", "0xb735597cfd905eddd0d721658f2e273a3d2728d8", "0xb8d5e4927be8eeabe8b6702582955e0a1aa8f29f", "0xbbfd05ae2e9a066d53071d18d17d1e3a2e4ae00f", "0xbc1a7a1bfc68731850f7ef1374e27d30817e0372", "0xc0132c9d90c15d6dae1198e1ece5df3b70bac915", "0xc63a7f77e7924abe9304f9db8100d0e9a8b5be6b", "0xc916837b1c78add3f08e12be06a91cfa46d8353d", "0xc9a75661739a23a64324fbf7cdd3e1fcdbe99925", "0xcb16824087ce2ec04f07d0d11c6769c3cd49d5d4", "0xcdc9d560162ed89a70b67e68276d72af08760f83", "0xcdef8c8fb082514fbf72ecc2da4d170b22705399", "0xd3ac6e03abde50ece156482a6429abb77ec75e98", "0xd4f66db4a08d063a805dd364ae37c487e34d940e", "0xd58e124ee4245f006f4e3e8bf07f809ce281d354", "0xd5ca19f46484b50b77034b09a42c3c92941881d3", "0xd6cfadc1989b16a85dff16600fb4063d39df6af0", "0xd866eee4f87f891328ce061cc1955844a1759263", "0xd9e86f699f2807b3ec4c4230417d79fda16e2386", "0xdb524d916968942e6debc7415f8ae6a642fd2cc1", "0xe2cb1ae985070ad86034aa8db877c35ad189b8fe", "0xe466509bd36ef82ecdb2553bc1e0cde6845a2910", "0xe5824ab3bc62317d57bbb85e9e10a94549e93d86", "0xeac9afc34f386f934fa3dd22bb22b54c106dc0c5", "0xed4575613ef226e966379e928cb404ea7eccf192", "0xee24157574a47c7a6c71cd03c875fbc4d9ce5106", "0xf4084df5ba98561e213335adb766a097d166a7b2", "0xf6ca5aae7299669d2182b79d3a70e1992d0c2754", "0xfaee2e6101c011e196702d66fa0e15c80e66f1de", "0xfdb0e9a7092eec1ba3b1273815344601a92551dc"},
	"Arcade Galaxy":                {"0x121a3fbbc709e0161ad74e53cb0d413f87f4a6f3", "0x86b1db315b490790df2ca12683b364c26b198882", "0x8ae6a66cd430c53a5df5e07bdbef08b39f0d78bc", "0xf88606850fdd98aed84441aca332df33d2776c95"},
	"Archer Hunter":                {"0x7231876f16a5e935bdac93fb0c1c65b17c5a69b4", "0xd287e43536b16f53add7bcf422e6fc177de796f1", "0xeac9b3a24a4c35c38f589e46abc6fdee701d7c46"},
	"Astro Karts":                  {"0x080e79bb339f009884ca2d450e6de7b33acbc634", "0x319421965bbbc7496556e9bd21daa9629bb71d86", "0x3a4dd2df3c968831fc9d1873786124694a171f5d", "0x44b79ecb984faa91c59bb46c859fde46e9716cea", "0x5922cfc80c61cc6b28ff878229b7bb0f0b690179", "0xb96fb386ff5192a2d9377aeac01d54c9bfd2f26e", "0xe963f77d238c9d9362e3955fed91b3d96f85795d", "0xf5c1b4a01c70c4a373fa4e3f541ea5e9816cbb84"},
	"BombPixel":                    {"0x50975a3d90e4995b9022ffc73b8d4c39bd6fec37", "0x5277f5d97907c9354731d5e8f8404437f33548aa", "0x54fe0208dd39551f84357a22ed53f0c282bef61a", "0x5a335231008fa4a8d2add954d0fb3a1d464702b8", "0x9ce148350158a6898ec17294e9fc1891a7406bec", "0xf413d08b1e7e6a0d4fc1b15aad03790c89a2b4d4"},
	"Boredtopia":                   {"0x0b1adcdbba3f22f42a55efb994180baae79e897c", "0x13e0c46cbe2d54cc2b9108c50c29a93c6d682c4e", "0x24a6029005bd5d5a59d03bf0f0ba6819cbe96dfc", "0x3c3bcca29d63802c081a75bdccbbead011488533", "0x3fb0406660afc6af7eaccd496b6e3030538f6a02", "0x67216d371fdc819a4fd43bf906e525441702225f", "0x67af057c59f34ac73438aa18e7f59edf318bcefd", "0x84ce6d62421ffcee5477457421ebb9314ca6b7b9", "0xed06254c8493e0cdb4fba1f9f80a3dfe485138a6"},
	"Candy Dreams":                 {"0xe3a5c6a15abc50084ee404db4b42e20147c00da9"},
	"Capy vs Monsters":             {"0x516e70ab91f817f4a7ba8aa02bec44a6532c4e33"},
	"CarbonDeFi":                   {"0xa4682a2a5fe02feff8bd200240a41ad0e6eaf8d5", "0xe4816658ad10bf215053c533cceae3f59e1f1087"},
	"Celestial Flutters":           {"0xe676bcd53fd9553159d2ef5b06b744fb57cf090e"},
	"ChainGPT":                     {"0xe7bcb5bedaf6585ee737e6d05c25f8050b11d74d"},
	"Citrex":                       {"0x3894085ef7ff0f0aedf52e2a2704928d1ec074f1", "0x7461cfe1a4766146cafce60f6907ea657550670d", "0x993543dc8bdfcba9fc7355d822108ef49db6b9f9"},
	"Dawnshard":                    {"0xa169f45552a437f0b45e2d5e4804709a0e8e573e", "0xfd762686fc040fa448b7e14136dd9dab619665a9"},
	"Debridge":                     {"0x2328ee20fa271073328dc94e52dd5b61aa0c91a7", "0x43de2d77bf8027e25dbd179b491e8d64f38398aa", "0x663dc15d3c1ac63ff12e45ab68fea3f0a883c251", "0x8244d6ffe0695b30b2bad424683ee3bc534ea464", "0x8a0c79f5532f3b2a16ad1e4282a5daf81928a824", "0x949b3b3c098348b879c9e4f15cecc8046d9c8a8c", "0xc1656b63d9eeba6d114f6be19565177893e5bcbf", "0xe7351fd770a37282b91d153ee690b63579d6dd7f", "0xef4fb24ad0916217251f553c0596f8edc630eb66", "0xf46b9e22a2a5b07bd2f601d9a59b4e01424f5e35", "0xfcf83648b8cdef62e5d03319a6f1fce16e4d6a59"},
	"Dive Diary":                   {"0xa4e07d28df2bcda705d10eaf60c88a7488007748"},
	"Donkeswap":                    {"0x4b4746216214f9e972c5d35d3fe88e6ec4c28a6b"},
	"Donkey Hunt":                  {"0x59b95b521de773b101d3cd297ea54906bda69ae5"},
	"Dragon Land":                  {"0x5969b71e40cbf3b939b835654abe14a2eebf1330"},
	"Dragon Ninja":                 {"0x4db1008bd2ac59e64d2ddd3a7ccca46243c62f1e"},
	"Dragon Slither":               {"0xf55ec370653bf89dc210763f46fd08453b5d7d62"},
	"DragonNinja":                  {"0x4db1008bd2ac59e64d2ddd3a7ccca46243c62f1e"},
	"DragonSwap":                   {"0x11da6463d6cb5a03411dbf5ab6f6bc3997ac7428", "0x150b4a3088dfb06d92531d1cca6e980fb9a270e1", "0x179d9a5592bc77050796f7be28058c51ca575df4", "0x71f6b49ae1558357bbb5a6074f1143c46cbca03d", "0xa4cf2f53d1195addde9e4d3aca54f556895712f2", "0xe30fedd158a2e3b13e9badaeabafc5516e95e8c7"},
	"Drift Zone":                   {"0xaaf80b71f4525fd3fd28edf3e805a95f32598d7d"},
	"DZap":                         {"0xb3926def2e0989b0700a57034c9a211bfbcf858b"},
	"Empire of SEI":                {"0x062992a70ae8f0b6af5c350474c23fe8c50487e2"},
	"Europe Fantasy League":        {"0x0f846c19e82226abb34f9e21215eb8a815d58661", "0x61070f0fee7b188eed23e32692f09ab64c3cceeb", "0x873446d7709b6f19a85c4072b2f9f7c27145a090", "0xae21e8dc31ed7c315ecb59f98f9688ae5d392fd5", "0xb923b7c7664ffffa8f32a9d192b61300bbf75ae8"},
	"FastUSD":                      {"0x37a4dd9ced2b19cfe8fac251cd727b5787e45269"},
	"Filament Finance":             {"0x7540f45b330d489491f7e873e0993f117bddb56f", "0xb28eab5253f6a0f8a5b0a1120d9c03db8fc10c2f"},
	"FishWar":                      {"0x85bc29178bac07468b76078998f550861239c469"},
	"Frax":                         {"0x64445f0aecc51e94ad52d8ac56b7190e764e561a"},
	"GGC: West Land Arena":         {"0x8428fde627634b67e64746233e8cd72a7749c5dc"},
	"Good Game Arcade":             {"0x2df9f48db4a64953c3a15292130ecbd0c4dd0cf2", "0x685f1378f72f895e6e91bc1549b3fc18e90771d6", "0x71f58e5f34ecfcaee30485d4c26abe4a57c8bca9", "0x87f54f9898e946c1dd214cb7bf17af24588ec6fe"},
	"Groovy Market":                {"0x6df78ab56073635838ddf6a592a38710207aaf09", "0xdd489c75be1039ec7d843a6ac2fd658350b067cf"},
	"Hawk Terminal":                {"0x09037f46f38ea15f84602d9c783ebb35d5ea3be3"},
	"Helium Wars":                  {"0x55c9326c99f975cba4028494c13eed4ad513c0fc", "0x884fdedbac2df3c2146810caf02f95baedabd0fa"},
	"Hot Spring":                   {"0x29ffd44130fcf917950cca2b29084095da10db95"},
	"Hyperlane":                    {"0x2f2afae1139ce54fefc03593fee8ab2adf4a85a7"},
	"Idle Glory":                   {"0x33b75754b4265d884c949b0a823c0d9e7b66b906"},
	"Impossible dash":              {"0xa2121284e904a99d95596f42ea3eaa2b6dcc63cd", "0xee939da4f04c7fd87180b5226714afcacb964f31"},
	"Infinity Heroes":              {"0x0501a7af3a1c447c411a71f292c77be8812f6a68"},
	"Interport Finance":            {"0x4e17775a51d74e396e8962cb4db24cda1f7bc31e", "0x58a6bb60978f8a973585286fdfa8f8dfc1ca2706", "0xe9abcc538da92b5e966fda887615b80a488d8ab0", "0xf9ce5d10977a75b73f06df0c37ba6a160736df6c"},
	"JellyVerse":                   {"0xfb43069f6d0473b85686a85f4ce4fc1fd8f00875"},
	"Jumper":                       {"0x0687f1b49e7d536adb8ab7efc80e934175df4da9", "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae", "0x1493e7b8d4dfade0a178dad9335470337a3a219a", "0x1563fe796907bd6ef6f98e035593072f4beef605", "0x22b31a1a81d5e594315c866616db793e799556c5", "0x31a9b1835864706af10103b31ea2b79bdb995f5f", "0x424bdbbaeda89732443fb1b737b6dc194a6ddbd5", "0x45d69a8a07f79de2364f0b80f78af86e99015162", "0x4a3f6c791716f3eb7de82cfa798b4bc8166b11d4", "0x4a8b4d675143116c523068476f8cedd99ed50a42", "0x4bdcd7fc26078e492b89763871163ccbca62c6d6", "0x6d425eab1eb6e002bf40c44e5d9ff7fc6a38824a", "0x6e378c84e657c57b2a8d183cff30ee5cc8989b61", "0x6f2baa7cd5f156ca1b132f7ff11e0fa2ad775f61", "0x7670fc09c1d4ada26e2199f98a221b7389b844b2", "0x7956280ec4b4d651c4083ca737a1fa808b5319d8", "0x8b3673b9b757afc8a86010889cd8657af5c34f06", "0x8c65d3be9b0a98492538068f95a4150b29c8e701", "0x9870f0c91d722b3393383722968269496d919bd8", "0xc00507d91cd2d4ebb8694e553ba70cf416c0779c", "0xc82fd49be3f3d851b9e10589c50784ceac7114a5", "0xc844d2e52c0fb544e68f2a48d39f0bffe424d37f", "0xeb98530f99e1c4406d24fde4b590c96ac7143aee", "0xed170c587ccb4dad6986798c8e7cc66fbb564ac5", "0xf22c55c50ff14d9ab1fa4d9dca832085d143e854", "0xfde9ce4e17b650efdca13d524f132876700d806f"},
	"Jumper Exchange":              {"0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae"},
	"Kame":                         {"0xa9b9e1af3cfd8b1c29e0ae4fddf2dadd74a108cc"},
	"Kawaii Puzzle":                {"0xf54e445bfb5a93ee4d24a4328b73c78084918a3e"},
	"Kismet":                       {"0x4538a11c2700b65fc1fdd6e933a2de8ece6a5c26", "0x879f549ad54bd5033016ba6c42d3ec2060863277", "0x87ac8b15c57ef093869958acc62bb8f066e3e14d", "0xa6311c7c9521a9c071a1838e195190b7c37a0524"},
	"Layerswap":                    {"0x2fc617e933a52713247ce25730f6695920b3befe"},
	"LayerZero":                    {"0x1a44076050125825900e736c501f859c50fe728c"},
	"Li.fi":                        {"0x0687f1b49e7d536adb8ab7efc80e934175df4da9", "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae", "0x1493e7b8d4dfade0a178dad9335470337a3a219a", "0x1563fe796907bd6ef6f98e035593072f4beef605", "0x22b31a1a81d5e594315c866616db793e799556c5", "0x31a9b1835864706af10103b31ea2b79bdb995f5f", "0x424bdbbaeda89732443fb1b737b6dc194a6ddbd5", "0x45d69a8a07f79de2364f0b80f78af86e99015162", "0x4a3f6c791716f3eb7de82cfa798b4bc8166b11d4", "0x4a8b4d675143116c523068476f8cedd99ed50a42", "0x4bdcd7fc26078e492b89763871163ccbca62c6d6", "0x6d425eab1eb6e002bf40c44e5d9ff7fc6a38824a", "0x6e378c84e657c57b2a8d183cff30ee5cc8989b61", "0x6f2baa7cd5f156ca1b132f7ff11e0fa2ad775f61", "0x7670fc09c1d4ada26e2199f98a221b7389b844b2", "0x7956280ec4b4d651c4083ca737a1fa808b5319d8", "0x8b3673b9b757afc8a86010889cd8657af5c34f06", "0x8c65d3be9b0a98492538068f95a4150b29c8e701", "0x9870f0c91d722b3393383722968269496d919bd8", "0xc00507d91cd2d4ebb8694e553ba70cf416c0779c", "0xc82fd49be3f3d851b9e10589c50784ceac7114a5", "0xc844d2e52c0fb544e68f2a48d39f0bffe424d37f", "0xeb98530f99e1c4406d24fde4b590c96ac7143aee", "0xed170c587ccb4dad6986798c8e7cc66fbb564ac5", "0xf22c55c50ff14d9ab1fa4d9dca832085d143e854", "0xfde9ce4e17b650efdca13d524f132876700d806f"},
	"LuckySea":                     {"0xb7334b5dcb7514523b25ccdbe07598577bdb1c6f"},
	"Magic Eden":                   {"0x0000000000000068f116a894984e2db1123eb395"},
	"Majyo Treasure":               {"0x0c3e52ba4e65a787bdf9582da39bae26872313f8"},
	"MetaArena (Final Glory)":      {"0x5b6d32f2b55b62da7a8cd553857eb6dc26bfdc63", "0xa5343d96d3cd64a966c5e73b4ab327e6bb258fca"},
	"Move Move Fish":               {"0x1b660c2ad67ad2c65c0dcd4e4623574bfc633840"},
	"MRKT":                         {"0x1669023b077a60319325262d339e5797e69901e0", "0x96e4c3315c0947e777e8528441fdcf7047e2ebda", "0xe22eade976931e13fdaf1cddc3531f4f36d99f61"},
	"My Guardian Base":             {"0x069b2f92998cc321d17fae0e94b0cf790ef357ea", "0x9df93b99f8b62e3b0663796983d9a93e66c5aba6"},
	"NFTs2Me":                      {"0x00000000001594c61dd8a6804da9ab58ed2483ce"},
	"Oku":                          {"0x0493f80f3d4da87aa0f3713e81e84062d769e5e1", "0x0a6358f069268c7dc4918d5b12c69a782b957ead", "0x1f126214173009c95be1ed34a77f5852594cf7bf", "0x38eb9e62abe4d3f70c0e161971f29593b8ae29ff", "0x41eea09c971294fcde3b6e553902b04a47be7442", "0x454050c4c9190390981ac4b8d5afcd7ac65eeffa", "0x5cfa8db453c9904511c4ea9eb0bfc903e36b9f5f", "0x6aa54a43d7eef5b239a18eed3af4877f46522bca", "0x6bb02cff2256cbab75a9d3d1323d22ca799311bb", "0x743e03cceb4af2efa3cc76838f6e8b50b63f184c", "0x75fc67473a91335b5b8f8821277262a13b38c9b3", "0x807f4e281b7a3b324825c64ca53c69f0b418de40", "0x8a1a9efb7f7f74ace10a31f2f5f9f7e804f957b1", "0x8b3c541c30f9b29560f56b9e44b59718916b69ef", "0xa3a573c8d14c93fca8fdecb7db168619563d9b00", "0xa683c66045ad16abb1bce5ad46a64d95f9a25785", "0xa9d71e1dd7ca26f26e656e66d6aa81ed7f745bf0", "0xaa52bb8110fe38d0d2d2af0b85c3a3ee622ca455", "0xb3309c48f8407651d918ca3da4c45de40109e641", "0xb49cdf5367dda15793b3751be5913cdf20d4d1ca", "0xb952578f3520ee8ea45b7914994dcf4702cee578", "0xbab319c397913c1b57b55686d7d871b0f129e4ef", "0xc41991725dfc870fee9363e9875019cb85effa61", "0xca11bde05977b3631167028862be2a173976ca11", "0xd5c6366e7cb1f367098bb2102e3d31e153a5a0fc", "0xd750bfbb4e50b67fd3258bffad3ec87481760b47", "0xdd489c75be1039ec7d843a6ac2fd658350b067cf", "0xdf92702c81690c8c83d25c162fd65cc9108c9d50", "0xe3dbcd53f4ce1b06ab200f4912bd35672e68f1fa"},
	"Oku (Uniswap)":                {"0x0465375274301ffcdf3274e59544d535e03b3688", "0x0493f80f3d4da87aa0f3713e81e84062d769e5e1", "0x0a6358f069268c7dc4918d5b12c69a782b957ead", "0x1502fa4be69d526124d453619276faccab275d3d", "0x1e18cdce56b3754c4dca34cb3a7439c24e8363de", "0x2880ab155794e7179c9ee2e38200202908c17b43", "0x2f2afae1139ce54fefc03593fee8ab2adf4a85a7", "0x38eb9e62abe4d3f70c0e161971f29593b8ae29ff", "0x41eea09c971294fcde3b6e553902b04a47be7442", "0x454050c4c9190390981ac4b8d5afcd7ac65eeffa", "0x47b4f8940eff392bc465d02faadf471796ffc2d1", "0x4a4d9abd36f923cba0af62a39c01dec2944fb638", "0x5523985926aa12ba58dc5ad00ddca99678d7227e", "0x5cfa8db453c9904511c4ea9eb0bfc903e36b9f5f", "0x60485c5e5e3d535b16cc1bd2c9243c7877374259", "0x6aa54a43d7eef5b239a18eed3af4877f46522bca", "0x6e8d0b4ebe31c334d53ff7eb08722a4941049070", "0x743e03cceb4af2efa3cc76838f6e8b50b63f184c", "0x75fc67473a91335b5b8f8821277262a13b38c9b3", "0x79828044c306dd3ae60db03cb56309883f74100b", "0x807f4e281b7a3b324825c64ca53c69f0b418de40", "0x8a1a9efb7f7f74ace10a31f2f5f9f7e804f957b1", "0x8b3c541c30f9b29560f56b9e44b59718916b69ef", "0x9f3b1c6b0cddfe7adadd7aadf72273b38eff0ebc", "0xa2d98bbb6fe820d6382d5f3ece7177060ed3ccea", "0xa3a573c8d14c93fca8fdecb7db168619563d9b00", "0xa683c66045ad16abb1bce5ad46a64d95f9a25785", "0xa9d71e1dd7ca26f26e656e66d6aa81ed7f745bf0", "0xaa52bb8110fe38d0d2d2af0b85c3a3ee622ca455", "0xaf4057ba4bfbdac52161b60b11eaf2613c3d5931", "0xb094948a74fe32352b67ebc4eff07f2e38d63e95", "0xb14e3db320b195990990108c271f0558a795a875", "0xb3309c48f8407651d918ca3da4c45de40109e641", "0xb49cdf5367dda15793b3751be5913cdf20d4d1ca", "0xb952578f3520ee8ea45b7914994dcf4702cee578", "0xbab319c397913c1b57b55686d7d871b0f129e4ef", "0xc3da629c518404860c8893a66ce3bb2e16bea6ec", "0xca11bde05977b3631167028862be2a173976ca11", "0xce733ca21882d1407386af13c59f59e02b1db5a9", "0xd5c6366e7cb1f367098bb2102e3d31e153a5a0fc", "0xdbbf09b9b9f99af7906160b59a3753924768aa47", "0xdd489c75be1039ec7d843a6ac2fd658350b067cf", "0xe3dbcd53f4ce1b06ab200f4912bd35672e68f1fa", "0xe81e52a751688e1a0c45a443eebc3f0c4bd54851", "0xe85a6a8f1168ef5dd89b5dc45e3add489997c8e1", "0xfe4bb996926aca85c9747bbec886ec2a3f510c66"},
	"OKX DEX":                      {"0x411d2c093e4c2e69bf0d8e94be1bf13dadd879c6", "0x801d8ed849039007a7170830623180396492c7ed"},
	"OnChainGM":                    {"0x30b30e4a8a5b8057bd86edc82a9f9613e4ef08af", "0xc21df1bb620ebe7f5ae0144df50de28ce0d47ae7"},
	"OpenOcean":                    {"0x6352a56caadc4f1e25cd6c75970fa768a3304e64"},
	"Opensea":                      {"0x0000000000000068f116a894984e2db1123eb395"},
	"Operation Safe Place Defense": {"0x5d84c391bc1ed4df4210a94308074aa77d177d52", "0xd8ede2f42eb68525b1366ebd47666aea0714705b"},
	"Orochi Network":               {"0x3ef3cae1578db51144f04f8da0336c225b183bbf"},
	"OverHerd":                     {"0xe2e68c384a4c8fb5a9d1782190ccb87c9ecc12a6"},
	"Pheasant Network":             {"0xfc9c6b6e0d02eade37ac8b6c59e7181726075696"},
	"Piratopia":                    {"0x34ad40405c23cb2a4b4d5099ab3578faea501981", "0x5ba434a4a7be53833ec921c26f029aea53ae71fe", "0xa678d15c771719e2e5410da31852fce65ab46a3d"},
	"Pit":                          {"0x0044f5f3f121beb7ef11c6cfbc45a56e80adfd6b", "0x07428d1cdc20ce70ac5d06e44ccb9920f99ab447", "0x0aefc6f2e9a866ddb4813cb1e897a2e9e26a1e53", "0x0f9bacd61cf1fd9d038f41a8380dbd7a826470a7", "0x130c58f93ce57121c97f524cd5171bdd258f4314", "0x1e3bceb9ad3dc3f80820d29039c1a46e28d3a573", "0x1eb5b573852897831414fbd607c214edb9b0b7dd", "0x318253dece357ef0f75428c7369a180d60fd0971", "0x3aec64f4e285cffd50605eb962455159d436d31b", "0x43e60c59567996c5a1b27ca8719adf6ca2bc407b", "0x49561c4a905f7accdacaec5e3c17113d5f1c5a3b", "0x5512d5e89a29447462ab6bbbcbd3fe6f0a0d9fdd", "0x575e2291c6fde8eef7284cbd7ff17615de75c47b", "0x5e2baf273586f747e406eb0fd0cadaece0f479da", "0x63e37028c1740303e8456962e6ddf98359fe0bdc", "0x6944bfdacd00957715ecbceaac3f49e07cb6f35f", "0x79756b033390232a5a55b5d6571a7bc1b11a2b93", "0x7c96bf5203fa1584a81f160b9445eab02bfa3e7b", "0x9975f0d8560ed6a29c376ddc950f5663527327e8", "0x9cddffc79ba597ccbbd6da6a81cc1b1ef29fb4de", "0xa4a3b54665a5ef21ac5b294b8945dd130ac6e5fc", "0xb4169a0cd663fb4a7762a71c9d0dd0ddc18201e2", "0xcd6d59a116f0d4e370a62b8d6ea146dd02e2114a", "0xdf7f9020800c2b444ad524982fc6ca5a30915811", "0xe560fc83ea78028a95ccab1ecd45039ffb62301c", "0xe77790035f956e9c6e895e57cded63a7ac69f2c9", "0xe8d7f53173bb5d0c898e4d576e4e297cb2859472", "0xed6a3cd7591df25bc92e42381d9c53e041b5acaa"},
	"Pixudi":                       {"0xa4a0aa78b8b20315274b37125386f4c4c2876282"},
	"Pyth":                         {"0x2880ab155794e7179c9ee2e38200202908c17b43", "0x98046bd286715d3b0bc227dd7a956b83d8978603"},
	"QuizMatch":                    {"0xd844298d9d42d652e263e2b4c750e5578d8b1e7a"},
	"Raiinmaker":                   {"0x481fe356df88169f5f38203dd7f3c67b7559fda5"},
	"RGBClash":                     {"0x3b1bcecea7b956c9215895a6746bb8998d492ced", "0x833068b2e9a81c55bcb4b0acb148002f6f9eff07", "0x9168af279bcd490dd422b9760bd9efa82ac81cb0", "0xda8b604c7078ec28bf051ca66e6b2b989fb1e96f"},
	"Sacred Tails":                 {"0x3c986ab7443338ef2f31131b3d1c7cc2113d3e0c", "0xc1be60fcdffb5c40aa85a7078f45ee638eeff4cd", "0xeb3ed409574f65a872224d06d0c1e79d47ca9cb6"},
	"Sailor Finance":               {"0x4ed1f9cc4ee6bc11b27e285464c4dcaf911c85d9", "0x8417845622e9e5d6c2e0e61ad70969a7ef7d3725", "0x9aeb489f5bc0d3eb7892dd7e1fae2d2ebd02e80b", "0xa51136931fdd3875902618bf6b3abe38ab2d703b", "0xd1efe48b71acd98db16fcb9e7152b086647ef544", "0xe294d5eb435807cd21017013bef620ed1aeafbeb", "0xe40703878ac5d3f76eac66f8688a8f5652af85b1"},
	"SeiBound":                     {"0x1650e7e46235fa6d961153ce6fad3a49b320a55e"},
	"Seipex":                       {"0x3a0a7a3ca25c17d15e8d51332fb25bfea274d107", "0xe70aaa288f801b208074171be87b804178ce5d11"},
	"SEIYARA":                      {"0x0b7dd6e165a089c8eb58ca9f851da42ae3fa1cc4", "0x475e850233d126762917d2f3a0afdca23d789e39"},
	"Shadow Mysteries":             {"0x4b60bef991c993a3dabdb70d1c7cb584dc5693dc", "0xec81d8283114bdd792e926dc46fb3eb54795af03"},
	"Silo":                         {"0x00a88cef6ee22e12a63e4925f7724ddebad9f11e", "0x0e68b4764e30cefa3133dd3b249d541fd914e26c", "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae", "0x23a7abeb8111eff8b7ae13db0122b11c67a92b64", "0x26d1ef68ceb048e75398859ef63abd163afa5f8e", "0x28a3f57ce1625b5849f2faef7b52016f49588d59", "0x3a236f67fce401d87d7215695235e201966576e4", "0x43dae8bb39d43f2fa7625715572c89c4d8ba26d6", "0x4a472dd119226f6f37f3c0424067678d30734a30", "0x521f16979c5f3d30b578657a594de3f5e2822b6f", "0x567b3deb44ad1e837296a5c6ffc798636b72e4ae", "0x5cf6826140c1c56ff49c808a1a75407cd1df9423", "0x603bfa58dbd9f4c0955f906473c80f04f001120a", "0x64445f0aecc51e94ad52d8ac56b7190e764e561a", "0x773bd3cf2d2e2236d3f836cb200bcb4162e16208", "0x7c1daae7bb0688c9bfe3a918a4224041c7177256", "0x7ccdcfbcfb6b4d6dbb148d5c4b1786eb87a58d43", "0x809ff4801aa5bdb33045d1fec810d082490d63a4", "0x89bb5417438bb630b9cdfe92eb9afd06d8b76886", "0x8c1014b5936dd88baa5f4db0423c3003615e03a0", "0x9278ad180828ed21ef509757af7bd0023cb121f5", "0x95a2d143910977d22460ddb9ca0c18e37c0ee227", "0x972377471f229ec94cbb3c10d615f4b571420e21", "0x97aa7b7501fa0fe66649de7394b9794fa40aef02", "0xa7fdcbe645d6b2b98639ebacbc347e2b575f6f70", "0xb9f3b158bc82ffd3c3532ef63ffef938d9ac20e0", "0xbd00c87850416db0995ef8030b104f875e1bdd15", "0xc097ab8cd7b053326dfe9fb3e3a31a0cce3b526f", "0xc56eb3d03c5d7720daf33a3718affb9bcab03fbc", "0xc99366dab9ad05157a680fe5107baff5b32d6214", "0xce8f24a58d85ed5c5a6824f7be1f8d4711a0eb4c", "0xd24972c11f91c1bb9eaee97ec96bb9c33cf7af24", "0xd3bf119cad7c8567e4543dc47fce8db104dd93dd", "0xd8936a68397f8bc87ac4e632484a3df2159611a5", "0xdd7d5e4ea2125d43c16eed8f1ffefffa2f4b4af6", "0xe1844c5d63a9543023008d332bd3d2e6f1fe1043", "0xe1e2dabab661c44f81b8b024a9395bf2b9431723", "0xe5085112160ff75ee89a540cdba570eafdaf7f57", "0xe6889b9698cf32206c6ca6f6f83da1b6e7f12fa0", "0xf2a024b29a7db71ec6ca6718a72bfb2a8fb0bf64", "0xfd5b6c13abafaab553c1b2fe865f0cf9fe6989e5"},
	"Silo Staking":                 {"0x5cf6826140c1c56ff49c808a1a75407cd1df9423"},
	"SpinCity":                     {"0x3e3900513e5da31183e816af1b2c460b6ff4c9e5", "0x6cafad6aaf5f3eab6a7a31472a8b577d5ecf8777", "0x87257d64deab8012227b8dad262d20d390ff3227", "0x8e5c235b586a8b4d3b326eb999f7c892daf45283", "0xba9498e8f312fffc78de8ef3cb73b0869b51549d"},
	"Star Symphony":                {"0x94158bf76873ee0a9704efef4566732889e51907", "0xe34d1bee70dfff37a5d6378472ee2172354c78e3"},
	"Stargate":                     {"0x0db9afb4c33be43a0a0e396fd1383b4ea97ab10a", "0x1502fa4be69d526124d453619276faccab275d3d", "0x45d417612e177672958dc0537c45a8f8d754ac2e", "0x5c386d85b1b82fd9db681b9176c8a4248bb6345b", "0x873cfb4bae1ab6a5de753400e9d0616e10dced22", "0xde48600aa18ae707f5d57e0faafec7c118abaeb2"},
	"Sugar Senpai":                 {"0xe563e28016866329e705003d279b9a52093204e6"},
	"Sunnyside Acres":              {"0xa3feae683e706d1c019bad2f783d394a18fb6704"},
	"Symbiosis":                    {"0x292fc50e4eb66c3f6514b9e402dbc25961824d62"},
	"Symphony":                     {"0xad3df981018149cd90e5869d14efe2516b108270"},
	"Synnax":                       {"0x059a6b0ba116c63191182a0956cf697d0d2213ec"},
	"Takara Lend":                  {"0x059798f39461e17047b6d2ad6aae4d3a0dd9dc82", "0x323917a279b209754b32ab57a817c64ecfe2af40", "0x56a171acb1bba46d4fdf21afbe89377574b8d9bd", "0x68a92be349d48766128c0ae893fc391859f9bc11", "0x71034bf5ec0fad7aee81a213403c8892f3d8caee", "0x8df1265bfb778ffd08341c63e7c67367c0a60288"},
	"Tea-REX":                      {"0x344a61a393c4c61d767c0c2f1fdfb8a09faaa817", "0xa4a956b2515336b754ee20ed58d3b6d67d44807a", "0xac009609bacefc9a25897581a9c4a028f79207f1", "0xbd95774b89ee0874df6b1f23884bfdf8c9ec696f"},
	"USDC":                         {"0x3894085ef7ff0f0aedf52e2a2704928d1ec074f1"},
	"USDT0":                        {"0x9151434b16b9763660705744891fa906f660ecc5"},
	"Vertex":                       {"0x0000000000035bd5849b8cb31eaa5b39630e6b72", "0x10ca72e87c0f2743fdf1a3b49989baecd4647df3", "0x14571ffa50edb7595b0d806b6529ba44f21687ba", "0x2777268eee0d224f99013bc4af24ec756007f1a6", "0x2894358e6f8daf5d4bf80517d698724c28eb0149", "0x3c2269811836af69497e5f486a85d7316753cf62", "0x56ffa2fd437c3a718322ea701bed40560745456e", "0x581a5e0ba509781bb99f13e96295ab90756a7efb", "0x5cea65a8d17b35128e806447fb01c6bac06fcc8c", "0x613cb5b7a8ffd4304161f30fba46ce4284c25e21", "0x79a0bf8a865398b2247609c88a53cd839a49b0a4", "0x992b7521b928e05d78d7444cb65a77fb5bb4be5c", "0xae1510367aa8d500bdf507e251147ea50b22307f", "0xbc096b6a7d5404de916b3333ad223a1b32eec8aa", "0xe60145b2b9c94186f16072db6d327e91ce18b96a", "0xf6f47578279030f62c83518101a24ef4db0fb46f"},
	"VIA Labs":                     {"0x15ac559da4951c796db6620fab286b96840d039a"},
	"Volta (sei Bunker)":           {"0x1352330f823611b4d3500933938b2e196bb4564b", "0x37b859507ac19888703db3648afe0368bb68bb0e", "0x8cfe7e4192fd43a5efa80dbc6ef61628fc8da169", "0xa77ccabf9ad93993a9243766a16bf71970cdda88"},
	"Wcoin":                        {"0x9e0c7cf1fc8bd83678cd211a51a26dea8a860c78", "0xb80199e97181c4f606ab82f6eb20796d15865340"},
	"WcoinGame":                    {"0x7d0e3c9b0ef8ad43ec9c85f81d90a106af8d5666", "0x829dffba31dfd30e1115ad3889cbb9a56d8f5ff3", "0xb80199e97181c4f606ab82f6eb20796d15865340", "0xe8eae1aab1b4bba29eabaefe7df4510833b6491e"},
	"Webump":                       {"0xcf57971769e2abe438c9644655bd7ae0f2f9fec8"},
	"Wild West Shooting":           {"0xd3f17c220da098a2a77384514913f34b88fd88ea"},
	"World of Dypians":             {"0x2deecf2a05f735890eb3ea085d55cec8f1a93895", "0x6041dc62b74e28596b4917693f6b0f5baa61a13f", "0x8e4917c1ba9598fbbf66934cb17ac28c3b5849ab"},
	"Yaka Finance":                 {"0x51121bcae92e302f19d06c193c95e1f7b81a444b"},
	"YEI":                          {"0x04295e6912f95f2690993473e6ccaae438cf3f06", "0x093066736e6762210de13f92b39cf862eee32819", "0x127201e84ad4ee06ec15104cf083696d6354f8dd", "0x1c4b5c523f859c0f1f14a722a1eafde10348f995", "0x241995b768c1ae629eb5a6f3749c6e7b8c4d47f2", "0x242022df3e68597810356b6210dfa53c3129f466", "0x2a662ef26556a7d8795bf7a678e3dd4b36fdec1e", "0x4ec5e3f9a32aabd6af62b9a22188f429d65f39c7", "0x5c57266688a4ad1d3ab61209ebcb967b84227642", "0x60c82a40c57736a9c692c42e87a8849fb407f0d6", "0x69ea2c310a950e58984f4bec4accf2ece391dafd", "0x7090d5fdcefb496651b55c20d56282cbcddc2ee2", "0x800f3e929686ec90eeaabb8b98ed1eff126d532c", "0x809ff4801aa5bdb33045d1fec810d082490d63a4", "0x8138da4417340594aeea4be8fbc7693d9875b6cb", "0x92e59fb4c379381926494880f94dbc3635207f89", "0x945c042a18a90dd7adb88922387d12efe32f4171", "0xa1ce28cebab91d8df346d19970e4ee69a6989734", "0xa524c4a280f3641743eba56e955a1c58e300712b", "0xb6298bcd7ec6ca2a6eabdd84a88969091b2c3291", "0xbf63c919a8c15f4741e75c232c7be0d0af4d1d05", "0xc15dce4e1bfabbe0897845d7f7ee56bc37113e08", "0xc1a6f27a4ccbabb1c2b1f8e98478e52d3d3cb935", "0xd078c43f88fbed47b3ce16dc361606b594c8f305", "0xf8157786e3401a7377becb7af288b84c8ee614e1", "0xf8feb964a1d02f61bcd4b8429c82cb8f5ee58993"},
	"Zeroway":                      {"0x6052c5c075212f013c856bff015872914ed3492a", "0xd5ebc5e5f38c2d8c91c43122a105327c1f0260b4"},
	"Zombies of Arcadia":           {"0x98c9f412b544da34ba2172473a63a821684f0de1"},
}

func StateSizeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "state-size",
		Short: "Print analytical results for state size",
		Run:   executeStateSize,
	}

	cmd.PersistentFlags().StringP("db-dir", "d", "", "Database Directory")
	cmd.PersistentFlags().Int64("height", 0, "Block Height")
	cmd.PersistentFlags().StringP("module", "m", "", "Module to export. Default to export all")
	return cmd
}

type contractSizeEntry struct {
	Address   string
	TotalSize int64
	KeyCount  int
}

type projectSizeEntry struct {
	Project   string
	TotalSize int64
	KeyCount  int
}

type moduleStats struct {
	keySizeByPrefix   map[string]int64
	valueSizeByPrefix map[string]int64
	numKeysByPrefix   map[string]int64
	contractSizes     map[string]*contractSizeEntry
	totalNumKeys      int
	totalKeySize      int
	totalValueSize    int
	totalSize         int
}

func newModuleStats() *moduleStats {
	return &moduleStats{
		keySizeByPrefix:   make(map[string]int64),
		valueSizeByPrefix: make(map[string]int64),
		numKeysByPrefix:   make(map[string]int64),
		contractSizes:     make(map[string]*contractSizeEntry),
	}
}

// evmPrefixLabels maps the first-byte hex prefix to its EVM key space name
var evmPrefixLabels = map[string]string{
	"01": "EVMAddressToSeiAddressKeyPrefix",
	"02": "SeiAddressToEVMAddressKeyPrefix",
	"03": "StateKeyPrefix",
	"04": "TransientStateKeyPrefix (deprecated)",
	"05": "AccountTransientStateKeyPrefix (deprecated)",
	"06": "TransientModuleStateKeyPrefix (deprecated)",
	"07": "CodeKeyPrefix",
	"08": "CodeHashKeyPrefix",
	"09": "CodeSizeKeyPrefix",
	"0A": "NonceKeyPrefix",
	"0B": "ReceiptKeyPrefix",
	"0C": "WhitelistedCodeHashesForBankSendPrefix",
	"0D": "BlockBloomPrefix",
	"0E": "TxHashesPrefix (deprecated)",
	"0F": "WhitelistedCodeHashesForDelegateCallPrefix",
	"15": "PointerRegistryPrefix",
	"16": "PointerCWCodePrefix",
	"17": "PointerReverseRegistryPrefix",
	"18": "AnteSurplusPrefix (transient)",
	"19": "DeferredInfoPrefix (transient)",
	"1A": "LegacyBlockBloomCutoffHeightKey",
	"1B": "BaseFeePerGasPrefix",
	"1C": "NextBaseFeePerGasPrefix",
}

func prefixDisplayName(moduleName, hexPrefix string) string {
	if moduleName == "evm" {
		if name, ok := evmPrefixLabels[strings.ToUpper(hexPrefix)]; ok {
			return name
		}
	}
	return hexPrefix
}

func executeStateSize(cmd *cobra.Command, _ []string) {
	module, _ := cmd.Flags().GetString("module")
	dbDir, _ := cmd.Flags().GetString("db-dir")
	height, _ := cmd.Flags().GetInt64("height")
	if dbDir == "" {
		panic("Must provide database dir")
	}

	opts := memiavl.Options{
		Dir:             dbDir,
		ZeroCopy:        true,
		CreateIfMissing: false,
	}
	db, err := memiavl.OpenDB(logger.NewNopLogger(), height, opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	fmt.Printf("Finished opening db, calculating state size for module: %s\n", module)
	err = PrintStateSize(module, db)
	if err != nil {
		panic(err)
	}
}

// PrintStateSize print the raw keys and values for given module at given height for memIAVL tree
func PrintStateSize(module string, db *memiavl.DB) error {
	modules := []string{}
	numToShow := 20
	if module == "" {
		modules = AllModules
	} else {
		modules = append(modules, module)
	}
	for _, moduleName := range modules {
		tree := db.TreeByName(moduleName)
		if tree == nil {
			fmt.Printf("Tree does not exist for module %s \n", moduleName)
			continue
		}
		fmt.Printf("Calculating for module: %s \n", moduleName)
		stats := newModuleStats()
		// Scan and accumulate statistics
		tree.ScanPostOrder(func(node memiavl.Node) bool {
			processNode(moduleName, module, node, stats)
			if stats.totalNumKeys%1000000 == 0 && stats.totalNumKeys > 0 {
				fmt.Printf("Scanned %d keys for module %s\n", stats.totalNumKeys, moduleName)
			}
			return true
		})

		printModuleTotals(moduleName, stats)
		printPrefixBreakdown(moduleName, stats)

		if module == "evm" {
			printEvmContractBreakdown(stats, numToShow)
			printEvmProjectTotals(stats)
			printEvmContractSizeDistribution(stats)
		}
	}
	return nil
}

func processNode(moduleName, requestedModule string, node memiavl.Node, stats *moduleStats) {
	if !node.IsLeaf() {
		return
	}
	stats.totalNumKeys++
	keySize := len(node.Key())
	valueSize := len(node.Value())
	stats.totalKeySize += keySize
	stats.totalValueSize += valueSize
	stats.totalSize += keySize + valueSize

	prefixKey := fmt.Sprintf("%X", node.Key())
	prefix := prefixKey[:2]
	stats.keySizeByPrefix[prefix] += int64(keySize)
	stats.valueSizeByPrefix[prefix] += int64(valueSize)
	stats.numKeysByPrefix[prefix]++

	if requestedModule == "evm" && prefix == "03" {
		addrHexNoPrefix := strings.ToLower(prefixKey[2:42]) // 40 hex chars
		if _, exists := stats.contractSizes[addrHexNoPrefix]; !exists {
			stats.contractSizes[addrHexNoPrefix] = &contractSizeEntry{Address: addrHexNoPrefix}
		}
		entry := stats.contractSizes[addrHexNoPrefix]
		entry.TotalSize += int64(keySize) + int64(valueSize)
		entry.KeyCount++
	}
}

func printModuleTotals(moduleName string, stats *moduleStats) {
	fmt.Printf(
		"Module %s total numKeys:%d, total keySize MB:%d, total valueSize MB:%d, totalSize MB: %d \n",
		moduleName,
		stats.totalNumKeys,
		stats.totalKeySize/1024/1024,
		stats.totalValueSize/1024/1024,
		stats.totalSize/1024/1024,
	)
}

func printPrefixBreakdown(moduleName string, stats *moduleStats) {
	keys := make([]string, 0, len(stats.keySizeByPrefix))
	for k := range stats.keySizeByPrefix {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		ki, kj := keys[i], keys[j]
		return stats.keySizeByPrefix[ki]+stats.valueSizeByPrefix[ki] > stats.keySizeByPrefix[kj]+stats.valueSizeByPrefix[kj]
	})

	labels := make(map[string]string, len(keys))
	labelWidth := len("Prefix")
	for _, k := range keys {
		l := prefixDisplayName(moduleName, k)
		labels[k] = l
		if len(l) > labelWidth {
			labelWidth = len(l)
		}
	}
	if labelWidth < 24 {
		labelWidth = 24
	}
	fmt.Printf("%-*s  %15s  %10s\n", labelWidth, "Prefix", "Total Size (MB)", "Percentage")
	for _, k := range keys {
		keySize := stats.keySizeByPrefix[k]
		valueSize := stats.valueSizeByPrefix[k]
		kvTotalSize := keySize + valueSize
		percentage := 0.0
		if stats.totalSize > 0 {
			percentage = float64(kvTotalSize) / float64(stats.totalSize) * 100
		}
		label := labels[k]
		fmt.Printf("%-*s  %15d  %9.2f\n", labelWidth, label, kvTotalSize/1024/1024, percentage)
	}
}

func printEvmContractBreakdown(stats *moduleStats, numToShow int) {
	var sortedContracts []contractSizeEntry
	for _, entry := range stats.contractSizes {
		sortedContracts = append(sortedContracts, *entry)
	}
	sort.Slice(sortedContracts, func(i, j int) bool { return sortedContracts[i].TotalSize > sortedContracts[j].TotalSize })

	fmt.Printf("\nDetailed breakdown for 0x03 prefix (top %d contracts by total size):\n", numToShow)
	fmt.Printf("%s,%s,%s\n", "Contract Address", "Total Size", "Key Count")
	if len(sortedContracts) < numToShow {
		numToShow = len(sortedContracts)
	}
	for i := 0; i < numToShow; i++ {
		contract := sortedContracts[i]
		fmt.Printf("0x%s,%d,%d\n", contract.Address, contract.TotalSize/1024/1024, contract.KeyCount)
	}
	fmt.Printf("Total unique contracts: %d\n", len(stats.contractSizes))
}

func printEvmProjectTotals(stats *moduleStats) {
	projectSizes := make(map[string]*projectSizeEntry)
	addrToProjects := buildAddrToProjects()
	for _, c := range stats.contractSizes {
		addr0x := "0x" + strings.ToLower(c.Address)
		if projs, ok := addrToProjects[addr0x]; ok {
			for _, p := range projs {
				ps := projectSizes[p]
				if ps == nil {
					ps = &projectSizeEntry{Project: p}
					projectSizes[p] = ps
				}
				ps.TotalSize += c.TotalSize
				ps.KeyCount += c.KeyCount
			}
		}
	}
	var projList []projectSizeEntry
	for _, ps := range projectSizes {
		projList = append(projList, *ps)
	}
	sort.Slice(projList, func(i, j int) bool { return projList[i].TotalSize > projList[j].TotalSize })
	fmt.Printf("\nProject totals (sum over listed contract addresses in 0x03 keys):\n")
	fmt.Printf("%s,%s,%s\n", "Project", "Total Size (MB)", "Key Count")
	for _, ps := range projList {
		fmt.Printf("%s,%d,%d\n", ps.Project, ps.TotalSize/1024/1024, ps.KeyCount)
	}
}

func printEvmContractSizeDistribution(stats *moduleStats) {
	const (
		KB = int64(1024)
		MB = int64(1024 * 1024)
	)
	total := len(stats.contractSizes)
	if total == 0 {
		fmt.Printf("\nNo EVM contracts found for size distribution.\n")
		return
	}

	var (
		lt1KBCount      int
		_1KBto1MBCount  int
		_1MBto10MBCount int
		_10MBto100Count int
		gt100MBCount    int

		lt1KBSizeBytes      int64
		_1KBto1MBSizeBytes  int64
		_1MBto10MBSizeBytes int64
		_10MBto100SizeBytes int64
		gt100MBSizeBytes    int64
	)

	for _, c := range stats.contractSizes {
		size := c.TotalSize
		switch {
		case size < 1*KB:
			lt1KBCount++
			lt1KBSizeBytes += size
		case size >= 1*KB && size < 1*MB:
			_1KBto1MBCount++
			_1KBto1MBSizeBytes += size
		case size >= 1*MB && size < 10*MB:
			_1MBto10MBCount++
			_1MBto10MBSizeBytes += size
		case size >= 10*MB && size <= 100*MB:
			_10MBto100Count++
			_10MBto100SizeBytes += size
		default: // > 100MB
			gt100MBCount++
			gt100MBSizeBytes += size
		}
	}

	// Sum sizes for size-based percentages
	sumSizeBytes := lt1KBSizeBytes + _1KBto1MBSizeBytes + _1MBto10MBSizeBytes + _10MBto100SizeBytes + gt100MBSizeBytes

	fmt.Printf("\nContract size distribution (StateKeyPrefix 0x03 only):\n")
	fmt.Printf("%-20s  %10s  %9s  %15s  %8s\n", "Bucket", "Count", "Count %", "Total Size (MB)", "Size %")
	printBucket := func(name string, count int, sizeBytes int64) {
		countPct := float64(count) / float64(total) * 100
		sizePct := 0.0
		if sumSizeBytes > 0 {
			sizePct = float64(sizeBytes) / float64(sumSizeBytes) * 100
		}
		fmt.Printf("%-20s  %10d  %8.2f  %15d  %7.2f\n", name, count, countPct, sizeBytes/MB, sizePct)
	}
	printBucket("<1KB", lt1KBCount, lt1KBSizeBytes)
	printBucket("1KB - <1MB", _1KBto1MBCount, _1KBto1MBSizeBytes)
	printBucket("1MB - <10MB", _1MBto10MBCount, _1MBto10MBSizeBytes)
	printBucket("10MB - 100MB", _10MBto100Count, _10MBto100SizeBytes)
	printBucket(">100MB", gt100MBCount, gt100MBSizeBytes)

	// Totals row and module share
	fmt.Printf("%-20s  %10d  %8.2f  %15d  %7.2f\n", "Total", total, 100.00, sumSizeBytes/MB, 100.00)
	if stats.totalSize > 0 {
		share := float64(sumSizeBytes) / float64(stats.totalSize) * 100
		fmt.Printf("Share of module total size: %.2f%%\n", share)
	}
}

func buildAddrToProjects() map[string][]string {
	addrToProjects := make(map[string][]string)
	for proj, addrs := range ProjectContracts {
		for _, a := range addrs {
			key := strings.ToLower(a) // keep "0x" prefix
			addrToProjects[key] = append(addrToProjects[key], proj)
		}
	}
	return addrToProjects
}
