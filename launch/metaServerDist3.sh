SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
$SCRIPTPATH/../java/target/surfstore/bin/runMetadataStore $SCRIPTPATH/../configs/configDistributed.txt -n 3
