SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
#heartbeat
echo "Test heartbeat:"
if $SCRIPTPATH/../java/target/surfstore/bin/runClient  $SCRIPTPATH/../configs/configDistributed.txt heartbeat /home/linux/ieng6/cs291s/cs291sbu/p2-p2-ly/test/file1/file1test.txt ; then
	echo "heartbeat success!"
else
	echo "heartbeat failed!"
fi
echo ""


